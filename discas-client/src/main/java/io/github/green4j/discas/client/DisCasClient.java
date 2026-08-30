/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.client.lock.DistributedLock;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockAcquireStatus;
import io.github.green4j.discas.client.lock.LockClientOps;
import io.github.green4j.discas.client.lock.LockInfo;
import io.github.green4j.discas.client.lock.LockInfoResult;
import io.github.green4j.discas.client.lock.LockInfoStatus;
import io.github.green4j.discas.client.lock.LockToken;
import io.github.green4j.discas.client.lock.LockValueCodec;
import io.github.green4j.discas.client.lock.LockWriteResult;
import io.github.green4j.discas.client.lock.LockWriteStatus;
import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.KeyHash;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.io.ReloadableFiles;
import io.github.green4j.discas.common.io.ReloadReport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/**
 * The client end of a discas cluster: key/value reads and writes, prefix scans, key watches and
 * distributed locks.
 * <p>
 * There is no leader to find and no session to keep. A request is sent to one peer, which runs
 * the CASPaxos round on the caller's behalf; if that peer cannot be reached the client moves to
 * the next one and retries, so any reachable member is as good as any other. Failing over is
 * therefore invisible to the caller, and a request that runs out of peers fails with
 * {@link RequestFailedException} rather than hanging.
 * <p>
 * Every operation returns a {@link CompletableFuture} and nothing blocks. All I/O and all
 * completion bookkeeping happen on a single {@link EventLoop} thread; the client either owns that
 * loop or shares one supplied by the caller (see the {@code ownsLoop} constructors), which is how
 * a client co-located with a node avoids a second thread entirely. Completions run on the loop,
 * so dependent stages attached with the non-async {@code then*} methods must not block it.
 * <p>
 * Instances are thread-safe and are meant to be long-lived and shared: connection state, the
 * pending-request table and peer rotation all live here. {@link #close()} fails every request
 * still in flight instead of leaving callers waiting on a future that can never complete.
 * <p>
 * Keys and values are {@link ByteBuffer}s; {@code String}-keyed overloads are provided throughout
 * and encode as UTF-8. Reads are linearizable by default -- {@link ReadConsistency#SERIALIZABLE}
 * trades that for a local, round-free answer that may be stale.
 */
public final class DisCasClient implements AutoCloseable, LockClientOps {
    /** Largest key the cluster accepts; a longer key is rejected before any round is started. */
    public static final int MAX_KEY_BYTES = KvLimits.MAX_KEY_BYTES;
    /** Largest value the cluster accepts; a longer value is rejected before any round is started. */
    public static final int MAX_VALUE_BYTES = KvLimits.MAX_VALUE_BYTES;

    private static final SecureRandom LOCK_TOKEN_RNG = new SecureRandom();

    private final ClientId clientId;

    private final ClientTransport transport;
    private final EventLoop loop;
    private final boolean ownsLoop;
    private final ClientObserver observer;
    private final ClusterClock clusterClock;
    private final List<NodeId> peers;

    /**
     * Consecutive failures per peer, and the instant each becomes worth trying again.
     * <p>
     * Chiefly a memory <em>between</em> calls. The client never waits a backoff out -- an
     * operation with no eligible coordinator fails and the caller decides whether to re-issue --
     * so what this buys is that the re-issued call goes straight to a coordinator that might work
     * instead of rediscovering the broken one, and that a caller retrying in a tight loop cannot
     * turn into a connect storm ({@code ensureConnected} opens a fresh socket per send and rate
     * limits nothing itself).
     * <p>
     * Both are reset the moment a peer answers, so a recovered coordinator costs nothing to
     * rediscover.
     */
    private final int[] peerFailures;
    private final long[] peerNextEligibleNanos;

    /**
     * Rejects new API calls and async-callback submissions once {@link #close()} has run. Reads and
     * writes are atomic with enqueue and drain through the monitor every public method takes.
     */
    private volatile boolean closed = false;

    private long correlationSeq = 0;
    private final Map<Long, PendingEntry> pending = new HashMap<>();
    private final Map<Long, PendingScan> pendingScans = new HashMap<>();
    private final Set<CompletableFuture<Void>> pendingDelays = ConcurrentHashMap.newKeySet();

    /**
     * Executor used for {@code *Async(..., executor)} callbacks inside this
     * client. When the client is closed, submissions throw
     * {@link ClientLifecycleException} with {@link ClientLifecycleException.Phase#ALREADY_CLOSED};
     * CompletableFuture propagates this as a CompletionException to the chained future,
     * preventing compose-chains from hanging across shutdown
     */
    private final Executor asyncCallbacks;

    // Every timing tunable lives on the config -- see DisCasClientConfig for the defaults and why
    // each one is what it is. Held as a field rather than read through the config on each use so
    // the hot paths stay a field read.
    private final Duration perAttemptTimeout;
    private final Duration requestDeadline;
    private final long peerRetryMinBackoffNanos;
    private final long peerRetryMaxBackoffNanos;
    private final Duration scanTimeout;
    private final Duration shutdownAwaitTimeout;
    private final Duration lockMinBackoff;
    private final Duration lockMaxBackoff;
    private final Duration watchMinBackoff;
    private final Duration watchMaxBackoff;

    private static final int LOCK_TOKEN_BYTES = 16;

    /** Ceiling on a lock/watch wait budget, well past any real one. See {@link #deadlineNanosFromNow}. */
    private static final long MAX_WAIT_SECONDS = TimeUnit.DAYS.toSeconds(365L);

    /**
     * Floor on the sleep taken when every coordinator is inside its backoff. Guards against a
     * zero-or-negative computed wait -- from a clock artefact or a backoff that expired between the
     * eligibility scan and the arithmetic -- turning the wait into a tight reschedule loop.
     */
    private static final long MIN_BACKOFF_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(1L);

    // Diagnostic text only. Nothing branches on these -- callers distinguish outcomes by
    // ClientErrorCode and by the exception's Phase/Cause -- so they are not API.
    private static final String ERR_CLIENT_SHUT_DOWN = "Client is shut down";
    private static final String ERR_CLIENT_SHUTTING_DOWN = "Client shutting down";
    private static final String ERR_ALL_PEERS_EXHAUSTED = "All peers exhausted on send failure";
    private static final String ERR_REQUEST_TIMED_OUT_PREFIX = "Request timed out";
    private static final String ERR_SCAN_NO_QUORUM_PREFIX = "Scan did not reach quorum (";
    private static final String ERR_INDETERMINATE_UNFENCED =
            "Unfenced write dispatched to a coordinator that did not answer; not re-sent";
    private static final String ERR_OWNER_ID_REQUIRED =
            "ownerId is required: it is what identifies this holder's own record afterwards";

    /**
     * The ordinary case: the client creates and owns its own event loop, started on the first
     * operation and shut down by {@link #close()}.
     */
    public DisCasClient(final ClientId clientId,
                     final ClientTransport transport) {
        this(clientId, transport, new EventLoop("cas-client"), true);
    }

    /**
     * @param loop the loop this client runs on. It is still owned by the client -- pass
     *             {@code ownsLoop = false} to share a loop the caller manages.
     */
    public DisCasClient(final ClientId clientId,
                     final ClientTransport transport,
                     final EventLoop loop) {
        this(clientId, transport, loop, true);
    }

    /**
     * @param ownsLoop when false the loop is externally owned -- shared with a co-located node,
     *                 say -- and the client neither starts nor shuts it down
     */
    public DisCasClient(final ClientId clientId,
                     final ClientTransport transport,
                     final EventLoop loop,
                     final boolean ownsLoop) {
        this(clientId, transport, loop, ownsLoop, ClientObserver.NONE);
    }

    /**
     * @param observer observability seam; {@link ClientObserver#NONE} reports nothing. Callbacks
     *                 fire on the loop thread and must not block.
     */
    public DisCasClient(final ClientId clientId,
                     final ClientTransport transport,
                     final EventLoop loop,
                     final boolean ownsLoop,
                     final ClientObserver observer) {
        this(clientId, transport, loop, ownsLoop, observer, DisCasClientConfig.defaults());
    }

    /**
     * @param config timing tunables. Set these when the client runs under a deadline of its own --
     *               an embedder that gives up after N seconds needs the client to settle first,
     *               or it reports its own timeout instead of the client's answer.
     */
    public DisCasClient(final ClientId clientId,
                     final ClientTransport transport,
                     final EventLoop loop,
                     final boolean ownsLoop,
                     final ClientObserver observer,
                     final DisCasClientConfig config) {
        this(clientId, transport, loop, ownsLoop, observer, config, TimeSource.SYSTEM);
    }

    /**
     * As above, reading time from {@code timeSource} instead of from the machine.
     * <p>
     * Package-private: it exists so two clients whose clocks disagree can be shown judging the
     * same lease the same way, which cannot be demonstrated by moving the machine's clock, and is
     * not something an application has any reason to do.
     */
    DisCasClient(final ClientId clientId,
                 final ClientTransport transport,
                 final EventLoop loop,
                 final boolean ownsLoop,
                 final ClientObserver observer,
                 final DisCasClientConfig config,
                 final TimeSource timeSource) {
        final DisCasClientConfig cfg = config == null ? DisCasClientConfig.defaults() : config;
        this.perAttemptTimeout = cfg.perAttemptTimeout();
        this.requestDeadline = cfg.requestDeadline();
        this.peerRetryMinBackoffNanos = cfg.peerRetryMinBackoff().toNanos();
        this.peerRetryMaxBackoffNanos = cfg.peerRetryMaxBackoff().toNanos();
        this.scanTimeout = cfg.scanTimeout();
        this.shutdownAwaitTimeout = cfg.shutdownAwaitTimeout();
        this.lockMinBackoff = cfg.lockMinBackoff();
        this.lockMaxBackoff = cfg.lockMaxBackoff();
        this.watchMinBackoff = cfg.watchMinBackoff();
        this.watchMaxBackoff = cfg.watchMaxBackoff();
        this.clientId = clientId;
        this.transport = transport;
        this.loop = loop;
        this.ownsLoop = ownsLoop;
        this.observer = observer == null ? ClientObserver.NONE : observer;
        // The clock lock leases are expressed on. Owned here because leases are a client-side
        // convention, fed by the transport because only it sees a handshake. Bound before anything
        // is sent, so no lease can be written against a clock the transport cannot yet report into.
        this.clusterClock = new ClusterClock(timeSource, this.observer);
        transport.bindClock(clusterClock);
        this.peers = transport.peers();
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("No peers available in transport");
        }
        // Per-peer retry state, indexed exactly like `peers`. Arrays rather than a map: this is
        // read on every dispatch, and the peer set is fixed for the life of the client (a nodes
        // file reload builds a whole new client, which correctly starts from a clean slate).
        // Loop-confined -- every dispatch runs on the event loop -- so no volatile is needed.
        this.peerFailures = new int[peers.size()];
        this.peerNextEligibleNanos = new long[peers.size()];

        this.asyncCallbacks = task -> {
            synchronized (this) {
                if (closed) {
                    throw new ClientLifecycleException(
                            ClientLifecycleException.Phase.ALREADY_CLOSED, ERR_CLIENT_SHUT_DOWN);
                }
                loop.execute(task);
            }
        };

        transport.register(message -> loop.execute(() -> onResponse(message)));
        // Deferred onto the loop rather than run inline. A connect that fails inside send() closes
        // its channel and reports the loss while still on the sending stack; handling it there
        // would re-dispatch the very request whose send is about to throw and be re-dispatched by
        // the catch. Running afterwards makes that entry visibly already moved on.
        transport.registerConnectionLost(peer -> loop.execute(() -> onConnectionLost(peer)));
        if (ownsLoop) {
            loop.start();
        }
    }

    private static ByteBuffer encodeStringUtf8(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void checkKeySize(final ByteBuffer key) {
        if (key.remaining() > MAX_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "Key size " + key.remaining() + " exceeds maximum " + MAX_KEY_BYTES + " bytes");
        }
    }

    private static void checkValueSize(final ByteBuffer value) {
        if (value != null && value.remaining() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException(
                    "Value size " + value.remaining() + " exceeds maximum " + MAX_VALUE_BYTES + " bytes");
        }
    }

    /** UTF-8 string-key form of {@link #get(ByteBuffer)}. */
    public CompletableFuture<GetResult> get(final String key) {
        return get(encodeStringUtf8(key));
    }

    /** UTF-8 string-key form of {@link #get(ByteBuffer, ReadConsistency)}. */
    public CompletableFuture<GetResult> get(final String key, final ReadConsistency consistency) {
        return get(encodeStringUtf8(key), consistency);
    }

    /** UTF-8 string-key form of {@link #watch(ByteBuffer, Version, Duration)}. */
    public CompletableFuture<WatchResult> watch(final String key, final Version sinceVersion,
                                                final Duration maxWait) {
        return watch(encodeStringUtf8(key), sinceVersion, maxWait);
    }

    /** UTF-8 string-key form of {@link #watch(ByteBuffer, Version, Duration, ReadConsistency)}. */
    public CompletableFuture<WatchResult> watch(final String key, final Version sinceVersion,
                                                final Duration maxWait,
                                                final ReadConsistency consistency) {
        return watch(encodeStringUtf8(key), sinceVersion, maxWait, consistency);
    }

    /** UTF-8 string-key form of {@link #put(ByteBuffer, ByteBuffer)}. */
    public CompletableFuture<Version> put(final String key,
                                          final ByteBuffer value) {
        return put(encodeStringUtf8(key), value);
    }

    /** UTF-8 string-key form of {@link #delete(ByteBuffer)}. */
    public CompletableFuture<Version> delete(final String key) {
        return delete(encodeStringUtf8(key));
    }

    /** UTF-8 string-key form of {@link #delete(ByteBuffer, Version)}. */
    public CompletableFuture<CasResult> delete(final String key,
                                                        final Version expectedVersion) {
        return delete(encodeStringUtf8(key), expectedVersion);
    }

    /** UTF-8 string-key form of {@link #tryLock(ByteBuffer, Duration, String)}. */
    public CompletableFuture<LockAcquireResult> tryLock(final String key,
                                                        final Duration leaseTtl,
                                                        final String ownerId) {
        return tryLock(encodeStringUtf8(key), leaseTtl, ownerId);
    }

    /** UTF-8 string-key form of {@link #lock(ByteBuffer, Duration, Duration, String)}. */
    public CompletableFuture<LockAcquireResult> lock(final String key,
                                                     final Duration leaseTtl,
                                                     final Duration waitTimeout,
                                                     final String ownerId) {
        return lock(encodeStringUtf8(key), leaseTtl, waitTimeout, ownerId);
    }

    /** UTF-8 string-key form of {@link #release(ByteBuffer, LockToken)}. */
    public CompletableFuture<LockWriteResult> release(final String key,
                                                      final LockToken token) {
        return release(encodeStringUtf8(key), token);
    }

    /** UTF-8 string-key form of {@link #renewLock(ByteBuffer, LockToken, Duration)}. */
    public CompletableFuture<LockWriteResult> renewLock(final String key,
                                                        final LockToken token,
                                                        final Duration newLeaseTtl) {
        return renewLock(encodeStringUtf8(key), token, newLeaseTtl);
    }

    /** UTF-8 string-key form of {@link #getLockInfo(ByteBuffer)}. */
    public CompletableFuture<LockInfoResult> getLockInfo(final String key) {
        return getLockInfo(encodeStringUtf8(key));
    }

    public CompletableFuture<GetResult> get(final ByteBuffer key) {
        return get(key, ReadConsistency.LINEARIZABLE);
    }

    /**
     * Reads {@code key} at the requested {@link ReadConsistency}, returning its value together
     * with its per-key {@link Version}. {@code LINEARIZABLE} (the default
     * {@link #get(ByteBuffer)}) runs a full Paxos round; {@code SERIALIZABLE} returns the target
     * node's locally-committed value with no round and no WAL write -- cheap but possibly stale.
     * Lock helpers always use linearizable reads regardless.
     * <p>
     * The version comes back on every read because the wire always carries it, and it is what a
     * caller needs to fence a {@link #cas(ByteBuffer, Version, ByteBuffer)} or start a
     * {@link #watch(ByteBuffer, Version, Duration, ReadConsistency)}. A caller that wants only
     * the bytes reads {@link GetResult#value()}, which is {@code null} when the key is absent or
     * tombstoned.
     */
    public CompletableFuture<GetResult> get(final ByteBuffer key, final ReadConsistency consistency) {
        return get(key, consistency, 0);
    }

    /**
     * As {@link #get(ByteBuffer, ReadConsistency)}, starting the coordinator walk at
     * {@code startAttempt} instead of at the key's home coordinator.
     * <p>
     * Only {@link #watchAttempt} passes anything but 0, and only for a serializable poll: a
     * serializable read is answered from one member's local state, so polling the same member
     * again is polling the same answer, and a member that is behind the cluster hides the change
     * for as long as the watch keeps asking it. Rotating the start walks the whole membership
     * across successive polls, which is what makes a change any reachable member holds observable.
     * <p>
     * Private, and it stays private: a caller choosing its own coordinator per request would
     * defeat coordinator affinity, which is what serializes one key behind one proposer.
     */
    private CompletableFuture<GetResult> get(final ByteBuffer key, final ReadConsistency consistency,
                                             final int startAttempt) {
        checkKeySize(key);
        final ReadConsistency level = consistency == null ? ReadConsistency.LINEARIZABLE : consistency;
        final CompletableFuture<GetResult> future = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                future.completeExceptionally(new ClientLifecycleException(
                        ClientLifecycleException.Phase.ALREADY_CLOSED, ERR_CLIENT_SHUT_DOWN));
                return future;
            }
            loop.execute(() -> {
                if (closed) {
                    future.completeExceptionally(shutdownError());
                    return;
                }
                final long correlationId = ++correlationSeq;
                final ByteBuffer routingKey = ByteBuffers.copyReadOnly(key);
                final ClientMessage msg =
                        new ClientMessage.ClientGetReq(clientId.value(), correlationId, routingKey, level);
                final PendingEntry entry = new PendingEntry(
                        future, msg, routingKey, deadlineNanosFromNow(requestDeadline), startAttempt);
                pending.put(correlationId, entry);
                sendAttempt(correlationId, entry, startAttempt);
            });
        }
        return future;
    }

    public CompletableFuture<WatchResult> watch(final ByteBuffer key, final Version sinceVersion,
                                                final Duration maxWait) {
        return watch(key, sinceVersion, maxWait, ReadConsistency.LINEARIZABLE);
    }

    /**
     * Blocking query / watch at the requested {@link ReadConsistency}: completes as soon as
     * {@code key}'s version advances past {@code sinceVersion}, or with the current (unchanged)
     * state once {@code maxWait} elapses.
     * Pass {@link Version#INITIAL} to fire on the first committed value (an existing key returns
     * immediately; an absent key blocks until it appears or the wait elapses).
     *
     * <p>Semantics are coalescing (latest-value): under rapid churn intermediate values may be
     * skipped -- discas is a CASPaxos register store and keeps no history to replay. Implemented as
     * a client-side poll with gentle backoff (the same shape as blocking lock acquire), each poll a
     * {@link #get} at {@code consistency} -- {@code LINEARIZABLE} in the default
     * {@link #watch(ByteBuffer, Version, Duration)}. Feed the returned
     * {@link WatchResult#version()} back in to continue watching.
     *
     * <p><b>A {@code SERIALIZABLE} watch rotates over the membership</b>, because a poll answered
     * from one member's local state says nothing about the cluster and asking that member again
     * says it no louder. Successive polls therefore address successive members, and the result
     * carries the <em>highest</em> version any of them returned. Two things follow. A committed
     * change is held by a majority by definition, so a watch that gets a full lap of polls in and
     * reaches enough members observes it -- a member that missed the accept broadcast can no
     * longer hide it. And {@link WatchResult#version()}, which this API asks the caller to feed
     * into the next watch, never falls below the version the caller passed in, so the chain cannot
     * walk backwards.
     *
     * <p>What a serializable watch still cannot promise is an answer inside a budget too short for
     * a lap: with {@code maxWait} on the order of one poll, which member answered is again the
     * whole story. Linearizable polling does not rotate and does not need to -- each poll is a
     * round, so a quorum already makes the versions it observes monotonic, and spreading those
     * rounds over coordinators would put two proposers on one key.
     *
     * <p>{@code maxWait} is measured on a monotonic clock, so the budget is unaffected by a
     * wall-clock/NTP adjustment during the watch.
     */
    public CompletableFuture<WatchResult> watch(final ByteBuffer key, final Version sinceVersion,
                                                final Duration maxWait,
                                                final ReadConsistency consistency) {
        checkKeySize(key);
        if (maxWait == null || maxWait.isNegative()) {
            final CompletableFuture<WatchResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException("maxWait must be >= 0"));
            return failed;
        }
        final Version since = sinceVersion == null ? Version.INITIAL : sinceVersion;
        final ReadConsistency level = consistency == null ? ReadConsistency.LINEARIZABLE : consistency;
        return watchAttempt(key.duplicate(), since, deadlineNanosFromNow(maxWait), level, 0, null);
    }

    /**
     * Writes {@code value} at {@code key}, unconditionally. Implemented as a CAS whose change
     * function ignores the current value, so it is a full consensus round like any other write
     * and is linearizable against concurrent writers -- last writer in ballot order wins.
     * <p>
     * Rejects oversized input with {@link IllegalArgumentException} <em>before</em> anything is
     * sent, so a too-large key or value never reaches the cluster.
     *
     * @return the {@link Version} this write committed at -- the key's new version, ready to fence
     *         a follow-up {@link #cas(ByteBuffer, Version, ByteBuffer)} or start a
     *         {@link #watch(ByteBuffer, Version, Duration, ReadConsistency)} on, with no read back
     */
    public CompletableFuture<Version> put(final ByteBuffer key,
                                          final ByteBuffer value) {
        checkKeySize(key);
        checkValueSize(value);
        final CompletableFuture<Version> future = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                future.completeExceptionally(new ClientLifecycleException(
                        ClientLifecycleException.Phase.ALREADY_CLOSED, ERR_CLIENT_SHUT_DOWN));
                return future;
            }
            loop.execute(() -> {
                if (closed) {
                    future.completeExceptionally(shutdownError());
                    return;
                }
                final long correlationId = ++correlationSeq;
                final ByteBuffer routingKey = ByteBuffers.copyReadOnly(key);
                final ClientMessage msg = new ClientMessage.ClientPutReq(
                        clientId.value(), correlationId, routingKey, ByteBuffers.copyReadOnly(value));
                final PendingEntry entry = new PendingEntry(
                        future, msg, routingKey, deadlineNanosFromNow(requestDeadline));
                pending.put(correlationId, entry);
                sendAttempt(correlationId, entry, 0);
            });
        }
        return future;
    }

    /**
     * Compare-and-set fenced on the key's {@link Version} instead of on its bytes.
     *
     * <p>Reach for this one when the version is something the caller already holds or already
     * knows -- from the {@link #put} that wrote it, from a {@link #watch}, or from
     * {@link Version#INITIAL}, which means "never written" and makes this a create-if-absent (see
     * {@link #putIfAbsent}). Losing there is final: the key exists, and reading again will not
     * change that. When instead the version has to be read first because the current state decides
     * what to write, the loop belongs to {@link #update} rather than to the caller.
     *
     * <p>A request is sent to one coordinator per attempt, and a coordinator the client has
     * stopped waiting for keeps driving its proposal. A CAS compared on the value re-applies
     * whenever the register happens to hold {@code expected} again, so an abandoned round landing
     * after an intervening inverse write applies the swap a second time and silently reverts the
     * writer in between. Compared on the version, the stale attempt carries a ballot the
     * intervening write has already overtaken, so the duplicate is rejected rather than applied.
     *
     * <p>That is also what makes this the one write whose unknown outcome the caller can resolve
     * alone: because a duplicate is provably a no-op, it is safe to re-send even when the client
     * cannot tell whether the first attempt landed. {@link #update} cannot promise the same -- it
     * is two round trips, and re-running it after a lost response would apply the transform twice.
     *
     * @param expectedVersion the version the caller last observed. {@link Version#INITIAL} means
     *                        "the key has no committed value", i.e. create-if-absent -- it is a
     *                        real version, not a wildcard.
     * @param desired         the value to commit; {@code null} tombstones the key
     */
    public CompletableFuture<CasResult> cas(final ByteBuffer key,
                                                     final Version expectedVersion,
                                                     final ByteBuffer desired) {
        checkKeySize(key);
        checkValueSize(desired);
        if (expectedVersion == null) {
            throw new IllegalArgumentException("expectedVersion is required");
        }
        final CompletableFuture<CasResult> future = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                future.completeExceptionally(new ClientLifecycleException(
                        ClientLifecycleException.Phase.ALREADY_CLOSED, ERR_CLIENT_SHUT_DOWN));
                return future;
            }
            loop.execute(() -> {
                if (closed) {
                    future.completeExceptionally(shutdownError());
                    return;
                }
                final long correlationId = ++correlationSeq;
                final ByteBuffer routingKey = ByteBuffers.copyReadOnly(key);
                final ClientMessage msg = new ClientMessage.ClientCasReq(
                        clientId.value(), correlationId, routingKey,
                        expectedVersion.ballot(),
                        desired == null ? null : ByteBuffers.copyReadOnly(desired));
                final PendingEntry entry = new PendingEntry(
                        future, msg, routingKey, deadlineNanosFromNow(requestDeadline));
                pending.put(correlationId, entry);
                sendAttempt(correlationId, entry, 0);
            });
        }
        return future;
    }

    /** UTF-8 overload of {@link #cas(ByteBuffer, Version, ByteBuffer)}. */
    public CompletableFuture<CasResult> cas(final String key,
                                                     final Version expectedVersion,
                                                     final String desired) {
        return cas(encodeStringUtf8(key), expectedVersion,
                desired == null ? null : encodeStringUtf8(desired));
    }

    /**
     * UTF-8 <em>key</em> form of {@link #cas(ByteBuffer, Version, ByteBuffer)}, for callers whose
     * key is text and whose value is opaque bytes -- an HTTP body, say.
     */
    public CompletableFuture<CasResult> cas(final String key,
                                                     final Version expectedVersion,
                                                     final ByteBuffer desired) {
        return cas(encodeStringUtf8(key), expectedVersion, desired);
    }

    /**
     * Read, transform, write, and on a lost compare read again -- the loop a version-fenced
     * {@link #cas(ByteBuffer, Version, ByteBuffer)} otherwise leaves to the caller.
     * <b>This is the write to reach for</b> whenever the current state decides anything: what
     * value to write, or whether to write at all.
     *
     * <p>Each attempt is a {@code LINEARIZABLE} {@link #get(ByteBuffer, ReadConsistency)} followed
     * by a {@code cas} fenced on the version that read observed. A lost compare means somebody
     * else committed in between, so the loop reads again and re-applies {@code transform} to what
     * it finds -- it never re-sends a version it already knows is stale. The read is linearizable
     * because a stale one can lose the same compare forever.
     *
     * <p><b>Only a lost compare is retried.</b> A timeout, a refusal, or any other failure
     * completes the returned future as it is, without another attempt. This is the difference
     * between {@code update} and {@code cas}, and it is not a limitation: a {@code cas} whose
     * outcome is unknown is safe to re-send, because the duplicate carries an overtaken ballot and
     * is provably a no-op. An {@code update} is two round trips and can offer no such thing --
     * re-running {@code v -> v + 1} after a lost <em>response</em> would increment twice. When the
     * outcome is unknown the caller is the only one who can decide, so it is told.
     *
     * <p>{@code transform} sees the current value, or {@code null} when the key is absent or
     * tombstoned, and returns the value to commit -- {@code null} tombstones it. To abandon the
     * update, throw from {@code transform}: the exception completes the returned future and
     * nothing is written. There is no sentinel for "leave it alone", and returning the same bytes
     * is a write like any other.
     *
     * <p>{@code transform} runs on the client's callback executor, never on the event loop, and
     * may be called more than once. The buffer handed to it is read-only and valid only for the
     * duration of the call; copy anything that has to outlive it.
     *
     * @param retryBudget how long to keep losing compares before giving up with
     *                    {@link UpdateContendedException}. It bounds the loop, not the individual
     *                    round trips -- each of those has the client's own request deadline -- and
     *                    is checked between attempts, so one full attempt always happens.
     * @return the value committed and the {@link Version} it committed at
     */
    public CompletableFuture<GetResult> update(final ByteBuffer key,
                                               final UnaryOperator<ByteBuffer> transform,
                                               final Duration retryBudget) {
        checkKeySize(key);
        if (transform == null) {
            throw new IllegalArgumentException("transform is required");
        }
        if (retryBudget == null || retryBudget.isNegative()) {
            throw new IllegalArgumentException("retryBudget must be >= 0");
        }
        return updateAttempt(ByteBuffers.copyReadOnly(key), transform,
                deadlineNanosFromNow(retryBudget), 1);
    }

    /**
     * {@link #update(ByteBuffer, UnaryOperator, Duration)} with the client's configured request
     * deadline as the retry budget -- the same budget one round trip is allowed, spent on the
     * whole loop instead.
     */
    public CompletableFuture<GetResult> update(final ByteBuffer key,
                                               final UnaryOperator<ByteBuffer> transform) {
        return update(key, transform, requestDeadline);
    }

    /**
     * UTF-8 form of {@link #update(ByteBuffer, UnaryOperator, Duration)}: {@code transform} sees
     * the decoded value, or {@code null} when the key is absent or tombstoned, and returning
     * {@code null} tombstones it.
     */
    public CompletableFuture<GetResult> update(final String key,
                                               final UnaryOperator<String> transform,
                                               final Duration retryBudget) {
        if (transform == null) {
            throw new IllegalArgumentException("transform is required");
        }
        return update(encodeStringUtf8(key), utf8Transform(transform), retryBudget);
    }

    /** UTF-8 form of {@link #update(ByteBuffer, UnaryOperator)}. */
    public CompletableFuture<GetResult> update(final String key,
                                               final UnaryOperator<String> transform) {
        return update(key, transform, requestDeadline);
    }

    private static UnaryOperator<ByteBuffer> utf8Transform(final UnaryOperator<String> transform) {
        return current -> {
            final String next = transform.apply(
                    current == null ? null : StandardCharsets.UTF_8.decode(current).toString());
            return next == null ? null : encodeStringUtf8(next);
        };
    }

    /**
     * One read-transform-write. Recursive rather than looping because every step is asynchronous;
     * the shape is {@link #lockAttempt}'s, minus the backoff: a lost compare means somebody else
     * just committed, so the register has already moved and there is nothing to wait out.
     */
    private CompletableFuture<GetResult> updateAttempt(final ByteBuffer key,
                                                       final UnaryOperator<ByteBuffer> transform,
                                                       final long deadlineNanos,
                                                       final int attempt) {
        return get(key.duplicate(), ReadConsistency.LINEARIZABLE).thenComposeAsync(current -> {
            final ByteBuffer desired = transform.apply(ByteBuffers.readOnly(current.value()));
            checkValueSize(desired);
            return cas(key.duplicate(), current.version(), desired).thenComposeAsync(result -> {
                if (result.swapped()) {
                    return CompletableFuture.completedFuture(
                            new GetResult(result.value(), result.version()));
                }
                if (elapsed(deadlineNanos)) {
                    return CompletableFuture.failedFuture(
                            new UpdateContendedException(attempt, result.version()));
                }
                return updateAttempt(key, transform, deadlineNanos, attempt + 1);
            }, asyncCallbacks);
        }, asyncCallbacks);
    }

    /**
     * Writes {@code value} at {@code key} only if the key has never been written --
     * {@link #cas(ByteBuffer, Version, ByteBuffer)} against {@link Version#INITIAL}, under the
     * name that says what it is for.
     * <p>
     * Not a shorthand for {@link #update}: losing here is final, not a reason to read again. The
     * key exists, and no amount of retrying will make it not exist. The {@link CasResult} carries
     * the winner's value and version, so a caller that lost already knows who took the key without
     * a second round trip.
     */
    public CompletableFuture<CasResult> putIfAbsent(final ByteBuffer key,
                                                    final ByteBuffer value) {
        return cas(key, Version.INITIAL, value);
    }

    /** UTF-8 form of {@link #putIfAbsent(ByteBuffer, ByteBuffer)}. */
    public CompletableFuture<CasResult> putIfAbsent(final String key,
                                                    final String value) {
        return cas(key, Version.INITIAL, value);
    }

    /**
     * UTF-8 <em>key</em> form of {@link #putIfAbsent(ByteBuffer, ByteBuffer)}, for callers whose
     * key is text and whose value is opaque bytes.
     */
    public CompletableFuture<CasResult> putIfAbsent(final String key,
                                                    final ByteBuffer value) {
        return cas(key, Version.INITIAL, value);
    }

    /**
     * Deletes {@code key} only if it is still at {@code expectedVersion} -- the delete form of
     * {@link #cas(ByteBuffer, Version, ByteBuffer)}, and preferred over the unfenced
     * {@link #delete(ByteBuffer)} for the same reason: the fence is what makes the operation safe
     * to re-send when a coordinator stops answering. When whether to delete depends on what the
     * key currently holds, the read-decide-write loop belongs to {@link #update}, which tombstones
     * on a {@code null} from its transform.
     * <p>
     * An unfenced {@link #delete(ByteBuffer)} left indeterminate can land after a later writer has
     * recreated the key, tombstoning a value the caller never saw. Fenced on the version, the
     * abandoned attempt carries a ballot the recreate has already overtaken, so it is rejected
     * rather than applied.
     * <p>
     * The delete is a version-fenced CAS to a tombstone and answers as one: {@code swapped()} is
     * true when this call removed the value, false when the key had moved on -- and then
     * {@code version()} is the version that won, ready to feed back into a re-read.
     * Deleting a key that is already tombstoned at {@code expectedVersion} still advances the
     * version, exactly as {@link #delete(ByteBuffer)} does.
     *
     * @param expectedVersion the version the caller last observed; {@link Version#INITIAL} means
     *                        "the key has no committed value", so the delete applies only while
     *                        the key has never been written
     */
    public CompletableFuture<CasResult> delete(final ByteBuffer key,
                                                        final Version expectedVersion) {
        return cas(key, expectedVersion, null);
    }

    /**
     * Deletes {@code key}. The key is tombstoned rather than removed: a tombstone is what stops
     * anti-entropy from resurrecting the value off a replica that has not seen the delete yet, so
     * the key stays visible to {@link #get} with a null value and an advanced version.
     * <p>
     * Unfenced, so an indeterminate outcome stays indeterminate; prefer
     * {@link #delete(ByteBuffer, Version)} where the caller has a version to fence on.
     *
     * @return the {@link Version} the tombstone committed at -- a delete advances the version like
     *         any other commit, so this is where a watcher of the deleted key resumes from
     */
    public CompletableFuture<Version> delete(final ByteBuffer key) {
        checkKeySize(key);
        final CompletableFuture<Version> future = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                future.completeExceptionally(new ClientLifecycleException(
                        ClientLifecycleException.Phase.ALREADY_CLOSED, ERR_CLIENT_SHUT_DOWN));
                return future;
            }
            loop.execute(() -> {
                if (closed) {
                    future.completeExceptionally(shutdownError());
                    return;
                }
                final long correlationId = ++correlationSeq;
                final ByteBuffer routingKey = ByteBuffers.copyReadOnly(key);
                final ClientMessage msg =
                        new ClientMessage.ClientDeleteReq(clientId.value(), correlationId, routingKey);
                final PendingEntry entry = new PendingEntry(
                        future, msg, routingKey, deadlineNanosFromNow(requestDeadline));
                pending.put(correlationId, entry);
                sendAttempt(correlationId, entry, 0);
            });
        }
        return future;
    }

    /**
     * Enumerate the keys visible to this client, merged across a quorum of nodes.
     * <strong>This is a key enumeration, not a snapshot and not a value read.</strong>
     * <p>
     * Unlike every other operation, {@code scan} is aggregated by the <em>client</em>: each node
     * answers from its local store with no consensus round. The future completes as soon as
     * {@code N/2 + 1} nodes have answered (faster than waiting for all {@code N}) and completes
     * <em>exceptionally</em> below that threshold -- it never silently returns a short list.
     * <p>
     * <strong>Guarantees:</strong> every key whose commit completed before the scan started will
     * appear. This is quorum intersection: a committed key had its accept quorum on a majority,
     * and any two majorities share a node.
     * <p>
     * <strong>Does NOT guarantee:</strong>
     * <ul>
     *   <li><em>Cross-key atomicity.</em> Different keys may reflect different instants. CASPaxos
     *       has no log and no global revision, so a consistent snapshot is undefinable, not merely
     *       unimplemented.</li>
     *   <li><em>Committed values.</em> {@link ScanResult} exposes {@code key()} and
     *       {@code version()} only -- no value. A node reports what it has
     *       <em>accepted</em>, and an accepted-but-not-chosen ballot can still be superseded, so a
     *       key mid-CAS on a minority may appear and later vanish. Treat the result as "keys that
     *       exist or are being created"; use {@code version()} as a watch position and {@code get(key)}
     *       when you need a committed value.</li>
     *   <li><em>Concurrent mutations</em> may or may not be reflected, with no way to tell which.</li>
     * </ul>
     * Tombstoned and promise-only keys are excluded. Entries are sorted by key.
     * <p>
     * <strong>Requires a sufficient node list.</strong> {@code N} is reported by the nodes, not
     * derived from the configured peer list, so a client configured with fewer than
     * {@code N/2 + 1} nodes fails here rather than under-counting the quorum. This is the only
     * operation for which the client's node list is load-bearing: every other call routes through
     * a single coordinator that itself runs a round across all {@code N}.
     * See {@link ClientTransport#clusterSize()}.
     *
     * @return one already-complete {@link ScanPage} holding the merged, key-sorted entries; fails
     *         with {@link RuntimeException} if fewer than a majority of nodes respond within the
     *         scan timeout
     */
    public CompletableFuture<ScanPage> scan() {
        return scan(ByteBuffers.EMPTY);
    }

    /** Auto-paging enumeration of the keys under {@code prefix} (UTF-8). */
    public CompletableFuture<ScanPage> scan(final String prefix) {
        return scan(encodeStringUtf8(prefix));
    }

    /** Auto-paging enumeration under {@code prefix} (UTF-8) at the given {@code coverage}. */
    public CompletableFuture<ScanPage> scan(final String prefix,
                                            final ScanCoverage coverage) {
        return scan(encodeStringUtf8(prefix), coverage);
    }

    /**
     * Auto-paging enumeration of the keys under {@code prefix}: walks every page internally and
     * returns the whole key set as a single already-complete {@link ScanPage}.
     * <p>
     * Convenient, but the entire result is held in memory and the walk spans multiple round
     * trips, so the completeness guarantee applies <em>per page</em> rather than to the whole
     * result -- a key created mid-walk, sorting before the current cursor, will not appear. Use
     * {@link #scan(ByteBuffer, ByteBuffer, int)} to bound memory or to drive paging yourself.
     * <p>
     * {@link ScanPage#respondedNodes()} is the weakest page's count, so
     * {@link ScanPage#quorumReached()} is true only if <em>every</em> page reached one: a walk is
     * no more trustworthy than the flimsiest page in it.
     */
    public CompletableFuture<ScanPage> scan(final ByteBuffer prefix) {
        return scan(prefix, ScanCoverage.QUORUM);
    }

    /** Auto-paging enumeration under {@code prefix} at the given {@code coverage}. */
    public CompletableFuture<ScanPage> scan(final ByteBuffer prefix,
                                            final ScanCoverage coverage) {
        // Copied here rather than deeper down: one prefix is reused by every page of the walk,
        // which outlives this call, so it must not be the caller's mutable buffer.
        return scanFrom(ByteBuffers.copyReadOnly(prefix), null, new ArrayList<>(), coverage,
                Integer.MAX_VALUE, 0);
    }

    /** Recursively drain pages into {@code accumulated} until the cursor runs out. */
    private CompletableFuture<ScanPage> scanFrom(final ByteBuffer prefix,
                                                 final ByteBuffer startAfter,
                                                 final List<ScanResult> accumulated,
                                                 final ScanCoverage coverage,
                                                 final int weakestResponded,
                                                 final int clusterSize) {
        return scanPage(prefix, startAfter, KvLimits.MAX_SCAN_LIMIT, coverage)
                .thenComposeAsync(page -> {
                    accumulated.addAll(page.results());
                    final int responded = Math.min(weakestResponded, page.respondedNodes());
                    final int size = Math.max(clusterSize, page.clusterSize());
                    if (page.complete()) {
                        return CompletableFuture.completedFuture(
                                new ScanPage(accumulated, null, responded, size));
                    }
                    return scanFrom(prefix, page.nextCursor(), accumulated, coverage,
                            responded, size);
                }, asyncCallbacks);
    }

    /**
     * One bounded page of the key enumeration, starting after {@code startAfter}.
     * <p>
     * Resume with {@link ScanPage#nextCursor()} until {@link ScanPage#complete()}. Each page is
     * an independent quorum read, so the guarantees in {@link #scan()} apply per page; the
     * keyspace may change between pages.
     *
     * @param prefix     keys must start with these bytes; empty matches all
     * @param startAfter exclusive lower bound, or {@code null} for the first page
     * @param limit      maximum entries; clamped by the node to {@link KvLimits#MAX_SCAN_LIMIT},
     *                   and a page is additionally cut at {@link KvLimits#MAX_SCAN_PAGE_BYTES}
     */
    public CompletableFuture<ScanPage> scan(final ByteBuffer prefix,
                                            final ByteBuffer startAfter,
                                            final int limit) {
        return scan(prefix, startAfter, limit, ScanCoverage.QUORUM);
    }

    /**
     * One page of the key enumeration at the given {@code coverage}.
     * <p>
     * {@link ScanCoverage#QUORUM} is the default elsewhere because it is the only setting whose
     * result carries a guarantee. {@link ScanCoverage#ANY_AVAILABLE} returns whatever answered and
     * labels how much that was, for cases where an incomplete listing beats none -- inspecting a
     * cluster that has lost quorum, or a client holding only a subset of the membership. Check
     * {@link ScanPage#quorumReached()} before treating such a page as the whole key set.
     */
    public CompletableFuture<ScanPage> scan(final ByteBuffer prefix,
                                            final ByteBuffer startAfter,
                                            final int limit,
                                            final ScanCoverage coverage) {
        return scanPage(ByteBuffers.copyReadOnly(prefix),
                ByteBuffers.copyReadOnly(startAfter),
                limit, coverage);
    }

    private CompletableFuture<ScanPage> scanPage(final ByteBuffer prefix,
                                                 final ByteBuffer startAfter,
                                                 final int limit,
                                                 final ScanCoverage coverage) {
        final CompletableFuture<ScanPage> future = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                future.completeExceptionally(new ClientLifecycleException(
                        ClientLifecycleException.Phase.ALREADY_CLOSED, ERR_CLIENT_SHUT_DOWN));
                return future;
            }
            loop.execute(() -> {
                if (closed) {
                    future.completeExceptionally(shutdownError());
                    return;
                }
                final long correlationId = ++correlationSeq;
                final PendingScan pendingScan = new PendingScan(correlationId, future, coverage);
                pendingScans.put(correlationId, pendingScan);

                pendingScan.timerHandle = loop.schedule(scanTimeout, () -> {
                    final PendingScan removed = pendingScans.remove(correlationId);
                    if (removed == null || removed.future.isDone()) {
                        return;
                    }
                    final int quorum = scanQuorum();
                    final boolean haveQuorum = quorum > 0 && removed.respondedCount() >= quorum;
                    // ANY_AVAILABLE still needs someone to have answered: a scan that reached
                    // nobody is not a partial result, it is no result, and an empty page would be
                    // indistinguishable from an empty cluster.
                    final boolean acceptPartial = removed.coverage == ScanCoverage.ANY_AVAILABLE
                            && removed.respondedCount() > 0;
                    if (haveQuorum || acceptPartial) {
                        if (!haveQuorum) {
                            observer.scanIncomplete(
                                    removed.respondedCount(), transport.clusterSize());
                        }
                        removed.future.complete(removed.mergeResults(transport.clusterSize()));
                    } else {
                        removed.future.completeExceptionally(scanNoQuorum(removed, quorum));
                    }
                });

                final ClientMessage.ClientScanReq request = new ClientMessage.ClientScanReq(
                        clientId.value(), correlationId, prefix, startAfter, limit);
                for (int peerIndex = 0; peerIndex < peers.size(); peerIndex++) {
                    final NodeId peer = peers.get(peerIndex);
                    try {
                        transport.send(peer, request);
                    } catch (final Exception e) {
                        // Tolerate individual send failures, but remember them: a peer that could
                        // not be reached will never answer, so it must not hold the scan open.
                        pendingScan.unreachableCount++;
                        observer.sendFailed(peer, e);
                    }
                }
                // Every peer already unreachable: settle now rather than wait out the timeout.
                maybeCompleteScan(correlationId, pendingScan);
            });
        }
        return future;
    }

    /**
     * Complete a scan if it can be decided now: a majority answered, or -- once no further answer
     * can arrive -- whatever the caller's {@link ScanCoverage} will accept.
     * <p>
     * QUORUM completes as soon as a majority is in, without waiting for stragglers: extra answers
     * cannot change the guarantee, only the latency. ANY_AVAILABLE cannot finish on a partial set
     * while answers may still be coming, since each one widens coverage -- so it waits until every
     * peer has answered or proved unreachable, and otherwise settles at the scan timeout.
     */
    private void maybeCompleteScan(final long correlationId, final PendingScan pendingScan) {
        if (pendingScan.future.isDone()) {
            return;
        }
        final int quorum = scanQuorum();
        final boolean haveQuorum = quorum > 0 && pendingScan.respondedCount() >= quorum;
        final boolean settled = pendingScan.settled(peers.size());
        if (!haveQuorum && !settled) {
            return; // more answers may still arrive
        }
        if (!haveQuorum
                && !(pendingScan.coverage == ScanCoverage.ANY_AVAILABLE
                     && pendingScan.respondedCount() > 0)) {
            // Settled without a quorum and the caller will not accept a partial result: fail now
            // rather than hold the caller until the timeout for an answer that cannot improve.
            pendingScans.remove(correlationId);
            if (pendingScan.timerHandle != null) {
                pendingScan.timerHandle.cancel();
            }
            pendingScan.future.completeExceptionally(scanNoQuorum(pendingScan, quorum));
            return;
        }
        pendingScans.remove(correlationId);
        if (pendingScan.timerHandle != null) {
            pendingScan.timerHandle.cancel();
        }
        if (!haveQuorum) {
            observer.scanIncomplete(pendingScan.respondedCount(), transport.clusterSize());
        }
        pendingScan.future.complete(pendingScan.mergeResults(transport.clusterSize()));
    }

    private RequestFailedException scanNoQuorum(final PendingScan pendingScan, final int quorum) {
        return new RequestFailedException(
                RequestFailedException.Cause.SCAN_NO_QUORUM,
                ERR_SCAN_NO_QUORUM_PREFIX + pendingScan.respondedCount()
                        + " of " + peers.size() + " configured nodes responded, "
                        + (quorum > 0
                            ? "quorum is " + quorum
                            : "cluster size unknown -- no node was reached")
                        + ")");
    }

    /**
     * The number of scan responses that constitute a majority of the cluster, or {@code 0} if the
     * authoritative cluster size is not known yet (no node reached).
     * <p>
     * Derived from {@link ClientTransport#clusterSize()} -- the {@code N} the nodes report -- and
     * <b>not</b> from the configured peer list: a client configured with a subset of the cluster
     * would otherwise compute a majority of its own list and accept a set of responses too small
     * to intersect a committing quorum.
     */
    private int scanQuorum() {
        final int n = transport.clusterSize();
        return n < 1 ? 0 : n / 2 + 1;
    }

    /**
     * Attempts to take the lock at {@code key} for {@code leaseTtl} in the name of
     * {@code ownerId}, and returns immediately either way -- see {@link #lock} to wait for a
     * contended lock.
     *
     * <p><b>The owner id is what makes an acquire recoverable, and the caller has to supply it.</b>
     * An acquire is a write, so its outcome can be unknown: the round may have committed and the
     * answer been lost. Nothing else in the record can settle that afterwards -- the token is
     * generated here and never reaches a caller whose acquire failed, and the generation is not
     * known in advance either. The owner id is the one field the caller chooses <em>before</em>
     * the write, so it is the only thing a later read can be compared against. That is why there
     * is no overload that invents one: an id the caller never saw cannot answer the question it
     * exists for.
     *
     * <p>When the key already holds a live lease in this same name the result is
     * {@link LockAcquireStatus#HELD_BY_SELF} and nothing is written. Turn that into a usable lock
     * with {@link #recoverLock(ByteBuffer, String)}; retrying the acquire would only wait out a
     * lease the caller already owns.
     *
     * @param ownerId names <b>one holder</b>, and must be unique among everything that can hold
     *                this key at the same time -- across processes, and across concurrent
     *                acquires within one process. Two holders sharing an id each see the other's
     *                lease as their own, which is the one way to lose mutual exclusion here. It
     *                is still not a credential: only the {@link LockToken} decides who may
     *                release or renew.
     */
    public CompletableFuture<LockAcquireResult> tryLock(
            final ByteBuffer key,
            final Duration leaseTtl,
            final String ownerId) {
        checkKeySize(key);
        if (leaseTtl == null || leaseTtl.isZero() || leaseTtl.isNegative()) {
            return failedAcquire(new IllegalArgumentException("leaseTtl must be > 0"));
        }
        if (ownerId == null || ownerId.isEmpty()) {
            return failedAcquire(new IllegalArgumentException(ERR_OWNER_ID_REQUIRED));
        }

        final ByteBuffer keyCopy = key.duplicate();
        // Anchored before the read that starts the acquire, so a lock handed out below reports a
        // little less time left than the record grants it. See DistributedLock.
        final long requestedAtNanos = clusterClock.monotonicNanos();
        // Versioned, not plain: the acquire below is fenced on the version this read observed.
        // Fencing on the record's bytes would rest on the generation inside them being monotonic --
        // a property of the payload rather than of the operation.
        return get(keyCopy, ReadConsistency.LINEARIZABLE).thenComposeAsync(current -> {
            final ByteBuffer currentValue = current.value();
            // Corrected time, not this client's own: the deadline written below is judged by
            // whichever client reads it next, so both have to be measuring against the cluster.
            final long now = clusterClock.nowMillis();
            final LockValueCodec.LockRecord currentRecord =
                    currentValue != null ? LockValueCodec.decode(currentValue) : null;
            if (currentValue != null && currentRecord == null) {
                return CompletableFuture.completedFuture(
                        LockAcquireResult.notLockRecord());
            }
            if (isLive(currentRecord, now)) {
                final LockInfo held = LockInfo.fromRecord(currentRecord, now);
                // Named apart rather than lumped into HELD_BY_OTHER: a lease standing in the
                // caller's own name is almost always its own acquire coming back, and reporting
                // that as somebody else's is what sends a caller into a wait it cannot win.
                return CompletableFuture.completedFuture(
                        ownerId.equals(currentRecord.ownerId())
                                ? LockAcquireResult.heldBySelf(held)
                                : LockAcquireResult.heldByOther(held));
            }

            final ByteBuffer token = randomToken();
            final long nextGeneration = (currentRecord == null) ? 1L : currentRecord.generation() + 1L;
            final LockValueCodec.LockRecord nextRecord = new LockValueCodec.LockRecord(
                    ownerId, token, now, now + leaseTtl.toMillis(), nextGeneration);
            final ByteBuffer desired = LockValueCodec.encode(nextRecord);

            return cas(keyCopy.duplicate(), current.version(), desired).thenComposeAsync(result -> {
                if (result.swapped()) {
                    final LockToken lockToken = new LockToken(token);
                    final LockInfo info = LockInfo.fromRecord(nextRecord, now);
                    final DistributedLock lock = new DistributedLock(
                            keyCopy.duplicate(), lockToken, info, this, clusterClock,
                            requestedAtNanos + leaseTtl.toNanos());
                    return CompletableFuture.completedFuture(
                            LockAcquireResult.acquired(lock));
                }
                // The compare was lost, so somebody committed between the read and the write.
                // Report who, which needs a second look: the value that won came back with the
                // refusal as raw bytes, and this says what they mean as a lock.
                return getLockInfo(keyCopy.duplicate()).thenApply(info -> {
                    if (info.status() == LockInfoStatus.NOT_LOCK_RECORD) {
                        return LockAcquireResult.notLockRecord();
                    }
                    final LockInfo winner = info.info();
                    if (winner != null && ownerId.equals(winner.ownerId())
                            && info.status() == LockInfoStatus.LOCKED) {
                        return LockAcquireResult.heldBySelf(winner);
                    }
                    return LockAcquireResult.heldByOther(winner);
                });
            }, asyncCallbacks);
        }, asyncCallbacks);
    }

    /**
     * Takes the lock at {@code key} for {@code leaseTtl} in the name of {@code ownerId}, retrying
     * with backoff until it is won or {@code waitTimeout} elapses; on expiry the result is
     * {@link LockAcquireStatus#TIMED_OUT} rather than a failed future. Waiting is client-side
     * polling -- the cluster keeps no wait queue, so there is no fairness between contenders.
     *
     * <p>Waiting stops early on {@link LockAcquireStatus#HELD_BY_SELF}: the lease in the way is
     * the caller's own, so no amount of waiting can win it, and spending the budget would turn a
     * recoverable acquire into a timeout that says nothing.
     *
     * <p>{@code waitTimeout} is measured on a monotonic clock, so the budget is unaffected by a
     * wall-clock/NTP adjustment while the acquire is retrying. The lease itself is not: it is an
     * epoch timestamp every client compares against its own wall clock (see
     * {@link #getLockInfo(ByteBuffer)}).
     *
     * <p>A zero {@code waitTimeout} is {@link #tryLock(ByteBuffer, Duration, String)}, and is
     * routed straight to it -- accepted rather than rejected because a caller spending down a
     * budget arrives at zero honestly, and answered with the precise refusal a single attempt
     * gives rather than the {@code TIMED_OUT} a wait would report.
     *
     * @param ownerId names one holder and must be unique among concurrent holders of this key;
     *                see {@link #tryLock(ByteBuffer, Duration, String)} for why it is required.
     */
    public CompletableFuture<LockAcquireResult> lock(
            final ByteBuffer key,
            final Duration leaseTtl,
            final Duration waitTimeout,
            final String ownerId) {
        checkKeySize(key);
        if (waitTimeout == null || waitTimeout.isNegative()) {
            return failedAcquire(new IllegalArgumentException("waitTimeout must be >= 0"));
        }
        if (ownerId == null || ownerId.isEmpty()) {
            return failedAcquire(new IllegalArgumentException(ERR_OWNER_ID_REQUIRED));
        }
        if (waitTimeout.isZero()) {
            // A wait of nothing is a single attempt, which already has a name and a sharper answer:
            // HELD_BY_OTHER or HELD_BY_SELF rather than a TIMED_OUT that took an extra read to say
            // less. Delegating keeps the two forms from drifting into two behaviours.
            return tryLock(key, leaseTtl, ownerId);
        }
        return lockAttempt(key.duplicate(), leaseTtl, deadlineNanosFromNow(waitTimeout), ownerId);
    }

    /**
     * Turns a lock already standing in {@code ownerId}'s name back into a usable
     * {@link io.github.green4j.discas.client.lock.Lock Lock} --
     * the recovery for an acquire whose outcome was never reported.
     *
     * <p>The acquire may well have committed with its answer lost on the way back, and a lock
     * nobody knows they hold is held until its lease runs out. This reads the key and, if the
     * live record is the caller's, rebuilds the lock from it: same token, same generation, so the
     * returned lock releases and renews exactly like the one the acquire would have handed over.
     * {@link LockAcquireStatus#NOT_HELD} means the acquire did not land and may simply be issued
     * again.
     *
     * <p><b>This is where the owner id is trusted.</b> {@link #tryLock} deliberately hands back no
     * lock on {@link LockAcquireStatus#HELD_BY_SELF}, because a shared id there would silently
     * give two callers the same lease. Calling this is the caller stating that the id is its own,
     * so the uniqueness contract on the acquire methods is what makes it sound.
     *
     * <p>The remaining lease is measured from now rather than from the original acquire, which
     * understates it by however long the record has already been standing -- the safe direction,
     * the same one a fresh acquire rounds towards.
     */
    public CompletableFuture<LockAcquireResult> recoverLock(final ByteBuffer key,
                                                            final String ownerId) {
        checkKeySize(key);
        if (ownerId == null || ownerId.isEmpty()) {
            return failedAcquire(new IllegalArgumentException(ERR_OWNER_ID_REQUIRED));
        }
        final ByteBuffer keyCopy = key.duplicate();
        final long requestedAtNanos = clusterClock.monotonicNanos();
        return get(keyCopy, ReadConsistency.LINEARIZABLE).thenApplyAsync(current -> {
            final ByteBuffer currentValue = current.value();
            if (currentValue == null) {
                return LockAcquireResult.notHeld();
            }
            final LockValueCodec.LockRecord record = LockValueCodec.decode(currentValue);
            if (record == null) {
                return LockAcquireResult.notLockRecord();
            }
            final long now = clusterClock.nowMillis();
            if (!isLive(record, now)) {
                // Released, or lapsed and so anyone's to take: there is no lease left to hand
                // back even when the name on it is ours.
                return LockAcquireResult.notHeld();
            }
            final LockInfo info = LockInfo.fromRecord(record, now);
            if (!ownerId.equals(record.ownerId())) {
                return LockAcquireResult.heldByOther(info);
            }
            final DistributedLock lock = new DistributedLock(
                    keyCopy.duplicate(), new LockToken(record.token()), info, this, clusterClock,
                    requestedAtNanos + TimeUnit.MILLISECONDS.toNanos(
                            record.leaseUntilEpochMs() - now));
            return LockAcquireResult.acquired(lock);
        }, asyncCallbacks);
    }

    /** UTF-8 string-key form of {@link #recoverLock(ByteBuffer, String)}. */
    public CompletableFuture<LockAcquireResult> recoverLock(final String key,
                                                            final String ownerId) {
        return recoverLock(encodeStringUtf8(key), ownerId);
    }

    /**
     * Whether {@code record} is a lease somebody is holding right now: present, not a release
     * marker, and not yet lapsed. The one test that decides whether a key is free to take.
     */
    private static boolean isLive(final LockValueCodec.LockRecord record, final long now) {
        return record != null
                && !record.isReleased()
                && record.leaseUntilEpochMs() > now;
    }

    private static CompletableFuture<LockAcquireResult> failedAcquire(final RuntimeException why) {
        final CompletableFuture<LockAcquireResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(why);
        return failed;
    }

    /**
     * Releases the lock at {@code key} if {@code token} is still the holder. Writes a release
     * marker rather than deleting, so the key's fencing generation survives and stays strictly
     * monotonic across release/re-acquire cycles.
     * <p>
     * An expired lease still releases: nobody has taken over while the token matches, and writing
     * the marker is the right cleanup either way. Only being displaced, or the key holding
     * something that is not this lock, stops it -- and the {@link LockWriteResult} says which.
     * <p>
     * Safe to retry after an answer that never arrived. The marker carries the releasing token, so
     * a second attempt that finds its own marker comes back
     * {@link LockWriteStatus#ALREADY_RELEASED} rather than {@link LockWriteStatus#NOT_HELD}: the
     * retry learns that the first attempt landed, instead of getting the answer a key that was
     * never locked would have given.
     */
    @Override
    public CompletableFuture<LockWriteResult> release(
            final ByteBuffer key,
            final LockToken token) {
        checkKeySize(key);
        if (token == null) {
            final CompletableFuture<LockWriteResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException("token is required"));
            return failed;
        }
        final ByteBuffer keyCopy = key.duplicate();
        return get(keyCopy, ReadConsistency.LINEARIZABLE).thenComposeAsync(current -> {
            final LockWriteResult refusal = refuseUnlessHolder(current.value(), token, false);
            if (refusal != null) {
                return CompletableFuture.completedFuture(refusal);
            }
            final LockValueCodec.LockRecord currentRecord = LockValueCodec.decode(current.value());
            // Write a released marker (not null) so the per-key generation
            // persists across release/re-acquire cycles. The marker decodes
            // as UNLOCKED for getLockInfo and is recognised by tryLock as a
            // freshly-acquirable slot.
            final LockValueCodec.LockRecord releasedMarker =
                    LockValueCodec.LockRecord.releasedMarker(currentRecord);
            final ByteBuffer desired = LockValueCodec.encode(releasedMarker);
            return cas(keyCopy.duplicate(), current.version(), desired)
                    .thenApply(this::lockWriteOutcome);
        }, asyncCallbacks);
    }

    /**
     * The half of a release or a renew that is the same for both: what the key had to hold for the
     * fenced write to be worth attempting. Returns the refusal to complete with, or {@code null}
     * when the caller is still the holder and the write should go ahead.
     *
     * @param leaseMustBeLive whether a lapsed lease of the caller's own is a refusal. It is for a
     *                        renew -- a waiter was already entitled to take over -- and is not for
     *                        a release, which is cleanup.
     */
    private LockWriteResult refuseUnlessHolder(final ByteBuffer currentValue,
                                               final LockToken token,
                                               final boolean leaseMustBeLive) {
        if (currentValue == null) {
            return LockWriteResult.notHeld();
        }
        final LockValueCodec.LockRecord record = LockValueCodec.decode(currentValue);
        if (record == null) {
            return LockWriteResult.notLockRecord();
        }
        final long now = clusterClock.nowMillis();
        if (record.isReleased()) {
            // The marker remembers its writer, so a retry of a release whose answer was lost is
            // told that it landed instead of being lumped in with "no lock here". A marker under
            // any other token cannot say that much: it proves the key has moved on, not whether
            // this caller's own release got in first.
            return record.token().equals(token.bytes())
                    ? LockWriteResult.alreadyReleased(LockInfo.fromRecord(record, now))
                    : LockWriteResult.notHeld();
        }
        if (!record.token().equals(token.bytes())) {
            return LockWriteResult.heldByOther(LockInfo.fromRecord(record, now));
        }
        if (leaseMustBeLive && record.leaseUntilEpochMs() <= now) {
            return LockWriteResult.expired(LockInfo.fromRecord(record, now));
        }
        return null;
    }

    /**
     * The round said we were the holder and the fenced write still lost: the key moved between the
     * read and the CAS. The value that won comes back with the refusal, so a caller learns what it
     * lost to without a second read.
     */
    private LockWriteResult lockWriteOutcome(final CasResult result) {
        if (result.swapped()) {
            return LockWriteResult.ok();
        }
        final LockValueCodec.LockRecord winner = result.value() == null
                ? null : LockValueCodec.decode(result.value());
        return LockWriteResult.contended(winner == null
                ? null : LockInfo.fromRecord(winner, clusterClock.nowMillis()));
    }

    /**
     * Extends the lease at {@code key} to {@code newLeaseTtl} from now if {@code token} is still
     * the holder. The generation is unchanged: a renew keeps the same fencing token, only a fresh
     * acquire bumps it.
     * <p>
     * A lease that has already lapsed is <em>not</em> renewable, and comes back as
     * {@link LockWriteStatus#EXPIRED}: from the moment it ran out any waiter was entitled to take
     * over, so extending it would hand back a lock this holder cannot claim to have kept. Acquire
     * again instead.
     */
    @Override
    public CompletableFuture<LockWriteResult> renewLock(
            final ByteBuffer key,
            final LockToken token,
            final Duration newLeaseTtl) {
        checkKeySize(key);
        if (token == null) {
            final CompletableFuture<LockWriteResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException("token is required"));
            return failed;
        }
        if (newLeaseTtl == null || newLeaseTtl.isZero() || newLeaseTtl.isNegative()) {
            final CompletableFuture<LockWriteResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException("newLeaseTtl TTL must be > 0"));
            return failed;
        }
        final ByteBuffer keyCopy = key.duplicate();
        return get(keyCopy, ReadConsistency.LINEARIZABLE).thenComposeAsync(current -> {
            final LockWriteResult refusal = refuseUnlessHolder(current.value(), token, true);
            if (refusal != null) {
                return CompletableFuture.completedFuture(refusal);
            }
            final LockValueCodec.LockRecord record = LockValueCodec.decode(current.value());
            final LockValueCodec.LockRecord nextRecord = new LockValueCodec.LockRecord(
                    record.ownerId(),
                    record.token(),
                    record.acquiredAtEpochMs(),
                    clusterClock.nowMillis() + newLeaseTtl.toMillis(),
                    record.generation());
            final ByteBuffer desired = LockValueCodec.encode(nextRecord);
            checkValueSize(desired);
            return cas(keyCopy.duplicate(), current.version(), desired)
                    .thenApply(this::lockWriteOutcome);
        }, asyncCallbacks);
    }

    /**
     * Reads {@code key}'s current lock state. Expiry is decided against the cluster's clock as
     * this client understands it -- see {@link ClusterClock} -- so two clients whose own clocks
     * disagree still judge the same lease the same way, as long as each has completed a handshake.
     */
    @Override
    public CompletableFuture<LockInfoResult> getLockInfo(final ByteBuffer key) {
        checkKeySize(key);
        final ByteBuffer keyCopy = key.duplicate();
        return get(keyCopy).thenApplyAsync(current -> {
            final ByteBuffer currentValue = current.value();
            if (currentValue == null) {
                return LockInfoResult.unlocked();
            }
            final LockValueCodec.LockRecord record = LockValueCodec.decode(currentValue);
            if (record == null) {
                return LockInfoResult.notLockRecord();
            }
            final long now = clusterClock.nowMillis();
            if (record.isReleased()) {
                // Free, and with the last tenancy still readable off the marker. The same reason
                // EXPIRED keeps its record rather than collapsing into UNLOCKED: a key that says
                // who let it go is auditable, and one that says nothing is not.
                return LockInfoResult.released(LockInfo.fromRecord(record, now));
            }
            final LockInfo info = LockInfo.fromRecord(record, now);
            if (record.leaseUntilEpochMs() <= now) {
                return LockInfoResult.expired(info);
            }
            return LockInfoResult.locked(info);
        }, asyncCallbacks);
    }

    private CompletableFuture<LockAcquireResult> lockAttempt(
            final ByteBuffer key,
            final Duration leaseTtl,
            final long deadlineNanos,
            final String ownerId) {
        return tryLock(key.duplicate(), leaseTtl, ownerId).thenComposeAsync(result -> {
            // HELD_BY_SELF ends the wait for the same reason the other two do: no further attempt
            // can change it. The lease in the way is this caller's own, and it will not lapse
            // while the caller is the one meant to be renewing it.
            if (result.status() == LockAcquireStatus.ACQUIRED
                    || result.status() == LockAcquireStatus.NOT_LOCK_RECORD
                    || result.status() == LockAcquireStatus.HELD_BY_SELF) {
                return CompletableFuture.completedFuture(result);
            }
            if (elapsed(deadlineNanos)) {
                return getLockInfo(key.duplicate()).thenApply(lockInfo ->
                        LockAcquireResult.timedOut(lockInfo.info()));
            }
            return delay(randomBackoff()).thenComposeAsync(ignored ->
                    lockAttempt(key.duplicate(), leaseTtl, deadlineNanos, ownerId), asyncCallbacks);
        }, asyncCallbacks);
    }

    /**
     * One poll of a watch, and the recursion that makes it a blocking query.
     * <p>
     * Two things travel with it. {@code poll} counts the polls made, and for a
     * {@link ReadConsistency#SERIALIZABLE} watch it is also the offset that poll's coordinator
     * walk starts at, so successive polls address successive members. A serializable read is
     * answered from one member's local state and nothing about polling it again makes that state
     * newer -- so a watch that keeps asking one member is blind for as long as that member is
     * behind, and rotating is what turns "the member I happened to ask has not seen it" into "no
     * reachable member has seen it". A linearizable poll does not rotate: it runs a full round, so
     * it contends on the register like a write does, and spreading those rounds over coordinators
     * would break the affinity that serializes one key behind one proposer.
     * <p>
     * {@code best} is the highest-versioned answer any poll has returned. It is what the result
     * is built from, so a later poll landing on a member that is behind can no longer walk the
     * caller backwards -- which matters because {@link WatchResult#version()} is documented as the
     * value to feed into the next watch, and a version below the caller's own would make that
     * chain regress.
     */
    private CompletableFuture<WatchResult> watchAttempt(
            final ByteBuffer key,
            final Version since,
            final long deadlineNanos,
            final ReadConsistency level,
            final int poll,
            final GetResult best) {
        final int startAttempt = level == ReadConsistency.SERIALIZABLE ? poll : 0;
        return get(key.duplicate(), level, startAttempt).handleAsync((observed, error) -> {
            if (error != null) {
                return onWatchPollFailed(key, since, deadlineNanos, level, poll, best, unwrap(error));
            }
            final GetResult latest = moreRecent(best, observed);
            if (latest.version().compareTo(since) > 0) {
                return CompletableFuture.completedFuture(WatchResult.changed(latest));
            }
            if (elapsed(deadlineNanos)) {
                return CompletableFuture.completedFuture(WatchResult.unchanged(latest));
            }
            return delay(randomWatchBackoff()).thenComposeAsync(ignored ->
                    watchAttempt(key.duplicate(), since, deadlineNanos, level, poll + 1, latest),
                    asyncCallbacks);
        }, asyncCallbacks).thenComposeAsync(next -> next, asyncCallbacks);
    }

    /** The later of two observations of one key, {@code null} counting as "nothing seen yet". */
    private static GetResult moreRecent(final GetResult best, final GetResult observed) {
        if (best == null) {
            return observed;
        }
        return observed.version().compareTo(best.version()) > 0 ? observed : best;
    }

    /**
     * A poll failed. Whether that ends the watch depends on why.
     * <p>
     * A watch is a blocking query: the caller asked to be told within {@code maxWait}, and the
     * polling underneath is an implementation detail it already hides (backoff, repeated reads,
     * an unchanged result). A quorum outage is the same kind of detail -- a linearizable read is a
     * Paxos round, so a partition fails every poll -- and ending the watch on the first one throws
     * away the rest of a budget the cluster may well recover inside. Retrying costs nothing in the
     * case that matters: an outage that never lifts still surfaces, at the deadline, with the
     * node's own verdict.
     * <p>
     * Not everything is worth retrying. A caller error fails identically forever, so stalling for
     * the full budget would turn an immediate answer into a long wait for the same one; a closed
     * client has nothing to retry against. Both propagate at once.
     * <p>
     * A failure at the deadline ends the watch only when no poll ever succeeded. Once one has,
     * the caller asked a question that has an answer -- "it had not changed as of what I saw" --
     * and throwing that away because the <em>last</em> poll happened to address a member that was
     * down would report a failure the watch does not have. It also keeps the rotation above from
     * making watches flakier: rotating means later polls address members the first one never did,
     * so a member being down is now something a healthy watch can meet.
     */
    private CompletableFuture<WatchResult> onWatchPollFailed(
            final ByteBuffer key,
            final Version since,
            final long deadlineNanos,
            final ReadConsistency level,
            final int poll,
            final GetResult best,
            final Throwable cause) {
        if (cause instanceof ClientLifecycleException
                || (cause instanceof DisCasOperationException
                        && ((DisCasOperationException) cause).isCallerError())) {
            return failedWatch(cause);
        }
        if (elapsed(deadlineNanos)) {
            return best == null
                    ? failedWatch(cause)
                    : CompletableFuture.completedFuture(WatchResult.unchanged(best));
        }
        return delay(randomWatchBackoff()).thenComposeAsync(ignored ->
                watchAttempt(key.duplicate(), since, deadlineNanos, level, poll + 1, best),
                asyncCallbacks);
    }

    private static CompletableFuture<WatchResult> failedWatch(final Throwable cause) {
        final CompletableFuture<WatchResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(cause);
        return failed;
    }

    /** Strips the {@link CompletionException} wrapper a composed stage adds. */
    private static Throwable unwrap(final Throwable error) {
        return error instanceof CompletionException && error.getCause() != null
                ? error.getCause() : error;
    }

    /**
     * A deadline {@code wait} from now, on the monotonic clock.
     * <p>
     * Derived from {@link System#nanoTime()} rather than {@code System.currentTimeMillis()} so a
     * caller's wait budget cannot be cut short or stretched by an NTP step or a manual clock
     * change mid-wait. The origin is arbitrary, which does not matter: the value is only ever
     * compared against another {@code nanoTime} reading, via {@link #elapsed(long)}.
     * <p>
     * Nanos overflow a {@code long} at ~292 years, so an absurd budget is clamped rather than
     * allowed to throw: both callers report bad input as a failed future and neither may start
     * throwing synchronously.
     */
    private static long deadlineNanosFromNow(final Duration wait) {
        final long waitNanos = wait.getSeconds() > MAX_WAIT_SECONDS
                ? TimeUnit.SECONDS.toNanos(MAX_WAIT_SECONDS) : wait.toNanos();
        return System.nanoTime() + waitNanos;
    }

    /**
     * Whether a deadline from {@link #deadlineNanosFromNow(Duration)} has passed. Compares by
     * subtraction, the form that stays correct across a {@code nanoTime} wraparound.
     */
    private static boolean elapsed(final long deadlineNanos) {
        return System.nanoTime() - deadlineNanos >= 0;
    }

    private CompletableFuture<Void> delay(final Duration d) {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                f.completeExceptionally(new ClientLifecycleException(
                        ClientLifecycleException.Phase.ALREADY_CLOSED, ERR_CLIENT_SHUT_DOWN));
                return f;
            }
            pendingDelays.add(f);
            loop.schedule(d, () -> {
                pendingDelays.remove(f);
                f.complete(null);
            });
        }
        return f;
    }

    private Duration randomBackoff() {
        return randomBetween(lockMinBackoff, lockMaxBackoff);
    }

    private Duration randomWatchBackoff() {
        return randomBetween(watchMinBackoff, watchMaxBackoff);
    }

    private static Duration randomBetween(final Duration min, final Duration max) {
        final long minMs = min.toMillis();
        final long maxMs = max.toMillis();
        final long wait = ThreadLocalRandom.current().nextLong(minMs, maxMs + 1);
        return Duration.ofMillis(wait);
    }

    private static ByteBuffer randomToken() {
        final byte[] token = new byte[LOCK_TOKEN_BYTES];
        LOCK_TOKEN_RNG.nextBytes(token);
        return ByteBuffer.wrap(token);
    }

    /**
     * Re-read every file this process is serving -- the node list it dials, its TLS key and trust
     * stores -- and apply the result, all of it or none of it. The returned report says what each
     * source did, and is the answer to "is what I just wrote now in force".
     * <p>
     * Nothing reads those files unless this is called, which is what makes them safe to edit in
     * place: no half-written store can be read as material, because between one call and the next
     * nobody is reading. And because a refusal anywhere stops the whole reload, a key and its
     * certificate change together or not at all.
     * <p>
     * Not a cluster operation and not on the loop: it reads local files on the calling thread and
     * returns when they have been applied, so it neither blocks the loop nor returns a future.
     */
    public ReloadReport reloadFiles() {
        return ReloadableFiles.shared().reloadAll();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            if (ownsLoop) {
                loop.execute(() -> {
                    drainPendingOnShutdown();
                    transport.close();
                });
            }
        }
        if (ownsLoop) {
            loop.shutdown();
            if (!loop.awaitTermination(shutdownAwaitTimeout)) {
                throw new ClientLifecycleException(ClientLifecycleException.Phase.SHUTDOWN_TIMED_OUT,
                    "Client event loop shutdown timed out");
            }
            return;
        }

        if (loop.inLoop()) {
            drainPendingOnShutdown();
            transport.close();
            return;
        }

        final CountDownLatch drainLatch = new CountDownLatch(1);
        loop.execute(() -> {
            try {
                drainPendingOnShutdown();
                transport.close();
            } finally {
                drainLatch.countDown();
            }
        });
        try {
            if (!drainLatch.await(shutdownAwaitTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new ClientLifecycleException(ClientLifecycleException.Phase.SHUTDOWN_TIMED_OUT,
                        "Client shared-loop drain timed out");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientLifecycleException(ClientLifecycleException.Phase.INTERRUPTED,
                    "Interrupted while waiting for client shared-loop drain", e);
        }
    }

    private static RuntimeException shutdownError() {
        return new ClientLifecycleException(
                ClientLifecycleException.Phase.CLOSING, ERR_CLIENT_SHUTTING_DOWN);
    }

    private void drainPendingOnShutdown() {
        final RuntimeException shutdownError = shutdownError();

        if (!pending.isEmpty()) {
            final List<PendingEntry> entries = new ArrayList<>(pending.values());
            pending.clear();
            for (final PendingEntry entry : entries) {
                if (entry.timerHandle != null) {
                    entry.timerHandle.cancel();
                }
                if (!entry.future.isDone()) {
                    entry.future.completeExceptionally(shutdownError);
                }
            }
        }

        if (!pendingScans.isEmpty()) {
            final List<PendingScan> scans = new ArrayList<>(pendingScans.values());
            pendingScans.clear();
            for (final PendingScan pendingScan : scans) {
                if (pendingScan.timerHandle != null) {
                    pendingScan.timerHandle.cancel();
                }
                if (!pendingScan.future.isDone()) {
                    pendingScan.future.completeExceptionally(shutdownError);
                }
            }
        }

        if (!pendingDelays.isEmpty()) {
            for (final CompletableFuture<Void> delayFuture : pendingDelays) {
                if (!delayFuture.isDone()) {
                    delayFuture.completeExceptionally(shutdownError);
                }
            }
            pendingDelays.clear();
        }
    }

    /**
     * Dispatch one attempt, preferring coordinators that are not inside their retry backoff.
     * <p>
     * <b>An available alternative is always taken immediately</b> -- moving to a different
     * coordinator costs a message and no time, and it is the whole point of a leaderless store:
     * the node that answered is not the node that must answer. No delay is ever inserted while
     * some coordinator is eligible.
     * <p>
     * <b>When every coordinator is inside its backoff, the operation waits rather than failing.</b>
     * The bound is {@link DisCasClientConfig#requestDeadline()} -- the caller's own statement of
     * how long the work is worth waiting for -- so waiting inside it is not a policy decision taken
     * behind the caller's back; it is the policy the caller supplied. Failing early instead was a
     * defect with two sharp edges: a client configured with one coordinator (the agent's nodes-file
     * bootstrap, until the full membership loads) died on its first failure with no reconnect at
     * all, and any single transient failure made the <em>next</em> request fail without touching
     * the wire, reporting "no coordinator left to try after 0 attempts".
     * <p>
     * What the client still does not do is outlive the deadline or retry on a clock of its own
     * choosing. Pinned by {@code PeerBackoffTest}.
     */
    private void sendAttempt(final long correlationId, final PendingEntry entry, final int attempt) {
        if (entry.timerHandle != null) {
            entry.timerHandle.cancel();
            entry.timerHandle = null;
        }

        final long now = System.nanoTime();
        if (now - entry.deadlineNanos >= 0) {
            pending.remove(correlationId);
            entry.future.completeExceptionally(noCoordinatorFailure(entry, attempt));
            return;
        }

        final int offset = eligibleOffset(entry.routingKey, attempt, now);
        if (offset < 0) {
            // Every coordinator is inside its backoff. Sleep until the earliest one is eligible,
            // or until the caller's deadline, whichever comes first -- the deadline check at the
            // top of this method is then what ends the operation. Re-entering with the same
            // `attempt` is deliberate: eligibleOffset rescans all M peers from there, so whichever
            // backoff expired first is the one picked up.
            final long untilEligible = nanosUntilAnyPeerEligible(entry.routingKey, attempt, now);
            final long untilDeadline = entry.deadlineNanos - now;
            // At least one tick, so a rounding artefact can never turn this into a spin.
            final long wait = Math.max(MIN_BACKOFF_WAIT_NANOS, Math.min(untilEligible, untilDeadline));
            entry.timerHandle = loop.schedule(Duration.ofNanos(wait), () -> {
                final PendingEntry current = pending.get(correlationId);
                if (current == null || current.future.isDone()) {
                    return;
                }
                sendAttempt(correlationId, current, attempt);
            });
            return;
        }

        // Install the retry timer BEFORE dispatching the request so any inline
        // reply (e.g. co-located InProcess transport) that runs onResponse on
        // the sending stack finds a real timerHandle to cancel and does not
        // race with the post-send timer installation
        entry.currentAttempt = offset;
        final int scheduledAttempt = offset;
        entry.timerHandle = loop.schedule(perAttemptTimeout, () -> {
            final PendingEntry current = pending.get(correlationId);
            if (current == null || current.future.isDone()) {
                return;
            }
            if (current.currentAttempt != scheduledAttempt) {
                return;
            }
            // Silence past the per-attempt timeout counts against that coordinator, so the next
            // pass skips it rather than waiting the same five seconds all over again.
            recordPeerFailure(peerIndex(current.routingKey, scheduledAttempt));
            if (!mayMoveToAnotherCoordinator(current)) {
                failIndeterminate(correlationId, current);
                return;
            }
            sendAttempt(correlationId, current, scheduledAttempt + 1);
        });

        // Not final: the catch below reports which peer was unreachable, and the send is inside
        // the try so a failure there keeps taking the same failover path it always did.
        NodeId targetPeer = null;
        try {
            targetPeer = peers.get(peerIndex(entry.routingKey, offset));
            transport.send(targetPeer, entry.request);
            entry.everDispatched = true;
        } catch (final Exception e) {
            entry.lastSendFailure = e;
            if (targetPeer != null) {
                // Otherwise invisible: the request is retried elsewhere and the caller never
                // learns that a peer could not be reached at all.
                observer.sendFailed(targetPeer, e);
            }
            if (entry.timerHandle != null) {
                entry.timerHandle.cancel();
                entry.timerHandle = null;
            }
            recordPeerFailure(peerIndex(entry.routingKey, offset));
            sendAttempt(correlationId, entry, offset + 1);
        }
    }

    /**
     * How long until the soonest of the coordinators {@link #eligibleOffset} would consider leaves
     * its backoff. Only called when none is eligible now, so every candidate has a future
     * eligibility instant.
     */
    private long nanosUntilAnyPeerEligible(final ByteBuffer routingKey, final int from,
                                           final long now) {
        long soonest = Long.MAX_VALUE;
        for (int i = 0; i < peers.size(); i++) {
            final int index = peerIndex(routingKey, from + i);
            final long remaining = peerNextEligibleNanos[index] - now;
            if (remaining < soonest) {
                soonest = remaining;
            }
        }
        return soonest == Long.MAX_VALUE ? MIN_BACKOFF_WAIT_NANOS : soonest;
    }

    /**
     * The first offset at or after {@code from} whose peer is out of backoff, or -1 if none is.
     * <p>
     * Scans a whole ring, so the walk wraps onto peers tried earlier in this same request -- which
     * is intended: a peer tried and failed is in backoff and therefore skipped, so backoff alone
     * expresses "already tried" and no per-request set is needed.
     */
    private int eligibleOffset(final ByteBuffer routingKey, final int from, final long now) {
        for (int i = 0; i < peers.size(); i++) {
            final int offset = from + i;
            final int index = peerIndex(routingKey, offset);
            if (peerFailures[index] == 0 || now - peerNextEligibleNanos[index] >= 0) {
                return offset;
            }
        }
        return -1;
    }

    /**
     * A coordinator's connection died: move everything that was riding on it, now, rather than
     * letting each request discover the loss separately when its per-attempt timer expires.
     * <p>
     * The transport knows instantly and the caller does not, which is the gap this closes: a
     * coordinator that dies just after accepting a request would otherwise cost a full per-attempt
     * timeout nobody needed to wait for.
     * <p>
     * Only entries whose <em>current</em> target is this peer are moved. An entry that has already
     * been re-dispatched elsewhere is somebody else's problem now, which is what makes the
     * deferred delivery safe.
     */
    private void onConnectionLost(final NodeId peer) {
        final int index = peers.indexOf(peer);
        if (index < 0) {
            return;
        }
        List<Long> affected = null;
        for (final Map.Entry<Long, PendingEntry> e : pending.entrySet()) {
            final PendingEntry entry = e.getValue();
            if (peerIndex(entry.routingKey, entry.currentAttempt) != index) {
                continue;
            }
            if (affected == null) {
                affected = new ArrayList<>();
            }
            affected.add(e.getKey());
        }
        observer.connectionLost(peer, affected == null ? 0 : affected.size());
        if (affected == null) {
            // No request was riding on it, but the peer is still down: remember that, so the next
            // request routed here skips it instead of paying a connect to find out.
            recordPeerFailure(index);
            return;
        }
        recordPeerFailure(index);
        // Collected first: re-dispatching mutates `pending`, which cannot be done mid-iteration.
        for (int i = 0; i < affected.size(); i++) {
            final long correlationId = affected.get(i);
            final PendingEntry entry = pending.get(correlationId);
            if (entry == null || entry.future.isDone()) {
                continue;
            }
            if (!mayMoveToAnotherCoordinator(entry)) {
                // The javadoc above is the reason this cannot simply move: a coordinator that dies
                // just after accepting a request may still have it committed by the survivors.
                failIndeterminate(correlationId, entry);
                continue;
            }
            sendAttempt(correlationId, entry, entry.currentAttempt + 1);
        }
    }

    /**
     * Why an operation ran out of coordinators to try.
     * <p>
     * The cause is what keeps two different situations apart: never having reached the wire at all
     * is a connectivity problem an operator should read differently from a cluster that accepted
     * the request and stayed silent.
     */
    private RequestFailedException noCoordinatorFailure(final PendingEntry entry, final int attempts) {
        if (!entry.everDispatched && entry.lastSendFailure != null) {
            return new RequestFailedException(
                    RequestFailedException.Cause.ALL_PEERS_EXHAUSTED,
                    ERR_ALL_PEERS_EXHAUSTED, entry.lastSendFailure);
        }
        return new RequestFailedException(
                RequestFailedException.Cause.TIMED_OUT,
                ERR_REQUEST_TIMED_OUT_PREFIX + " (no coordinator left to try after "
                        + attempts + " attempts)");
    }

    private void recordPeerFailure(final int index) {
        final int failures = ++peerFailures[index];
        long step = peerRetryMinBackoffNanos << Math.min(failures - 1, 32);
        if (step <= 0 || step > peerRetryMaxBackoffNanos) {
            step = peerRetryMaxBackoffNanos;
        }
        // Jitter of up to one base step, so clients that failed together do not return together.
        peerNextEligibleNanos[index] =
                System.nanoTime() + step + ThreadLocalRandom.current().nextLong(peerRetryMinBackoffNanos);
    }

    /**
     * That peer answered, so whatever it said, it is reachable: clear its backoff outright rather
     * than decaying it. This is the "re-settable" half -- without it a peer that failed once a day
     * would climb to the maximum backoff and stay there.
     */
    private void recordPeerSuccess(final int index) {
        peerFailures[index] = 0;
        peerNextEligibleNanos[index] = 0L;
    }

    /**
     * hashCode() has poor avalanche properties for short keys (1-4 bytes),
     * producing clustered values that cause hot-peer skew. For example,
     * single-byte keys 0x00-0xFF produce only 256 distinct hash values
     * that map unevenly across peers
     * distributionHash() provides uniform distribution regardless of key
     * length, ensuring balanced load across the cluster
     */
    private NodeId pickPeer(final ByteBuffer routingKey, final int attempt) {
        return peers.get(peerIndex(routingKey, attempt));
    }

    /** Where a given attempt offset lands in {@link #peers}, and so in the per-peer health arrays. */
    private int peerIndex(final ByteBuffer routingKey, final int attempt) {
        final int hash = KeyHash.distributionHash(routingKey);
        return Integer.remainderUnsigned(hash + attempt, peers.size());
    }

    /**
     * Whether a node's explicit failure is worth re-asking somewhere else, in which case the
     * request is dispatched to the next peer at once and the caller's future is left alone.
     * <p>
     * The discriminator is <b>node-local, and refused without running a round to exhaustion</b>.
     * Two codes qualify: {@link ClientErrorCode#NOT_READY} is a condition of that one node, still
     * replaying its log, and {@link ClientErrorCode#NO_QUORUM_AT_COORDINATOR} is a condition of
     * that one node's connectivity -- under an asymmetric partition a coordinator cut off from the
     * majority cannot serve a client that reaches the majority perfectly well through someone else.
     * Both are answered cheaply, so moving on costs a message rather than a round.
     * <p>
     * <em>Not</em> {@link ClientErrorCode#UNAVAILABLE}, which is reserved for failures that say
     * nothing about connectivity -- a lost ballot duel, a stalled accept, a saturated proposer.
     * Those are as likely elsewhere and each further attempt costs a whole proposer round timeout,
     * so failing over on them would make a failing write cost {@code N} round timeouts instead of
     * one. {@code ACCESS_DENIED} and {@code INVALID_ARGUMENT} are properties of the request itself
     * and would fail identically everywhere.
     * <p>
     * A <b>version-fenced write is the exception.</b> A duplicate of one is provably a no-op,
     * because a stale attempt carries a ballot the register has already overtaken -- which is the
     * whole reason the fence exists. So a fenced write keeps walking the coordinators on any
     * node-side failure, the indeterminate one included, and either lands or runs out of the
     * caller's deadline. An unfenced write cannot: re-sending it after
     * {@link ClientErrorCode#UNAVAILABLE} is exactly the duplicate-apply hazard.
     */
    private static boolean worthAnotherCoordinator(final PendingEntry entry,
                                                   final ClientErrorCode code) {
        if (code == ClientErrorCode.NOT_READY
                || code == ClientErrorCode.NO_QUORUM_AT_COORDINATOR) {
            return true;
        }
        if (!(entry.request instanceof ClientMessage.ClientCasReq)) {
            return false;
        }
        // Fenced: a duplicate cannot apply, so trying elsewhere can only help. Caller errors are
        // still properties of the request and would fail identically everywhere.
        return code == ClientErrorCode.UNAVAILABLE
                || code == ClientErrorCode.BALLOT_LOST
                || code == ClientErrorCode.PROPOSAL_EXPIRED
                || code == ClientErrorCode.INTERNAL;
    }

    /**
     * {@link #worthAnotherCoordinator}'s rule for the case where no answer arrived at all: the
     * coordinator went silent past the per-attempt timeout, or its connection died.
     * <p>
     * Silence is the harder half. An explicit refusal at least says which phase it failed in, and
     * that is what lets the code-based rule move an unfenced write on a node-local
     * {@code NOT_READY}. Here there is nothing to read, so nothing can rule out that the silent
     * coordinator is still driving a round -- and a duplicate unfenced write that lands after
     * somebody else committed reverts them. Only the two unfenced writes are held back:
     * a version-fenced duplicate carries a ballot the register has already overtaken and provably
     * cannot apply, and a duplicate <em>read</em> changes nothing at all, so making a read wait out
     * a dead coordinator would cost availability and buy no safety.
     * <p>
     * A request that never reached the wire -- the send itself threw -- is safe whatever it is,
     * because a coordinator that was never given the request cannot be driving a round for it.
     */
    private static boolean mayMoveToAnotherCoordinator(final PendingEntry entry) {
        final boolean unfencedWrite = entry.request instanceof ClientMessage.ClientPutReq
                || entry.request instanceof ClientMessage.ClientDeleteReq;
        return !unfencedWrite || !entry.everDispatched;
    }

    /**
     * End an unfenced write that reached a coordinator and got no answer. Failing here rather than
     * at the caller's deadline is the point: further waiting cannot turn an unknown outcome into a
     * known one, and re-sending is what we have just refused to do.
     */
    private void failIndeterminate(final long correlationId, final PendingEntry entry) {
        pending.remove(correlationId);
        if (entry.timerHandle != null) {
            entry.timerHandle.cancel();
            entry.timerHandle = null;
        }
        entry.future.completeExceptionally(new RequestFailedException(
                RequestFailedException.Cause.INDETERMINATE, ERR_INDETERMINATE_UNFENCED));
    }

    private boolean retryOnNextPeer(final long correlationId, final PendingEntry entry,
                                    final ClientErrorCode code) {
        final int index = peerIndex(entry.routingKey, entry.currentAttempt);
        if (!worthAnotherCoordinator(entry, code)) {
            // A definitive answer. Whatever it says about the request, the coordinator itself is
            // healthy and must not be penalised -- a rejected write is not a broken peer.
            recordPeerSuccess(index);
            return false;
        }
        // Node-local refusals do count against the peer, so the next request routed to this key
        // skips it instead of paying a round trip to be refused again.
        recordPeerFailure(index);
        final int nextAttempt = entry.currentAttempt + 1;
        if (nextAttempt - entry.startAttempt >= peers.size()) {
            return false; // every peer tried; report the last node's verdict
        }
        observer.requestFailedOver(pickPeer(entry.routingKey, entry.currentAttempt), code);
        // onResponse already removed the entry; re-arm it before re-dispatching.
        pending.put(correlationId, entry);
        sendAttempt(correlationId, entry, nextAttempt);
        return true;
    }

    /**
     * Take the pending request a response belongs to, and stop its timer.
     *
     * @return the entry, or {@code null} if nothing is waiting -- a late answer to a request that
     *         already failed over, timed out or was completed by another coordinator
     */
    @SuppressWarnings("unchecked")
    private PendingEntry takePending(final long correlationId) {
        final PendingEntry entry = pending.remove(correlationId);
        if (entry != null && entry.timerHandle != null) {
            entry.timerHandle.cancel();
        }
        return entry;
    }

    /**
     * The one place a pending future's result type is recovered. {@link PendingEntry} holds a
     * {@code CompletableFuture<?>} because one map holds every operation in flight; what a
     * correlation id completes with is fixed by the request that created it, so the cast is safe
     * and is spelled out here rather than once per response branch.
     */
    @SuppressWarnings("unchecked")
    private static <T> void complete(final PendingEntry entry, final T result) {
        ((CompletableFuture<T>) entry.future).complete(result);
    }

    /**
     * The tail every failed response shares: try the next coordinator, and fail the caller only
     * when there is not one -- which is what makes a node-side failure ({@code NOT_READY}, no
     * quorum at that coordinator) invisible to a caller that another member can serve.
     */
    private void failOrRetry(final PendingEntry entry, final long correlationId,
                             final ClientErrorCode errorCode, final String error) {
        if (!retryOnNextPeer(correlationId, entry, errorCode)) {
            entry.future.completeExceptionally(new DisCasOperationException(errorCode, error));
        }
    }

    private void onResponse(final ClientMessage message) {
        if (message instanceof ClientMessage.ClientGetResp) {
            final ClientMessage.ClientGetResp response = (ClientMessage.ClientGetResp) message;
            final PendingEntry entry = takePending(response.correlationId());
            if (entry == null) {
                return;
            }
            if (!response.ok()) {
                failOrRetry(entry, response.correlationId(), response.errorCode(), response.error());
                return;
            }
            recordPeerSuccess(peerIndex(entry.routingKey, entry.currentAttempt));
            complete(entry, new GetResult(response.value(), new Version(response.version())));
            return;
        }

        if (message instanceof ClientMessage.ClientPutResp) {
            final ClientMessage.ClientPutResp response = (ClientMessage.ClientPutResp) message;
            final PendingEntry entry = takePending(response.correlationId());
            if (entry == null) {
                return;
            }
            if (!response.ok()) {
                failOrRetry(entry, response.correlationId(), response.errorCode(), response.error());
                return;
            }
            recordPeerSuccess(peerIndex(entry.routingKey, entry.currentAttempt));
            complete(entry, new Version(response.version()));
            return;
        }

        if (message instanceof ClientMessage.ClientCasResp) {
            final ClientMessage.ClientCasResp response =
                    (ClientMessage.ClientCasResp) message;
            final PendingEntry entry = takePending(response.correlationId());
            if (entry == null) {
                return;
            }
            if (!response.ok()) {
                failOrRetry(entry, response.correlationId(), response.errorCode(), response.error());
                return;
            }
            recordPeerSuccess(peerIndex(entry.routingKey, entry.currentAttempt));
            complete(entry, new CasResult(response.swapped(),
                    response.value(), new Version(response.version())));
            return;
        }

        if (message instanceof ClientMessage.ClientDeleteResp) {
            final ClientMessage.ClientDeleteResp response = (ClientMessage.ClientDeleteResp) message;
            final PendingEntry entry = takePending(response.correlationId());
            if (entry == null) {
                return;
            }
            if (!response.ok()) {
                failOrRetry(entry, response.correlationId(), response.errorCode(), response.error());
                return;
            }
            recordPeerSuccess(peerIndex(entry.routingKey, entry.currentAttempt));
            complete(entry, new Version(response.version()));
            return;
        }

        if (message instanceof ClientMessage.ClientScanResp) {
            final ClientMessage.ClientScanResp response = (ClientMessage.ClientScanResp) message;
            final PendingScan pendingScan = pendingScans.get(response.correlationId());
            if (pendingScan == null) {
                return;
            }
            pendingScan.addResponse(response);
            // Evaluate quorum on arrival, not at send time: connections are established lazily,
            // so N is typically still unknown when scan() is issued. The hello response always
            // precedes a scan response on the same connection, so N is known by the time the
            // first scan response lands.
            maybeCompleteScan(response.correlationId(), pendingScan);
        }
    }

    private static final class PendingEntry {
        final CompletableFuture<?> future;
        final ClientMessage request;
        final ByteBuffer routingKey;
        /**
         * When this operation gives up, whatever coordinators remain. The failover walk is no
         * longer bounded by the peer count, so this is the only thing that ends a request that
         * nobody answers.
         */
        final long deadlineNanos;
        /**
         * The attempt offset this request's walk began at, which is 0 for everything except a
         * serializable watch poll (see {@link #watchAttempt}). Only
         * {@link #retryOnNextPeer} reads it, and only to count attempts relative to it: the walk
         * visits {@code M} coordinators wherever it starts, and comparing an absolute offset
         * against {@code peers.size()} would cut a walk that started late down to its remainder.
         */
        final int startAttempt;
        int currentAttempt;
        EventLoop.TimerHandle timerHandle;
        /**
         * Whether any attempt got as far as the wire, and the last send failure if not. Together
         * they keep "could not reach a single coordinator" distinguishable from "someone took the
         * request and nobody answered" now that both end at the same deadline.
         */
        boolean everDispatched;
        Throwable lastSendFailure;

        PendingEntry(final CompletableFuture<?> future, final ClientMessage request,
                     final ByteBuffer routingKey, final long deadlineNanos) {
            this(future, request, routingKey, deadlineNanos, 0);
        }

        PendingEntry(final CompletableFuture<?> future, final ClientMessage request,
                     final ByteBuffer routingKey, final long deadlineNanos,
                     final int startAttempt) {
            this.future = future;
            this.request = request;
            this.routingKey = routingKey;
            this.deadlineNanos = deadlineNanos;
            this.startAttempt = startAttempt;
            this.currentAttempt = startAttempt;
        }
    }

    private static final class PendingScan {
        private final long correlationId;
        private final CompletableFuture<ScanPage> future;
        private final List<ClientMessage.ClientScanResp> responses = new ArrayList<>();
        private final Set<String> respondedPeers = new HashSet<>();
        private EventLoop.TimerHandle timerHandle;

        private final ScanCoverage coverage;
        /**
         * Peers whose send threw, so no answer can ever arrive from them. Counting these lets a
         * scan settle as soon as every peer has either answered or proved unreachable, instead of
         * always waiting out the scan timeout. Without it a partial scan took the full timeout,
         * which is at or beyond the agent's own request budget -- so a best-effort listing would
         * have 504'd before it could return.
         */
        private int unreachableCount;

        private PendingScan(final long correlationId,
                    final CompletableFuture<ScanPage> future,
                    final ScanCoverage coverage) {
            this.correlationId = correlationId;
            this.future = future;
            this.coverage = coverage;
        }

        private boolean addResponse(final ClientMessage.ClientScanResp response) {
            if (!respondedPeers.add(response.senderId())) {
                return false;
            }
            responses.add(response);
            return true;
        }

        private int respondedCount() {
            return respondedPeers.size();
        }

        /** True when no further answer can arrive: everyone answered or failed to be reached. */
        private boolean settled(final int peerCount) {
            return respondedPeers.size() + unreachableCount >= peerCount;
        }

        /**
         * The highest key this merge can be trusted up to, or {@code null} when every responding
         * node is exhausted (so the merge is complete).
         * <p>
         * A node whose page was truncated may hold further keys it did not send. Merging in a key
         * that sorts <em>after</em> such a node's last entry would silently skip whatever that
         * node holds in between, so the merge is only sound up to the smallest last-key among
         * nodes reporting more. That key doubles as the next page's cursor.
         */
        private ByteBuffer trustedUpTo() {
            return scanTrustBound(responses);
        }

        /**
         * Merge the collected pages into one sound page, truncated at {@link #trustedUpTo()}.
         * Tombstones are dropped <em>after</em> the merge so a delete on a higher ballot still
         * suppresses a live value held by a lagging node.
         */
        private ScanPage mergeResults(final int clusterSizeAtMerge) {
            final ByteBuffer bound = trustedUpTo();
            final Map<ByteBuffer, ClientMessage.ScanEntry> merged = new HashMap<>();
            for (int r = 0; r < responses.size(); r++) {
                final List<ClientMessage.ScanEntry> entries = responses.get(r).entries();
                for (int e = 0; e < entries.size(); e++) {
                    final ClientMessage.ScanEntry scanEntry = entries.get(e);
                    if (bound != null && scanEntry.key().compareTo(bound) > 0) {
                        continue; // beyond what this merge can vouch for; the next page covers it
                    }
                    merged.merge(scanEntry.key(), scanEntry, (first, second) ->
                            first.version().compareTo(second.version()) >= 0 ? first : second);
                }
            }
            final List<ScanResult> results = new ArrayList<>();
            for (final ClientMessage.ScanEntry scanEntry : merged.values()) {
                if (!scanEntry.tombstone()) {
                    results.add(new ScanResult(scanEntry.key(), new Version(scanEntry.version())));
                }
            }
            results.sort(Comparator.comparing(ScanResult::key));
            return new ScanPage(results, bound,
                    respondedCount(), clusterSizeAtMerge);
        }
    }

    /**
     * How far a merge of {@code responses} can be trusted, or {@code null} when every responding
     * node is exhausted (the merge is then complete).
     * <p>
     * A node whose page was truncated may hold further keys it did not send, so merging in a key
     * that sorts <em>after</em> that node's last entry would silently skip whatever it holds in
     * between. The bound is therefore the <em>smallest</em> last-key among nodes reporting more;
     * nodes that are exhausted, or that returned nothing, impose no bound. The bound doubles as
     * the next page's cursor.
     * <p>
     * Package-private and static so the rule can be exercised directly against divergent node
     * pages -- in a healthy cluster every node returns the same page, which would make an
     * incorrect bound (say, the largest last-key) indistinguishable from a correct one.
     */
    static ByteBuffer scanTrustBound(final List<ClientMessage.ClientScanResp> responses) {
        ByteBuffer bound = null;
        for (int i = 0; i < responses.size(); i++) {
            final ClientMessage.ClientScanResp response = responses.get(i);
            if (!response.hasMore() || response.entries().isEmpty()) {
                continue;
            }
            final ByteBuffer last = response.entries().get(response.entries().size() - 1).key();
            if (bound == null || last.compareTo(bound) < 0) {
                bound = last;
            }
        }
        return bound;
    }
}
