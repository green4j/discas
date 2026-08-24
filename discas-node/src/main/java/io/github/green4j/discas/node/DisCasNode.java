/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.io.Closeables;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.wal.Wal;

import io.github.green4j.discas.node.acl.ClientAcl;
import io.github.green4j.discas.node.acl.ClientAuthorizer;
import io.github.green4j.discas.node.observability.HealthSource;
import io.github.green4j.discas.node.transport.PeerTransport;
import io.github.green4j.discas.common.client.ClientIngress;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ResponseSink;
import io.github.green4j.discas.common.identity.ClientId;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * One cluster member: the acceptor, the proposer, the store, anti-entropy and tombstone collection,
 * assembled and wired to a transport and a log, all running on one event loop.
 * <p>
 * A node starts by replaying its log and, if it has none, by taking a promise floor from the
 * cluster; it serves nothing until then. {@link NodeState} is the whole lifecycle, and
 * {@link NodeObserver} is where every event it produces goes.
 */
public final class DisCasNode implements AutoCloseable {
    private final NodeConfig cfg;
    private final NodeId nodeId;
    private final Wal wal;
    private final EventLoop loop;
    private final LocalStore store;
    private final Acceptor acceptor;
    private final Proposer proposer;
    private final AntiEntropy antiEntropy;
    private final CeilingRecovery ceilingRecovery;
    private final TombstoneCollector tombstoneCollector;
    private final TombstoneSweeper tombstoneSweeper;
    private final ClientHandler clientHandler;
    private final ClientAuthorizer clientAuthorizer;
    private final PeerTransport peerTransport;
    private final SnapshotScheduler snapshotScheduler;
    private final Duration repairInterval;
    private final NodeObserver observer;
    private final List<AutoCloseable> lifecycleCloseables = new ArrayList<>();

    // The whole start/restart model, in one field. Written only on the event loop, but read off it
    // by the health endpoints, so volatile: a probe must not see a stale state indefinitely.
    private volatile NodeState state = NodeState.REPLAYING;
    // Lifecycle guards. Read/written from both the loop thread and a foreign closer, so
    // volatile; close() additionally serialises on the instance so two foreign threads
    // cannot both enter the shutdown sequence.
    private volatile boolean started = false;
    private volatile boolean closed = false;
    private long replayProgressAtNanos;

    private static final String ERR_NODE_CLOSING = "Node closing";
    private static final int RECOVERY_BATCH_SIZE = 100;
    private static final int SNAPSHOT_BATCH_SIZE = 50;
    private static final long REPLAY_PROGRESS_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

    public DisCasNode(final NodeConfig cfg,
                   final Wal wal,
                   final EventLoop loop,
                   final PeerTransport peerTransport) {
        this(cfg, wal, loop, peerTransport, NodeObserver.NONE);
    }

    /**
     * @param cfg identity, quorum basis and every timing this node runs on -- see
     *            {@link NodeConfig} for the individual deadlines and their defaults.
     * @param observer receives every lifecycle/consensus/anti-entropy event this node
     *                 produces (see {@link NodeObserver}); use {@link NodeObserver#NONE}
     *                 for silent operation, or {@link StderrNodeObserver} to print diagnostics to
     *                 {@code System.err}.
     */
    public DisCasNode(final NodeConfig cfg,
                   final Wal wal,
                   final EventLoop loop,
                   final PeerTransport peerTransport,
                   final NodeObserver observer) {
        // NodeConfig validates its own identity and timing fields; this constructor accepted a
        // null wal, loop or transport and failed later, somewhere else.
        this.cfg = require(cfg, "cfg");
        this.nodeId = cfg.nodeId;
        this.wal = require(wal, "wal");
        this.loop = require(loop, "loop");
        this.peerTransport = require(peerTransport, "peerTransport");
        this.repairInterval = cfg.repairInterval();
        this.observer = observer == null ? NodeObserver.NONE : observer;

        final CorrelationIdGenerator correlationIdGenerator = new CorrelationIdGenerator(nodeId);

        this.store = new LocalStore(wal, this.observer, cfg.storeCapacityBytes());
        this.acceptor = new Acceptor(nodeId, store, this.observer);
        this.proposer = new Proposer(
                nodeId, peerTransport, acceptor, loop, store, correlationIdGenerator, this.observer,
                cfg);
        this.antiEntropy = new AntiEntropy(
                nodeId, store, proposer, peerTransport, loop, correlationIdGenerator, this.observer,
                cfg.peerResponseTimeout());
        this.ceilingRecovery = new CeilingRecovery(
                nodeId, store, peerTransport, loop, correlationIdGenerator, this.observer,
                cfg.ceilingRecoveryRetryCap(), this::stillWaiting);
        this.tombstoneCollector = new TombstoneCollector(
                nodeId, store, peerTransport, loop, correlationIdGenerator,
                cfg.peerResponseTimeout());
        this.tombstoneSweeper = new TombstoneSweeper(
                store, new SweepAffinity(nodeId, peerTransport), tombstoneCollector, loop,
                this.observer, cfg.tombstoneSweepInterval());
        // What this node tells a peer about its own storage at every handshake, so the peer can
        // check the one thing this node cannot check about itself.
        peerTransport.bindPromiseCeiling(this::promiseCeilingClaim);
        this.clientAuthorizer = new ClientAuthorizer();
        this.clientHandler = new ClientHandler(
                nodeId, proposer, store, loop, clientAuthorizer, this.observer);
        this.snapshotScheduler = new SnapshotScheduler();
    }

    public void registerClientMessages(final Consumer<ClientIngress> registrar) {
        registrar.accept((clientId, message, replySink) ->
                loop.execute(() -> dispatchClient(clientId, message, replySink)));
    }

    /**
     * Activate client authorization from {@code acl} (default-deny prefix grants). Must be
     * called before {@link #start()}. When never called, the node stays permissive
     * (AllowAll): every operation on every key is allowed.
     */
    public void registerClientAcl(final ClientAcl acl) {
        clientAuthorizer.bind(acl, loop);
    }

    /**
     * Register a resource (e.g. a TCP client server) whose lifecycle is
     * bound to this node -- {@link #close()} will close it before shutting
     * down the event loop. Multiple resources may be registered; they are
     * closed in registration order.
     */
    public void addLifecycleCloseable(final AutoCloseable resource) {
        lifecycleCloseables.add(resource);
    }

    private static <T> T require(final T value, final String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        if (closed) {
            throw new NodeLifecycleException(NodeLifecycleException.Phase.CLOSING,
                    "Node is closed");
        }
        started = true;
        loop.start();
        loop.execute(this::recover);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (loop.inLoop()) {
            enter(NodeState.CLOSING, null, null);
            antiEntropy.close();
            ceilingRecovery.close();
            tombstoneSweeper.close();
            tombstoneCollector.close();
            proposer.drainAllPending(
                    new NodeLifecycleException(NodeLifecycleException.Phase.CLOSING, ERR_NODE_CLOSING));
            peerTransport.close();
            closeLifecycleResources();
            // The loop must be shut down even if the WAL fails to close, or the loop thread
            // outlives the node. The off-loop branch below already funnels this through
            // closeError; keep the two branches equivalent.
            try {
                wal.close();
            } finally {
                loop.shutdown();
            }
            return;
        }

        final CountDownLatch closeLatch = new CountDownLatch(1);
        final AtomicReference<RuntimeException> closeError = new AtomicReference<>();
        loop.execute(() -> {
            try {
                enter(NodeState.CLOSING, null, null);
                antiEntropy.close();
                ceilingRecovery.close();
                tombstoneSweeper.close();
                tombstoneCollector.close();
                proposer.drainAllPending(
                    new NodeLifecycleException(NodeLifecycleException.Phase.CLOSING, ERR_NODE_CLOSING));
                peerTransport.close();
                closeLifecycleResources();
                wal.close();
            } catch (final RuntimeException e) {
                closeError.set(e);
            } finally {
                closeLatch.countDown();
            }
        });

        try {
            if (!closeLatch.await(cfg.shutdownAwaitTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                throw new NodeLifecycleException(NodeLifecycleException.Phase.PRE_SHUTDOWN_TIMED_OUT,
                        "Node pre-shutdown close timed out");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NodeLifecycleException(NodeLifecycleException.Phase.INTERRUPTED,
                    "Interrupted while waiting for node close", e);
        }

        loop.shutdown();
        if (!loop.awaitTermination(cfg.shutdownAwaitTimeout())) {
            throw new NodeLifecycleException(NodeLifecycleException.Phase.SHUTDOWN_TIMED_OUT,
                    "Node event loop shutdown timed out");
        }
        if (closeError.get() != null) {
            throw closeError.get();
        }
    }

    /**
     * The one place {@link #state} changes, so the field and what is reported about it cannot drift
     * apart. {@code FAILED} and {@code CLOSING} are terminal: a late transition out of either would
     * announce a node that is on its way down as though it were coming up.
     */
    private void enter(final NodeState next, final String detail, final Throwable cause) {
        final NodeState previous = state;
        if (previous == NodeState.FAILED || previous == NodeState.CLOSING) {
            return;
        }
        state = next;
        observer.nodeState(previous, next, detail, cause);
    }

    /** A state that is still waiting for something, saying so again. Never changes {@link #state}. */
    private void stillWaiting(final String detail) {
        observer.nodeState(state, state, detail, null);
    }

    private void closeLifecycleResources() {
        Closeables.closeAll(lifecycleCloseables, observer::lifecycleResourceCloseFailed);
    }

    private void recover() {
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        replayProgressAtNanos = System.nanoTime();
        loadNextBatch(loader);
    }

    private void loadNextBatch(final LocalStore.ThrottledLoader loader) {
        try {
            final boolean moreWork = loader.loadBatch(RECOVERY_BATCH_SIZE);
            if (moreWork) {
                reportReplayProgress(loader);
                loop.execute(() -> loadNextBatch(loader));
            } else {
                onReplayComplete();
            }
        } catch (final Exception e) {
            enter(NodeState.FAILED, "replay failed", e);
            loop.shutdown();
        }
    }

    /**
     * Replay runs in batches off the event loop and a large snapshot takes a while, so it says where
     * it is at a fixed interval rather than per batch -- a batch is 100 entries and would drown the
     * log. Reported as the state repeating itself, like every other wait.
     */
    private void reportReplayProgress(final LocalStore.ThrottledLoader loader) {
        final long now = System.nanoTime();
        if (now - replayProgressAtNanos < REPLAY_PROGRESS_INTERVAL_NANOS) {
            return;
        }
        replayProgressAtNanos = now;
        stillWaiting("replayed " + loader.appliedEntries() + " of " + loader.snapshotSize()
                + " snapshot entries");
    }

    /**
     * Local replay is done. Everything durable this node holds has been seen, so its two reserved
     * bounds are final and it can answer a peer's ceiling request from here.
     * <p>
     * A node that replayed <em>nothing</em> has no floor of its own and must get one from the
     * cluster before it may promise, accept or serve -- so readiness waits on that, not on replay.
     * The peer handler is registered first because the exchange runs over it.
     */
    private void onReplayComplete() {
        peerTransport.register(message -> loop.execute(() -> dispatchPeer(message)));

        // Asked here and nowhere earlier: the answer is continuity of the log, which is established
        // by the replay that just finished reading it.
        final boolean proven = wal.ceilingIsProven();
        ceilingRecovery.afterReplay(proven);
        if (proven) {
            serve("keys=" + store.keyCount());
            return;
        }
        enter(NodeState.AWAITING_FLOOR, "no ceiling this node can prove", null);
        ceilingRecovery.start(this::serve);
    }

    private void serve(final String detail) {
        proposer.rehydrateAfterRecovery();

        enter(NodeState.SERVING, detail, null);

        antiEntropy.startPeriodicRepair(repairInterval);
        // Only once this node serves: a sweep asks every member, and a node that is not serving is
        // one of the members that would have to answer.
        tombstoneSweeper.start();

        loop.scheduleRepeat(cfg.snapshotInterval(), snapshotScheduler::startSnapshot);
        loop.scheduleRepeat(cfg.promiseEvictionInterval(), store::evictPromiseOnly);
        loop.scheduleRepeat(cfg.walForceInterval(), wal::force);
    }

    /**
     * What this node can honestly say about how far its storage has promised, which is exactly one
     * of three things and never a guess. Read on the loop, at every handshake, because the ceiling
     * rises.
     * <p>
     * Only a serving node makes a claim. The two that do not are different situations and the
     * transport treats them differently -- a node still replaying is refused a handshake until it
     * has one, a node that replayed but cannot prove its ceiling is admitted, because being
     * admitted is how it gets a floor and it serves nothing until it does. See
     * {@link PeerTransport#PROMISE_CEILING_UNPROVEN}.
     */
    private long promiseCeilingClaim() {
        if (state == NodeState.SERVING) {
            return store.promisedUpTo();
        }
        return state == NodeState.AWAITING_FLOOR
                ? PeerTransport.PROMISE_CEILING_UNPROVEN
                : PeerTransport.PROMISE_CEILING_REPLAYING;
    }

    private void dispatchPeer(final PeerMessage message) {
        // The ceiling exchange runs before readiness by design: a node that started without state
        // needs it to become ready at all, and a cluster starting from nothing has every member in
        // that position at once. Both directions are safe here -- answering reports only bounds
        // replay has already established, and a response is only accepted while this node is asking.
        if (state == NodeState.AWAITING_FLOOR || state == NodeState.SERVING) {
            if (message instanceof PeerMessage.CeilingReq) {
                ceilingRecovery.handleRequest((PeerMessage.CeilingReq) message);
                return;
            }
            if (message instanceof PeerMessage.CeilingResp) {
                ceilingRecovery.onResponse((PeerMessage.CeilingResp) message);
                return;
            }
        }
        if (state != NodeState.SERVING) {
            // Dropping is correct here: Paxos assumes lossy links, and the sender's round timeout
            // already treats silence as a non-vote. Synthesising a NACK would be worse -- it
            // carries a promised ballot, and inventing one could perturb the sender's ballot
            // space. Reported so an operator can see recovery shedding load.
            observer.requestBeforeReady(true);
            return;
        }

        if (message instanceof PeerMessage.PrepareReq) {
            final PeerMessage.PrepareReq prepareRequest = (PeerMessage.PrepareReq) message;
            try {
                final PeerMessage.PrepareResp response = acceptor.handlePrepare(prepareRequest);
                peerTransport.send(prepareRequest.senderId(), response);
            } catch (final Exception e) {
                observer.prepareHandlerFailed(prepareRequest.key(), e);
                try {
                    final Ballot errorBallot = new Ballot(prepareRequest.ballot().counter() + 1, nodeId);
                    peerTransport.send(prepareRequest.senderId(),
                            new PeerMessage.PrepareResp(nodeId, prepareRequest.correlationId(),
                                    false, errorBallot, Ballot.ZERO, null, false));
                } catch (final Exception ignored) {
                }
            }
            return;
        }
        if (message instanceof PeerMessage.AcceptReq) {
            final PeerMessage.AcceptReq acceptRequest = (PeerMessage.AcceptReq) message;
            try {
                final PeerMessage.AcceptResp response = acceptor.handleAccept(acceptRequest);
                peerTransport.send(acceptRequest.senderId(), response);
            } catch (final Exception e) {
                observer.acceptHandlerFailed(acceptRequest.key(), e);
                try {
                    final Ballot errorBallot = new Ballot(acceptRequest.ballot().counter() + 1, nodeId);
                    peerTransport.send(acceptRequest.senderId(),
                            new PeerMessage.AcceptResp(nodeId, acceptRequest.correlationId(), false, errorBallot));
                } catch (final Exception ignored) {
                }
            }
            return;
        }
        if (message instanceof PeerMessage.PrepareResp) {
            proposer.onPrepareResp((PeerMessage.PrepareResp) message);
            return;
        }
        if (message instanceof PeerMessage.AcceptResp) {
            proposer.onAcceptResp((PeerMessage.AcceptResp) message);
            return;
        }
        if (message instanceof PeerMessage.DigestReq) {
            antiEntropy.handleDigestReq((PeerMessage.DigestReq) message);
            return;
        }
        if (message instanceof PeerMessage.KeysReq) {
            antiEntropy.handleKeysReq((PeerMessage.KeysReq) message);
            return;
        }
        if (message instanceof PeerMessage.DigestResp) {
            antiEntropy.onDigestResp((PeerMessage.DigestResp) message);
            return;
        }
        if (message instanceof PeerMessage.KeysResp) {
            antiEntropy.onKeysResp((PeerMessage.KeysResp) message);
            return;
        }
        if (message instanceof PeerMessage.PurgeCheckReq) {
            // Silence is a legitimate answer here -- it blocks the collection, which is the safe
            // outcome -- so there is no error reply to synthesise, unlike prepare and accept.
            final PeerMessage.PurgeCheckReq check = (PeerMessage.PurgeCheckReq) message;
            peerTransport.send(check.senderId(), new PeerMessage.PurgeCheckResp(
                    nodeId, check.correlationId(),
                    store.purgeCheck(check.key(), check.tombstoneBallot())));
            return;
        }
        if (message instanceof PeerMessage.PurgeReq) {
            // Nothing to reply to and nothing to retry: a member that misses this keeps the
            // tombstone, and the next sweep asks again.
            final PeerMessage.PurgeReq purge = (PeerMessage.PurgeReq) message;
            store.purge(purge.key(), purge.tombstoneBallot());
            return;
        }
        if (message instanceof PeerMessage.PurgeCheckResp) {
            tombstoneCollector.onCheckResp((PeerMessage.PurgeCheckResp) message);
            return;
        }
    }

    private void dispatchClient(final ClientId clientId, final ClientMessage message,
                                final ResponseSink replySink) {
        if (state != NodeState.SERVING) {
            observer.requestBeforeReady(false);
            replyNotReady(message, replySink);
            return;
        }

        if (message instanceof ClientMessage.ClientGetReq) {
            clientHandler.handleGet(clientId, (ClientMessage.ClientGetReq) message, replySink);
            return;
        }
        if (message instanceof ClientMessage.ClientPutReq) {
            clientHandler.handlePut(clientId, (ClientMessage.ClientPutReq) message, replySink);
            return;
        }
        if (message instanceof ClientMessage.ClientCasReq) {
            clientHandler.handleCas(
                    clientId, (ClientMessage.ClientCasReq) message, replySink);
            return;
        }
        if (message instanceof ClientMessage.ClientDeleteReq) {
            clientHandler.handleDelete(clientId, (ClientMessage.ClientDeleteReq) message, replySink);
            return;
        }
        if (message instanceof ClientMessage.ClientScanReq) {
            clientHandler.handleScan(clientId, (ClientMessage.ClientScanReq) message, replySink);
            return;
        }
    }

    /**
     * Refuse a client request that arrived before recovery finished, with
     * {@link ClientErrorCode#UNAVAILABLE}.
     * <p>
     * Dropping it in silence would cost the client its full per-attempt timeout against every node
     * in turn -- on a cluster restart, where every node is recovering at once, the whole retry
     * budget spent learning nothing. An explicit retryable refusal lets it move on immediately.
     * <p>
     * Scan is the exception: {@code ClientScanResp} carries no error field, and an empty page is
     * indistinguishable from "this node holds no keys". Staying silent makes this node a
     * non-responder, which is exactly what the scan quorum check expects.
     */
    private void replyNotReady(final ClientMessage message, final ResponseSink replySink) {
        final long correlationId = message.correlationId();
        final String reason = "node " + nodeId + " is recovering";
        if (message instanceof ClientMessage.ClientGetReq) {
            replySink.send(new ClientMessage.ClientGetResp(
                    nodeId.value(), correlationId, false, null, reason,
                    ClientErrorCode.NOT_READY));
        } else if (message instanceof ClientMessage.ClientPutReq) {
            replySink.send(new ClientMessage.ClientPutResp(
                    nodeId.value(), correlationId, false, reason, ClientErrorCode.NOT_READY));
        } else if (message instanceof ClientMessage.ClientCasReq) {
            replySink.send(new ClientMessage.ClientCasResp(
                    nodeId.value(), correlationId, false, false, null,
                    Ballot.ZERO, reason, ClientErrorCode.NOT_READY));
        } else if (message instanceof ClientMessage.ClientDeleteReq) {
            replySink.send(new ClientMessage.ClientDeleteResp(
                    nodeId.value(), correlationId, false, reason, ClientErrorCode.NOT_READY));
        }
    }

    private final class SnapshotScheduler {
        private LocalStore.ThrottledSnapshot activeSnapshotWriter = null;

        private void startSnapshot() {
            if (activeSnapshotWriter != null) {
                return;
            }
            activeSnapshotWriter = store.beginSnapshot();
            observer.snapshotStarted();
            writeNextBatch();
        }

        private void writeNextBatch() {
            if (activeSnapshotWriter == null) {
                return;
            }
            try {
                final boolean more = activeSnapshotWriter.writeBatch(SNAPSHOT_BATCH_SIZE);
                if (more) {
                    loop.execute(this::writeNextBatch);
                } else {
                    activeSnapshotWriter = null;
                    observer.snapshotCompleted();
                }
            } catch (final Exception e) {
                final boolean committed = activeSnapshotWriter.isCommitted();
                observer.snapshotFailed(e, committed);
                if (committed) {
                    enter(NodeState.FAILED, "snapshot failed after commit", e);
                    loop.shutdown();
                } else {
                    activeSnapshotWriter.abort();
                }
                activeSnapshotWriter = null;
            }
        }
    }

    public EventLoop loop() {
        return loop;
    }

    /**
     * This node's store, for tests in this package.
     * <p>
     * Package-private because the protocol makes the difference invisible from outside: a key that
     * is absent and a key held as a tombstone read the same to a client, and tombstone collection
     * is precisely the difference between them. Loop-confined like every other read of it -- call
     * it from {@link #loop()}.
     */
    LocalStore store() {
        return store;
    }

    public NodeId nodeId() {
        return nodeId;
    }

    /**
     * The cluster size {@code N} this node was started with -- the same frozen quorum basis
     * the {@code Proposer} uses. Reported to clients in CLIENT_HELLO_RESP so a client can
     * derive the real quorum instead of guessing from its own (possibly partial) node list.
     *
     * @see io.github.green4j.discas.node.transport.PeerTransport#clusterSize()
     */
    public int clusterSize() {
        return peerTransport.clusterSize();
    }

    /**
     * A read-only view of this node's health, for an observability endpoint.
     * <p>
     * Handing out a {@link HealthSource} rather than making the state public keeps the probe's reach
     * to exactly the two facts it needs. The returned view is live -- it reflects the node as it
     * changes, rather than a snapshot taken now -- and is safe to read from another thread.
     */
    public HealthSource healthSource() {
        return new HealthSource() {
            @Override
            public NodeState state() {
                return state;
            }

            @Override
            public boolean walDegraded() {
                return wal.isDegraded();
            }

            @Override
            public String walDegradedReason() {
                return wal.degradedReason();
            }
        };
    }
}
