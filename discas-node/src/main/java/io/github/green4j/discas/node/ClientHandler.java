/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.client.ResponseSink;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.acl.ClientAuthorizer;
import io.github.green4j.discas.node.acl.ClientOp;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Translates client requests into CAS rounds on the event loop.
 * <p>
 * Every operation is first checked against the {@link ClientAuthorizer} using the
 * <b>trusted</b> {@link ClientId} bound to the connection at CLIENT_HELLO time (never the
 * self-declared {@code senderId} in the request); a denied operation returns an
 * "access denied" error response without touching consensus.
 *
 * <h2>Where the wire becomes storage</h2>
 * This is the boundary. A {@link ClientMessage} carries plain {@link ByteBuffer}s; everything
 * below -- the store, the proposer, the WAL -- speaks {@link HashedBytes}, because it hashes
 * and compares keys repeatedly and keeps them long after the request is gone. Each handler
 * therefore converts once, up front, before any asynchronous work, and uses only the converted
 * form from there on.
 * <p>
 * The conversion is {@link HashedBytes#adopt} rather than a copy: the decoded message already
 * owns a private read-only copy of its bytes (see the ownership rule on {@code ClientMessage}),
 * so copying again would buy nothing. Size limits are checked first, off the raw buffers, so an
 * oversized request is rejected before anything is adopted at all.
 * <p>
 * Scans are the exception that proves the rule: a prefix and a cursor are only ever compared,
 * so they go to the store as buffers and are never converted.
 */
final class ClientHandler {
    /**
     * Human-readable detail accompanying {@link ClientErrorCode#ACCESS_DENIED}; the code, not
     * this text, is what callers distinguish the case by.
     */
    static final String ACCESS_DENIED = ClientMessage.ERR_ACCESS_DENIED;

    /**
     * Detail accompanying {@link ClientErrorCode#NO_QUORUM_AT_COORDINATOR} when the refusal is
     * taken up front, from a verdict a previous round already established.
     */
    private static final String NO_MAJORITY = "this coordinator cannot see a majority";

    static final String STORE_FULL = "the store is at its configured share of the heap; "
            + "delete something before writing more";

    /**
     * The client-facing code for a round that gave up, taken from the {@link RoundFailure} the
     * proposer attached rather than from the message text.
     * <p>
     * The proposer knows which phase it died in and the client does not, so this is the only place
     * that information can be preserved. Two things ride on it, and they are different questions:
     * <ul>
     *   <li><b>Is another coordinator worth trying?</b> Only
     *       {@link RoundFailure#INSUFFICIENT_RESPONDERS} means <em>this node</em> could not see a
     *       majority, so only it maps to {@link ClientErrorCode#NO_QUORUM_AT_COORDINATOR}.</li>
     *   <li><b>Is the outcome known?</b> {@link RoundFailure#BALLOT_NACK} lost the duel before
     *       Accept, so nothing was accepted and the answer is determinate --
     *       {@link ClientErrorCode#BALLOT_LOST}. {@link RoundFailure#ACCEPT_TIMEOUT} got as far as
     *       Accept, so some acceptors may hold the proposal and a later proposer can complete it:
     *       that is {@link ClientErrorCode#UNAVAILABLE}, which now means <em>indeterminate</em>
     *       and nothing else.</li>
     * </ul>
     * Collapsing the last two would leave a client unable to tell a free retry from an unsafe
     * one.
     */
    private static ClientErrorCode roundErrorCode(final Throwable error) {
        Throwable cause = error;
        // whenCompleteAsync hands back the raw exception for a directly-completed future, but a
        // future completed through a composition stage arrives wrapped.
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof RoundFailedException) {
            switch (((RoundFailedException) cause).failure()) {
                case INSUFFICIENT_RESPONDERS:
                    return ClientErrorCode.NO_QUORUM_AT_COORDINATOR;
                case BALLOT_NACK:
                    return ClientErrorCode.BALLOT_LOST;
                case PROPOSAL_EXPIRED:
                    return ClientErrorCode.PROPOSAL_EXPIRED;
                case ACCEPT_TIMEOUT:
                default:
                    return ClientErrorCode.UNAVAILABLE;
            }
        }
        // Not a round failure at all -- a saturated proposer, an exception on the node side.
        // Indeterminate by default: the safe reading when the phase is unknown.
        return ClientErrorCode.UNAVAILABLE;
    }

    private final String senderId;
    private final Proposer proposer;
    private final LocalStore store;
    private final EventLoop loop;
    private final ClientAuthorizer authorizer;
    private final NodeObserver observer;
    private final Executor asyncCallbacks;

    ClientHandler(final NodeId nodeId, final Proposer proposer,
                         final LocalStore store, final EventLoop loop,
                         final ClientAuthorizer authorizer) {
        this(nodeId, proposer, store, loop, authorizer, NodeObserver.NONE);
    }

    ClientHandler(final NodeId nodeId, final Proposer proposer,
                         final LocalStore store, final EventLoop loop,
                         final ClientAuthorizer authorizer, final NodeObserver observer) {
        this.senderId = nodeId.value();
        this.proposer = proposer;
        this.store = store;
        this.loop = loop;
        this.authorizer = authorizer;
        this.observer = observer == null ? NodeObserver.NONE : observer;
        this.asyncCallbacks = loop.rejectingExecutor();
    }

    public void handleGet(final ClientId clientId,
                          final ClientMessage.ClientGetReq request, final ResponseSink replySink) {
        final String sizeError = sizeError(request.key());
        if (sizeError != null) {
            replySink.send(new ClientMessage.ClientGetResp(
                    senderId, request.correlationId(), false, null, sizeError,
                    ClientErrorCode.INVALID_ARGUMENT));
            return;
        }
        final HashedBytes key = HashedBytes.adopt(request.key());
        if (!authorizer.allow(clientId, ClientOp.GET, key)) {
            replySink.send(new ClientMessage.ClientGetResp(
                    senderId, request.correlationId(), false, null, ACCESS_DENIED,
                    ClientErrorCode.ACCESS_DENIED));
            return;
        }
        if (request.consistency() == ReadConsistency.SERIALIZABLE) {
            // Serializable read: return this node's locally-committed value with no
            // Paxos round and no WAL write. May be stale/dirty under concurrent
            // writers; callers opt in explicitly (see ReadConsistency.SERIALIZABLE).
            final KeyState keyState = store.get(key);
            HashedBytes value = null;
            if (keyState != null && !keyState.accepted.isZero() && !keyState.tombstone) {
                value = keyState.value;
            }
            final Ballot version = keyState != null ? keyState.accepted : Ballot.ZERO;
            observer.serializableRead(key);
            replySink.send(new ClientMessage.ClientGetResp(
                    senderId, request.correlationId(), true, HashedBytes.toBuffer(value), null,
                    ClientErrorCode.NONE, version));
            return;
        }
        // A round already discovered this node cannot reach a majority. Spending another whole
        // round timeout to rediscover it helps nobody: refuse now so the client switches
        // coordinators at once. Only linearizable work is gated -- the serializable branch above
        // and handleScan below are served from local state and stay available.
        if (!proposer.canReachMajority()) {
            observer.roundRefusedNoMajority(key);
            replySink.send(new ClientMessage.ClientGetResp(
                    senderId, request.correlationId(), false, null, NO_MAJORITY,
                    ClientErrorCode.NO_QUORUM_AT_COORDINATOR));
            return;
        }
        try {
            proposer.identityRound(key).whenCompleteAsync((result, error) -> {
                if (error != null) {
                    replySink.send(new ClientMessage.ClientGetResp(
                            senderId, request.correlationId(), false, null, error.getMessage(),
                            roundErrorCode(error)));
                } else {
                    HashedBytes value = null;
                    if (result.keyExists() && !result.tombstone()) {
                        value = result.value();
                    }
                    replySink.send(new ClientMessage.ClientGetResp(
                            senderId, request.correlationId(), true, HashedBytes.toBuffer(value),
                            null, ClientErrorCode.NONE, result.version()));
                }
            }, asyncCallbacks);
        } catch (final Exception e) {
            replySink.send(new ClientMessage.ClientGetResp(
                    senderId, request.correlationId(), false, null,
                    "Internal error: " + e.getMessage(), ClientErrorCode.INTERNAL));
        }
    }

    public void handlePut(final ClientId clientId,
                          final ClientMessage.ClientPutReq request, final ResponseSink replySink) {
        final String sizeError = sizeError(request.key(), request.value());
        if (sizeError != null) {
            replySink.send(new ClientMessage.ClientPutResp(
                    senderId, request.correlationId(), false, sizeError,
                    ClientErrorCode.INVALID_ARGUMENT));
            return;
        }
        final HashedBytes key = HashedBytes.adopt(request.key());
        final HashedBytes value = HashedBytes.adopt(request.value());
        if (!authorizer.allow(clientId, ClientOp.PUT, key)) {
            replySink.send(new ClientMessage.ClientPutResp(
                    senderId, request.correlationId(), false, ACCESS_DENIED,
                    ClientErrorCode.ACCESS_DENIED));
            return;
        }
        if (!proposer.canReachMajority()) {
            observer.roundRefusedNoMajority(key);
            replySink.send(new ClientMessage.ClientPutResp(
                    senderId, request.correlationId(), false, NO_MAJORITY,
                    ClientErrorCode.NO_QUORUM_AT_COORDINATOR));
            return;
        }
        if (!store.hasCapacityFor(key, value, false)) {
            observer.writeRefusedNoCapacity(key, true);
            replySink.send(new ClientMessage.ClientPutResp(
                    senderId, request.correlationId(), false, STORE_FULL,
                    ClientErrorCode.STORE_FULL));
            return;
        }
        try {
            proposer.write(key, oldValue -> value).whenCompleteAsync((result, error) -> {
                if (error != null) {
                    replySink.send(new ClientMessage.ClientPutResp(
                            senderId, request.correlationId(), false, error.getMessage(),
                            roundErrorCode(error)));
                } else {
                    replySink.send(new ClientMessage.ClientPutResp(
                            senderId, request.correlationId(), true, null, ClientErrorCode.NONE,
                            result.version()));
                }
            }, asyncCallbacks);
        } catch (final Exception e) {
            replySink.send(new ClientMessage.ClientPutResp(
                    senderId, request.correlationId(), false,
                    "Internal error: " + e.getMessage(), ClientErrorCode.INTERNAL));
        }
    }

    /**
     * CAS fenced on the key's accepted ballot rather than on its bytes.
     * <p>
     * The comparison happens inside the round, against quorum-agreed state, so nothing here has to
     * infer whether the swap took: {@link Proposer.CasResult#fenceMatched()} says so directly. The
     * reply carries the version as observed, which is what lets a caller that lost the compare
     * recompute without a second round trip -- and what makes this write safe to re-send after a
     * coordinator stops answering.
     */
    public void handleCas(final ClientId clientId,
                                   final ClientMessage.ClientCasReq request,
                                   final ResponseSink replySink) {
        final String sizeError = sizeError(request.key(), request.desired());
        if (sizeError != null) {
            replySink.send(new ClientMessage.ClientCasResp(
                    senderId, request.correlationId(), false, false, null, Ballot.ZERO, sizeError,
                    ClientErrorCode.INVALID_ARGUMENT));
            return;
        }
        final HashedBytes key = HashedBytes.adopt(request.key());
        final HashedBytes desired = HashedBytes.adopt(request.desired());
        // A null desired commits a tombstone, which is a delete however it is spelled on the
        // wire, so it needs the DELETE grant as well: otherwise a CAS-only grant would remove
        // keys the ACL is written to protect.
        if (!authorizer.allow(clientId, ClientOp.CAS, key)
                || (desired == null && !authorizer.allow(clientId, ClientOp.DELETE, key))) {
            replySink.send(new ClientMessage.ClientCasResp(
                    senderId, request.correlationId(), false, false, null, Ballot.ZERO,
                    ACCESS_DENIED, ClientErrorCode.ACCESS_DENIED));
            return;
        }

        if (!proposer.canReachMajority()) {
            observer.roundRefusedNoMajority(key);
            replySink.send(new ClientMessage.ClientCasResp(
                    senderId, request.correlationId(), false, false, null, Ballot.ZERO,
                    NO_MAJORITY, ClientErrorCode.NO_QUORUM_AT_COORDINATOR));
            return;
        }
        // A CAS to null is a delete, and a delete is never refused for want of room: a store that
        // cannot shrink when it is full has no way back.
        if (!store.hasCapacityFor(key, desired, desired == null)) {
            observer.writeRefusedNoCapacity(key, true);
            replySink.send(new ClientMessage.ClientCasResp(
                    senderId, request.correlationId(), false, false, null, Ballot.ZERO,
                    STORE_FULL, ClientErrorCode.STORE_FULL));
            return;
        }

        try {
            proposer.cas(key, request.expectedVersion(), current -> desired)
                    .whenCompleteAsync((result, error) -> {
                        if (error != null) {
                            replySink.send(new ClientMessage.ClientCasResp(
                                    senderId, request.correlationId(), false, false, null,
                                    Ballot.ZERO, error.getMessage(), roundErrorCode(error)));
                            return;
                        }

                        final HashedBytes observed = (result.keyExists() && !result.tombstone())
                                ? result.value() : null;
                        replySink.send(new ClientMessage.ClientCasResp(
                                senderId, request.correlationId(), true, result.fenceMatched(),
                                HashedBytes.toBuffer(observed), result.version(), null,
                                ClientErrorCode.NONE));
                    }, asyncCallbacks);
        } catch (final Exception e) {
            replySink.send(new ClientMessage.ClientCasResp(
                    senderId, request.correlationId(), false, false, null, Ballot.ZERO,
                    "Internal error: " + e.getMessage(), ClientErrorCode.INTERNAL));
        }
    }

    public void handleDelete(final ClientId clientId,
                             final ClientMessage.ClientDeleteReq request, final ResponseSink replySink) {
        final String sizeError = sizeError(request.key());
        if (sizeError != null) {
            replySink.send(new ClientMessage.ClientDeleteResp(
                    senderId, request.correlationId(), false, sizeError,
                    ClientErrorCode.INVALID_ARGUMENT));
            return;
        }
        final HashedBytes key = HashedBytes.adopt(request.key());
        if (!authorizer.allow(clientId, ClientOp.DELETE, key)) {
            replySink.send(new ClientMessage.ClientDeleteResp(
                    senderId, request.correlationId(), false, ACCESS_DENIED,
                    ClientErrorCode.ACCESS_DENIED));
            return;
        }
        if (!proposer.canReachMajority()) {
            observer.roundRefusedNoMajority(key);
            replySink.send(new ClientMessage.ClientDeleteResp(
                    senderId, request.correlationId(), false, NO_MAJORITY,
                    ClientErrorCode.NO_QUORUM_AT_COORDINATOR));
            return;
        }
        try {
            proposer.write(key, oldValue -> null).whenCompleteAsync((result, error) -> {
                if (error != null) {
                    replySink.send(new ClientMessage.ClientDeleteResp(
                            senderId, request.correlationId(), false, error.getMessage(),
                            roundErrorCode(error)));
                } else {
                    replySink.send(new ClientMessage.ClientDeleteResp(
                            senderId, request.correlationId(), true, null, ClientErrorCode.NONE,
                            result.version()));
                }
            }, asyncCallbacks);
        } catch (final Exception e) {
            replySink.send(new ClientMessage.ClientDeleteResp(
                    senderId, request.correlationId(), false,
                    "Internal error: " + e.getMessage(), ClientErrorCode.INTERNAL));
        }
    }

    void handleScan(final ClientId clientId,
                           final ClientMessage.ClientScanReq request, final ResponseSink replySink) {
        try {
            // The ACL is applied as the page is filled, not to a finished page: filtering
            // afterwards would shorten the page below the limit even when more matching keys
            // remain, which the client cannot distinguish from exhaustion and would make it
            // stop paginating early.
            final LocalStore.ScanPage page = store.scanLocal(
                    request.prefix(), request.startAfter(), request.limit(),
                    key -> authorizer.allow(clientId, ClientOp.SCAN, key));
            replySink.send(new ClientMessage.ClientScanResp(
                    senderId, request.correlationId(), page.entries(), page.hasMore()));
        } catch (final Exception e) {
            // Deliberately send NOTHING. An empty ClientScanResp is indistinguishable from
            // "this node holds no keys", so replying on failure would let the client count a
            // failed node towards its scan quorum and silently merge in a phantom empty store.
            // Staying silent makes this node a non-responder, which is exactly what it is; the
            // client's scan timeout and quorum check then handle it correctly.
            observer.scanFailed(e);
        }
    }

    /**
     * The size limit a request breached, or {@code null} when it breached none. Enforced here
     * (node ingress) so an oversized request is rejected cleanly rather than failing downstream
     * in the transport reassembler or the WAL writer.
     * <p>
     * Reads the request's raw buffers, before any of them is adopted into storage form, so an
     * oversized request costs nothing beyond the length check.
     * <p>
     * Fixed arity rather than varargs: this runs on every put, cas and delete, where varargs
     * would allocate an array per request to carry at most two values.
     */
    private static String sizeError(final ByteBuffer key) {
        return sizeError(key, null, null);
    }

    private static String sizeError(final ByteBuffer key, final ByteBuffer value) {
        return sizeError(key, value, null);
    }

    private static String sizeError(final ByteBuffer key, final ByteBuffer first,
                                    final ByteBuffer second) {
        if (key != null && key.remaining() > KvLimits.MAX_KEY_BYTES) {
            return "key size " + key.remaining()
                    + " exceeds maximum " + KvLimits.MAX_KEY_BYTES + " bytes";
        }
        final String firstError = valueSizeError(first);
        if (firstError != null) {
            return firstError;
        }
        return valueSizeError(second);
    }

    private static String valueSizeError(final ByteBuffer value) {
        if (value != null && value.remaining() > KvLimits.MAX_VALUE_BYTES) {
            return "value size " + value.remaining() + " exceeds maximum "
                    + KvLimits.MAX_VALUE_BYTES + " bytes";
        }
        return null;
    }

}
