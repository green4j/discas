/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;

/**
 * The acceptor half of the protocol: it promises, it accepts, and it refuses.
 *
 * <h2>Two floors, not one</h2>
 * A ballot has to clear <em>both</em>:
 * <ul>
 *   <li>{@code keyState.promised} -- what this incarnation has actually promised for this key;</li>
 *   <li>{@link LocalStore#recoveryPromiseFloor()} -- what a <em>previous</em> incarnation may have
 *       promised for <em>any</em> key and forgotten, because a promise is acknowledged before the
 *       WAL is forced.</li>
 * </ul>
 * The second floor is what makes a lost WAL tail survivable. Without it a node restarting inside the
 * sync window could accept a ballot lower than one it had already promised against, and two
 * different values could be chosen -- a safety failure, not a durability one, because a promise was
 * never replicated and so no quorum can repair it.
 *
 * <h2>And a ceiling</h2>
 * Nothing may be promised or accepted above {@link LocalStore#promisedUpTo()} until that ceiling has
 * been durably raised. That is the write the floor above is derived from, and it is the one forced
 * write on this path -- paid once per chunk of ballot space, not once per promise.
 */
public final class Acceptor {

    /**
     * How far past the requested counter the ceiling is raised. Amortizes the forced write: a
     * proposer advancing one ballot at a time costs one sync per this many rounds. The cost of
     * overshooting is that a restart refuses a slightly wider band of ballots, which costs the
     * proposer one round.
     */
    private static final long CEILING_CHUNK = 1024;

    private final NodeId nodeId;
    private final LocalStore store;
    private final NodeObserver observer;

    public Acceptor(final NodeId nodeId, final LocalStore store) {
        this(nodeId, store, NodeObserver.NONE);
    }

    public Acceptor(final NodeId nodeId, final LocalStore store, final NodeObserver observer) {
        this.nodeId = nodeId;
        this.store = store;
        this.observer = observer == null ? NodeObserver.NONE : observer;
    }

    /**
     * Prepare handler; may be invoked locally by the same-node proposer
     */
    public PeerMessage.PrepareResp handlePrepare(final PeerMessage.PrepareReq request) {
        final KeyState keyState = store.getOrCreate(request.key());

        if (store.isWalDegraded()) {
            // WAL has hit an unrecoverable I/O failure; reply NACK so the
            // proposer drops this acceptor from quorum but the node stays
            // up to serve other traffic.
            return nack(request.correlationId(), keyState);
        }

        // >= instead of > makes retransmits of the same ballot
        // idempotent. A proposer that re-sends its prepare (e.g. after
        // a timeout with no response) will receive an ACK rather than a
        // NACK. This matches handleAccept which already uses >=
        if (request.ballot().compareTo(keyState.promised) < 0) {
            return nack(request.correlationId(), keyState);
        }
        if (!clearsRecoveryFloor(request.ballot())) {
            return nack(request.correlationId(), keyState);
        }
        if (!raiseCeilingFor(request.ballot())) {
            return nack(request.correlationId(), keyState);
        }

        store.writePromise(request.key(), request.ballot());
        return new PeerMessage.PrepareResp(
                nodeId, request.correlationId(), true,
                request.ballot(), keyState.accepted,
                keyState.value, keyState.tombstone);
    }

    /**
     * Accept handler; may be invoked locally
     */
    public PeerMessage.AcceptResp handleAccept(final PeerMessage.AcceptReq request) {
        final KeyState keyState = store.getOrCreate(request.key());

        if (store.isWalDegraded()) {
            return new PeerMessage.AcceptResp(
                    nodeId, request.correlationId(), false, refusalBallot(keyState));
        }

        // The same two floors and the same ceiling as handlePrepare, because an accept can arrive
        // for a ballot this node never promised: a proposer may reach quorum on a prepare that
        // excluded us and then broadcast the accept to everyone.
        if (request.ballot().compareTo(keyState.promised) < 0
                || !clearsRecoveryFloor(request.ballot())
                || !raiseCeilingFor(request.ballot())) {
            return new PeerMessage.AcceptResp(
                    nodeId, request.correlationId(), false, refusalBallot(keyState));
        }

        // No room for the pair. Refusing here is safe in exactly the way a peer being down is safe:
        // safety rests on an acceptor never accepting below what it promised, never on it always
        // accepting, so a refusal is indistinguishable from silence and Paxos is built to tolerate
        // silence. What it costs is determinacy -- a minority may have taken the value before this
        // one refused, so the round's caller is told UNAVAILABLE rather than STORE_FULL. The
        // coordinator's own check runs before the round precisely so that this one is the rare case.
        if (!store.hasCapacityFor(request.key(), request.value(), request.tombstone())) {
            observer.writeRefusedNoCapacity(request.key(), false);
            return new PeerMessage.AcceptResp(
                    nodeId, request.correlationId(), false, refusalBallot(keyState));
        }

        store.writeAccept(request.key(), request.ballot(),
                request.value(), request.tombstone());
        return new PeerMessage.AcceptResp(
                nodeId, request.correlationId(), true, request.ballot());
    }

    /**
     * Whether {@code ballot} is above everything a previous incarnation could have promised.
     * <p>
     * Strictly above: a counter equal to the floor is exactly a counter that may have been promised
     * and lost, so the retransmit-idempotency argument for {@code >=} against
     * {@code keyState.promised} does not carry over here -- that argument rests on knowing what was
     * promised, which is the knowledge this floor exists because we lack.
     */
    private boolean clearsRecoveryFloor(final Ballot ballot) {
        return ballot.counter() > store.recoveryPromiseFloor();
    }

    /** Durably reserve the right to promise this counter, if it is not already reserved. */
    private boolean raiseCeilingFor(final Ballot ballot) {
        if (ballot.counter() <= store.promisedUpTo()) {
            return true;
        }
        return store.reservePromiseCeiling(ballot.counter() + CEILING_CHUNK - 1);
    }

    /**
     * The ballot reported on a refusal: high enough that a proposer bumping past it will clear both
     * floors on its next attempt.
     * <p>
     * {@code Proposer.onPrepareResp} requires a legitimate NACK to carry a ballot strictly greater
     * than the round's, and {@code observeExternalBallot} then bumps the proposer past it. Reporting
     * only {@code keyState.promised} would let a proposer refused by the recovery floor retry at a
     * counter the floor rejects again, once per attempt, until its retries ran out.
     */
    private Ballot refusalBallot(final KeyState keyState) {
        final long floor = store.recoveryPromiseFloor();
        if (keyState.promised.counter() >= floor) {
            return keyState.promised;
        }
        return new Ballot(floor, nodeId);
    }

    private PeerMessage.PrepareResp nack(final long correlationId, final KeyState keyState) {
        return new PeerMessage.PrepareResp(
                nodeId, correlationId, false,
                refusalBallot(keyState), Ballot.ZERO, null, false);
    }
}
