/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerHelloRespStatus;

/**
 * The one seam every lifecycle, consensus, anti-entropy and peer-handshake event a node produces is
 * reported through, so a logging or metrics backend attaches without touching protocol code.
 * <p>
 * All methods are {@code default} no-ops: an implementation overrides only the events it cares
 * about, and {@link #NONE} reports nothing.
 * <p>
 * <b>Threading:</b> every callback fires on the node's event loop, so an implementation must not
 * block. Some fire on hot paths ({@link #roundCommitted}), so it must not allocate or build strings
 * either -- arguments are passed as raw handles ({@link HashedBytes}, {@link Ballot}) precisely so
 * that a no-op default costs nothing.
 */
public interface NodeObserver {

    /** No-op observer: the default. Reports nothing, allocates nothing. */
    NodeObserver NONE = new NodeObserver() {
    };

    /**
     * The node moved from one {@link NodeState} to another -- the whole start/restart model, and the
     * only place it is reported. A repeated state ({@code from == to}) is a state that is still
     * waiting for something and says so again.
     *
     * @param detail one short line of numbers behind the transition ({@code "keys=1240"},
     *               {@code "floor=5119 from 2 witnesses"}), or {@code null}
     * @param cause  what forced the transition, for {@link NodeState#FAILED}; {@code null} otherwise
     */
    default void nodeState(final NodeState from, final NodeState to,
                           final String detail, final Throwable cause) {
    }

    /**
     * A request arrived while the node was not {@link NodeState#SERVING} and was refused. Client
     * requests are answered with {@code UNAVAILABLE}; peer messages are dropped, which the sender's
     * round timeout already handles.
     *
     * @param peerMessage true for a dropped peer message, false for a refused client request
     */
    default void requestBeforeReady(final boolean peerMessage) {
    }

    /** A background snapshot write has started. */
    default void snapshotStarted() {
    }

    /** A background snapshot committed successfully. */
    default void snapshotCompleted() {
    }

    /**
     * A background snapshot write failed. {@code committed} indicates whether the
     * failure happened after the snapshot had already been committed (fail-stop) or
     * before (the partial snapshot was aborted).
     */
    default void snapshotFailed(final Throwable error, final boolean committed) {
    }

    /** The WAL has entered its unrecoverable degraded state; the node NACKs consensus. */
    default void walDegraded(final String reason) {
    }

    /** A lifecycle-bound resource failed to close during shutdown. */
    default void lifecycleResourceCloseFailed(final Throwable error) {
    }

    /**
     * A task, timer or shutdown-drain callback threw on the node's event loop. The loop catches
     * and carries on -- one bad task must not stop consensus -- so without this the failure is
     * invisible to anything but stderr.
     *
     * @param context which part of the loop failed ({@code "event loop"}, {@code "timer task"},
     *                {@code "shutdown drain"})
     */
    default void eventLoopTaskFailed(final String context, final Throwable error) {
    }

    /** A proposer round reached an accept quorum and committed {@code ballot} for {@code key}. */
    default void roundCommitted(final HashedBytes key, final Ballot ballot) {
    }

    /** A proposer round for {@code key} failed after exhausting retries. */
    default void roundFailed(final HashedBytes key, final String reason) {
    }

    /**
     * A linearizable operation on {@code key} was refused up front because a previous round already
     * established that this node cannot reach a majority.
     * <p>
     * Distinct from {@link #roundFailed}: no round was run at all. A rising count here is the
     * signal that this node is on the minority side of a partition and is shedding work to
     * coordinators that are not.
     */
    default void roundRefusedNoMajority(final HashedBytes key) {
    }

    /**
     * A write was refused because this node has no room for the pair: storing it would take its key
     * and value bytes past its share of the heap.
     *
     * @param coordinator true when this node refused a client before running a round -- determinate,
     *                    nothing was proposed. False when it refused an accept as an acceptor, which
     *                    is the guard that actually keeps the JVM alive and leaves the round's
     *                    outcome unknown to its caller
     */
    default void writeRefusedNoCapacity(final HashedBytes key, final boolean coordinator) {
    }

    /**
     * This node's store crossed its capacity line, in either direction. Reported on the flip rather
     * than per refusal, for the same reason {@link #quorumAvailability} is: a node can sit on the
     * wrong side of it for as long as it takes somebody to delete something, and the rate of
     * refusals is a counter's job.
     */
    default void storeCapacity(final boolean available, final long storedBytes,
                               final long capacityBytes) {
    }

    /** A prepare was rejected by an acceptor already promised to {@code promised}. */
    default void prepareRejected(final Ballot promised) {
    }

    /** An accept was rejected by an acceptor already promised to {@code promised}. */
    default void acceptRejected(final Ballot promised) {
    }

    /** The proposer observed an external ballot counter higher than its own and advanced. */
    default void externalBallotObserved(final long counter) {
    }

    /** A serializable (local, no-round) read was served for {@code key}. */
    default void serializableRead(final HashedBytes key) {
    }

    /** The local acceptor's prepare handler threw for {@code key}. */
    default void prepareHandlerFailed(final HashedBytes key, final Throwable error) {
    }

    /** The local acceptor's accept handler threw for {@code key}. */
    default void acceptHandlerFailed(final HashedBytes key, final Throwable error) {
    }

    /** The scan handler threw. */
    default void scanFailed(final Throwable error) {
    }

    /** Answering a peer's ceiling request failed, or the request could not be sent to {@code peer}. */
    default void ceilingRequestFailed(final NodeId peer, final Throwable error) {
    }

    /**
     * This node's handshaked peers crossed the quorum line, in either direction -- the one change in
     * its picture of the cluster that cannot be read off the individual peer events without doing
     * the arithmetic. A node can sit on the wrong side of it for as long as the partition lasts, so
     * it is reported when it flips rather than left to a scrape.
     *
     * @param available  whether this node plus its handshaked peers can now form a quorum
     * @param handshaked how many peers are up, excluding this node
     */
    default void quorumAvailability(final boolean available, final int handshaked) {
    }

    /**
     * A member arrived with a different {@code incarnation_id} than the one it last presented: its
     * storage has been replaced. Not a refusal -- the member recovers its promise floor from a
     * quorum before it serves anything, which is what makes a deliberate disk replacement and a
     * wiped node the same, safe, event.
     */
    default void peerIncarnationChanged(final NodeId peer, final String previousIncarnation,
                                        final String currentIncarnation) {
    }

    /** A new anti-entropy repair cycle has started. */
    default void repairCycleStarted() {
    }

    /** A Paxos repair round was issued for a divergent {@code key}. */
    default void keyRepaired(final HashedBytes key) {
    }

    /**
     * A tombstone sweep finished -- one per {@code tombstoneSweepInterval}, whether or not it had
     * anything to collect. Everything an operator is told about collection comes from here.
     * <p>
     * Reported per sweep rather than per collection: a gauge that only moved when something was
     * collected would freeze at its last value in exactly the cluster that has stopped collecting.
     * A cluster that cannot collect is usually a cluster with a member down, which is an
     * operational condition and is reported as itself.
     */
    default void tombstoneSwept(final TombstoneSweep sweep) {
    }

    /**
     * A key was dropped for want of confirmation: this node could not account for its own history
     * (it took its promise floor from the cluster), the key sat at or below that floor, and no
     * member holds it.
     * <p>
     * Data leaving a node without anyone deciding it should is worth a line, even though the state
     * was never chosen -- it is the visible trace of a member that came back with less than it left
     * with, and of the repair that would otherwise have pushed it back out.
     *
     * @see LocalStore#dropUnaccountedFor
     */
    default void unaccountedKeyDropped(final HashedBytes key, final Ballot accepted) {
    }

    /** Serving a digest request from {@code peer} failed. */
    default void digestRequestFailed(final NodeId peer, final Throwable error) {
    }

    /** Serving a keys request from {@code peer} failed. */
    default void keysRequestFailed(final NodeId peer, final Throwable error) {
    }

    /** A membership reload was rejected (e.g. it would change the frozen cluster size N). */
    default void membersReloadRejected(final String reason) {
    }

    /**
     * A membership reload was accepted and applied; {@code members} is the size of the list now in
     * force (always the frozen {@code N}, since a list that changes it is what gets rejected).
     * <p>
     * The event that <b>ends</b> a rejection: without it, a refused member list would be a state
     * nothing can clear, still alerting long after the file was fixed.
     */
    default void membersReloadAccepted(final int members) {
    }

    /** A peer rejected (or we rejected) a PEER_HELLO handshake. */
    default void peerHandshakeRejected(final NodeId peer, final PeerHelloRespStatus status,
                                       final String cause) {
    }

    /**
     * {@code peer}'s PEER_HELLO handshake completed and the connection is usable for consensus
     * traffic.
     * <p>
     * <b>This is a handshake event, not a TCP one.</b> A socket reaching {@code finishConnect} means
     * nothing yet: the handshake that follows can still be rejected for a cluster-id, node-id,
     * cluster-size or replay failure, and a peer in that state can carry no protocol messages.
     * Only once it fires here does the peer count towards a quorum, which is what makes this the
     * signal a readiness probe is built on. When the mesh runs mTLS the TLS handshake completes
     * before PEER_HELLO, so this event also implies the channel is encrypted and mutually
     * authenticated.
     * <p>
     * Fires from both sides of the single-dialer model -- the dialer on receiving an {@code OK}
     * PEER_HELLO_RESP, the acceptor on authenticating an inbound PEER_HELLO -- so a node learns
     * about every handshaked peer regardless of which end placed the call.
     */
    default void peerHandshakeCompleted(final NodeId peer) {
    }

    /**
     * How far {@code peer}'s wall clock sits from ours, in milliseconds, as seen at the handshake
     * (positive: the peer is ahead of us). Includes the network leg, which on a peer mesh is
     * sub-millisecond next to any skew worth acting on.
     * <p>
     * The handshake's own five-minute bound is a replay guard, not clock discipline, so anything
     * inside it would otherwise go unreported -- including a mesh drifting steadily towards the
     * cliff. It matters because clients correct their lock leases against a coordinator's clock:
     * two clients agree only as far as the nodes they asked agree, so the spread across the mesh is
     * the accuracy ceiling of every lease in the system.
     * <p>
     * Reported by the accepting side of the single-dialer model, which is the side that reads a
     * PEER_HELLO timestamp -- one reading per pair, not two.
     */
    default void peerClockSkewObserved(final NodeId peer, final long skewMillis) {
    }

    /**
     * A previously handshaked {@code peer}'s connection went away.
     * <p>
     * Fires only for peers that had reached {@link #peerHandshakeCompleted}, so a dial that failed
     * or a handshake that was rejected produces no spurious disconnect. {@code reason} is a short
     * operator-facing description of what closed the connection.
     */
    default void peerDisconnected(final NodeId peer, final String reason) {
    }
}
