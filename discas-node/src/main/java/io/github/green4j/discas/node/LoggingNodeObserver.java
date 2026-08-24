/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.logging.LogThrottle;
import io.github.green4j.discas.common.operator.OperatorAttention;
import io.github.green4j.discas.common.operator.OperatorState;
import io.github.green4j.discas.node.transport.PeerHelloRespStatus;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Writes the node's operator-facing events to a {@link Log}, then forwards them.
 * <p>
 * Two kinds of line come out of here, and the difference is the whole design.
 * <b>Lifecycle facts</b> are written directly: recovery finishing, a peer arriving or leaving, a
 * configuration accepted. They are {@code info}, they are a record rather than a signal, and nobody
 * is expected to do anything about one.
 * <b>Everything that needs an operator to act</b> goes through {@link OperatorAttention} instead of
 * being logged here -- which is what puts the action on the line, the same identifier on a metric
 * sample, and one documented row, all from a single name. There is no third path: an
 * {@code error} this class writes itself would be a line with no action.
 * <p>
 * Everything not in either category is a rate, and belongs in {@link MetricsNodeObserver} where it
 * can be aggregated rather than grepped -- logging {@code roundCommitted} would emit one line per
 * write and drown the very records this exists to surface.
 */
public class LoggingNodeObserver extends DelegatingNodeObserver {

    /**
     * How long collection has to have been stuck before it is worth a line.
     * <p>
     * Not a sweep and not an alert. A sweep that collects nothing is the ordinary case -- a member
     * restarting blocks a few and nobody needs to read about it -- so a line per blocked sweep would
     * teach an operator to skip these. Long enough that only a member that is actually gone produces
     * one, short enough that whoever reads the log after an incident can see when it started.
     */
    private static final long BLOCKED_LOG_AFTER_NANOS = TimeUnit.MINUTES
            .toNanos(10);

    /** Skew worth acting on; below this it is ordinary and reporting it would only be noise. */
    private static final long NOTABLE_CLOCK_SKEW_MS = 1_000L;

    /**
     * What a handshake rejection means for the operator, per status: one callback, ten statuses,
     * eight distinct actions. A {@code null} is a status that needs no intervention.
     */
    private static final Map<PeerHelloRespStatus, OperatorState> HANDSHAKE_STATES =
            new EnumMap<>(PeerHelloRespStatus.class);

    static {
        HANDSHAKE_STATES.put(PeerHelloRespStatus.PROTOCOL_MISMATCH,
                OperatorState.VERSION_MISMATCH);
        HANDSHAKE_STATES.put(PeerHelloRespStatus.CLUSTER_MISMATCH,
                OperatorState.FOREIGN_CLUSTER);
        HANDSHAKE_STATES.put(PeerHelloRespStatus.IDENTITY_MISMATCH,
                OperatorState.IDENTITY_MISMATCH);
        HANDSHAKE_STATES.put(PeerHelloRespStatus.UNKNOWN_PEER,
                OperatorState.UNKNOWN_MEMBER);
        HANDSHAKE_STATES.put(PeerHelloRespStatus.HELLO_TIMESTAMP_SKEW,
                OperatorState.CLOCK_UNUSABLE);
        HANDSHAKE_STATES.put(PeerHelloRespStatus.CLUSTER_SIZE_MISMATCH,
                OperatorState.CLUSTER_SIZE_MISMATCH);
        HANDSHAKE_STATES.put(PeerHelloRespStatus.INCARNATION_DUPLICATED,
                OperatorState.STORAGE_CLONED);
        HANDSHAKE_STATES.put(PeerHelloRespStatus.CEILING_ROLLED_BACK,
                OperatorState.PEER_STORAGE_ROLLED_BACK);
        // NOT_REPLAYED is transient and self-clearing: the dialer's backoff retries and replay ends
        // it, so it fires on every rolling restart and there is nothing to do about one.
        // INCARNATION_CHANGED is retired and never sent.
    }

    private final Log log;
    private final OperatorAttention attention;
    private final long blockedAfterNanos;

    /**
     * The three sites here whose rate is set by something other than a person.
     * <p>
     * A peer arriving and leaving is one line each and perfectly quiet until a link flaps, at which
     * point it is two lines per cycle for as long as it lasts -- and a mesh of N members multiplies
     * that. A refused handshake is per dial attempt, so a member that will not be admitted retries
     * for as long as it is left running. Each gets its own throttle rather than sharing one: a peer
     * flapping must not be able to hide a different peer being refused for a reason somebody has to
     * act on.
     */
    private final LogThrottle peerUpThrottle = new LogThrottle();
    private final LogThrottle peerDownThrottle = new LogThrottle();
    private final LogThrottle handshakeThrottle = new LogThrottle();

    /** Loop-confined, like every other callback: the sweeps that arrive here are serialised. */
    private long collectionBlockedSinceNanos;
    private boolean collectionBlockReported;

    public LoggingNodeObserver(final Log log, final OperatorAttention attention,
                               final NodeObserver delegate) {
        this(log, attention, delegate, BLOCKED_LOG_AFTER_NANOS);
    }

    /**
     * Package-private: the blocked threshold is injectable so a test can drive both collection lines
     * without waiting out the real one, and there is nothing an operator would set it from.
     */
    LoggingNodeObserver(final Log log, final OperatorAttention attention,
                        final NodeObserver delegate, final long blockedAfterNanos) {
        super(delegate);
        this.log = log;
        this.attention = attention;
        this.blockedAfterNanos = blockedAfterNanos;
    }

    /**
     * Two lines and no more: one when collection has been stuck for a while, naming what is waiting
     * and who is holding it up, and one when it starts again. The counts behind them are a scrape's
     * job ({@link MetricsNodeObserver}); what a log can add is <em>when</em> it changed and
     * <em>which member</em> to look at.
     * <p>
     * <b>Blocked collection raises no operator state</b>, and that is deliberate rather than an
     * omission: every cause of it already has one, on the node that owns the action. A {@code
     * silent} blocker is that member's {@link OperatorState#PEER_DOWN}; a member answering {@code
     * retained} because its log is degraded is {@link OperatorState#WAL_DEGRADED} <em>on that
     * member</em>; a member merely behind needs nothing, because anti-entropy ends it. A state here
     * would be a second name for conditions that are already named, raised on the node that cannot
     * act on them.
     */
    @Override
    public void tombstoneSwept(final TombstoneSweep sweep) {
        if (sweep.blocked()) {
            final long now = System.nanoTime();
            if (collectionBlockedSinceNanos == 0L) {
                collectionBlockedSinceNanos = now;
            } else if (!collectionBlockReported
                    && now - collectionBlockedSinceNanos >= blockedAfterNanos) {
                collectionBlockReported = true;
                log.info("Tombstone collection blocked for "
                        + TimeUnit.NANOSECONDS.toMinutes(now - collectionBlockedSinceNanos)
                        + "m: tombstones=" + sweep.tombstones()
                        + " blocked by " + sweep.blockers());
            }
            super.tombstoneSwept(sweep);
            return;
        }
        if (collectionBlockReported) {
            log.info("Tombstone collection resumed after "
                    + TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - collectionBlockedSinceNanos)
                    + "m: tombstones=" + sweep.tombstones());
        }
        collectionBlockedSinceNanos = 0L;
        collectionBlockReported = false;
        super.tombstoneSwept(sweep);
    }

    /**
     * One line per transition, and the only startup narrative there is:
     * {@code REPLAYING -> SERVING (keys=1240)}. A repeated state is the same state saying where it
     * has got to -- replay progress, or a floor still short of witnesses.
     * <p>
     * <b>Info, not error.</b> Every state here is a normal one for a node to be in, including the
     * ones it can sit in for minutes. Two are not left at that. {@link NodeState#FAILED} arrives
     * with a cause and is storage the node cannot interpret. {@link NodeState#AWAITING_FLOOR} is
     * ordinary for a few seconds and a stuck cluster after two minutes, which is exactly the shape
     * {@link OperatorState#normalFor()} exists for -- so it is raised on entry and cleared on the way
     * out, and says nothing at all in the ordinary case.
     */
    @Override
    public void nodeState(final NodeState from, final NodeState to,
                          final String detail, final Throwable cause) {
        final String line = (from == to ? "node " + to : "node " + from + " -> " + to)
                + (detail == null ? "" : " (" + detail + ")");
        if (cause != null) {
            // Branching on the type, never on the reason text beside it. A node that failed because
            // its log does not fit is not a node with damaged storage, and telling its operator to
            // delete what is under --wal-dir would destroy an intact log to fix a heap setting.
            attention.raise(cause instanceof StoreCapacityExceededException
                            ? OperatorState.STORE_FULL
                            : OperatorState.STORAGE_UNREADABLE,
                    null, line, cause);
        } else {
            log.info(line);
        }
        if (to == NodeState.AWAITING_FLOOR) {
            attention.raise(OperatorState.FLOOR_UNAVAILABLE, null, line);
        } else if (from == NodeState.AWAITING_FLOOR) {
            attention.clear(OperatorState.FLOOR_UNAVAILABLE, null);
        }
        super.nodeState(from, to, detail, cause);
    }

    @Override
    public void snapshotStarted() {
        log.info("Snapshot started");
        super.snapshotStarted();
    }

    @Override
    public void snapshotCompleted() {
        log.info("Snapshot committed");
        attention.clear(OperatorState.SNAPSHOT_FAILING, null);
        super.snapshotCompleted();
    }

    @Override
    public void snapshotFailed(final Throwable error, final boolean committed) {
        attention.raise(OperatorState.SNAPSHOT_FAILING, null,
                "snapshot write failed, committed=" + committed, error);
        super.snapshotFailed(error, committed);
    }

    @Override
    public void walDegraded(final String reason) {
        attention.raise(OperatorState.WAL_DEGRADED, null, "the WAL cannot append: " + reason);
        super.walDegraded(reason);
    }

    @Override
    public void lifecycleResourceCloseFailed(final Throwable error) {
        attention.raise(OperatorState.SHUTDOWN_INCOMPLETE, null,
                "a lifecycle resource threw while closing", error);
        super.lifecycleResourceCloseFailed(error);
    }

    /**
     * The three sites where the loop catches an exception and carries on report one state between
     * them, scoped by where it threw. They have one action -- collect the trace and report it -- and
     * a state per throwing site would be enumeration in place of a rule, owing a new documented row
     * every time somebody adds a handler.
     */
    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        attention.raise(OperatorState.UNHANDLED_ERROR, context, context + " threw", error);
        super.eventLoopTaskFailed(context, error);
    }

    @Override
    public void prepareHandlerFailed(final HashedBytes key, final Throwable error) {
        attention.raise(OperatorState.UNHANDLED_ERROR, "prepare handler",
                "the acceptor's prepare handler threw", error);
        super.prepareHandlerFailed(key, error);
    }

    @Override
    public void acceptHandlerFailed(final HashedBytes key, final Throwable error) {
        attention.raise(OperatorState.UNHANDLED_ERROR, "accept handler",
                "the acceptor's accept handler threw", error);
        super.acceptHandlerFailed(key, error);
    }

    /**
     * Scanning is local: this fires when the store or the ACL threw, never for a quorum that did not
     * answer -- that judgement is the client's and it is told by silence. So it is a defect like the
     * two above, not a peer condition.
     */
    @Override
    public void scanFailed(final Throwable error) {
        attention.raise(OperatorState.UNHANDLED_ERROR, "scan handler",
                "serving a scan page threw", error);
        super.scanFailed(error);
    }

    /**
     * The node deleted state, and this is the only signal that it did. It cannot be un-happened and
     * nothing in this process can say it has been dealt with, so the state stays raised until a
     * restart -- which is the acknowledgement.
     */
    @Override
    public void unaccountedKeyDropped(final HashedBytes key, final Ballot accepted) {
        attention.raise(OperatorState.UNACCOUNTED_KEYS_DROPPED, null,
                "dropped a key this node could not account for and no member holds"
                        + " (first was " + key + " at ballot " + accepted + ")");
        super.unaccountedKeyDropped(key, accepted);
    }

    /**
     * Raised while this node is refusing writes for want of room, cleared when it has room again --
     * which is what a delete does. The per-refusal rate stays a counter: an operator needs to know
     * that it started and that it stopped, not that it happened again.
     */
    @Override
    public void storeCapacity(final boolean available, final long storedBytes,
                              final long capacityBytes) {
        if (available) {
            attention.clear(OperatorState.STORE_FULL, null);
        } else {
            attention.raise(OperatorState.STORE_FULL, null,
                    "the store is estimated at " + storedBytes + " of " + capacityBytes
                            + " permitted heap bytes and is refusing to grow");
        }
        super.storeCapacity(available, storedBytes, capacityBytes);
    }

    @Override
    public void membersReloadRejected(final String reason) {
        attention.raise(OperatorState.MEMBERS_REJECTED, null,
                "the reloaded member list was refused: " + reason);
        super.membersReloadRejected(reason);
    }

    @Override
    public void membersReloadAccepted(final int members) {
        log.info("Members reloaded: " + members + " member(s)");
        attention.clear(OperatorState.MEMBERS_REJECTED, null);
        super.membersReloadAccepted(members);
    }

    @Override
    public void peerHandshakeRejected(final NodeId peer, final PeerHelloRespStatus status,
                                      final String cause) {
        final String detail = "handshake with " + peer.value() + " refused, status=" + status
                + (cause == null || cause.isEmpty() ? "" : ", cause=" + cause);
        final OperatorState state = HANDSHAKE_STATES.get(status);
        if (state == null) {
            handshakeThrottle.info(log, detail + "; transient, the dialer retries");
        } else {
            attention.raise(state, peer.value(), detail);
        }
        super.peerHandshakeRejected(peer, status, cause);
    }

    /**
     * The other half of what an operator watches during a start: not this node's state, but its
     * picture of the cluster. Three lines make it up -- a peer arriving, a peer leaving, and a peer
     * arriving as a different incarnation of itself.
     */
    @Override
    public void peerIncarnationChanged(final NodeId peer, final String previousIncarnation,
                                       final String currentIncarnation) {
        log.info("Peer " + peer.value() + " has new storage: incarnation " + previousIncarnation
                + " -> " + currentIncarnation + "; it recovers its own promise floor before serving");
        super.peerIncarnationChanged(peer, previousIncarnation, currentIncarnation);
    }

    @Override
    public void quorumAvailability(final boolean available, final int handshaked) {
        if (available) {
            log.info("Quorum available: " + handshaked + " peer(s) up");
            attention.clear(OperatorState.QUORUM_LOST, null);
        } else {
            attention.raise(OperatorState.QUORUM_LOST, null,
                    "only " + handshaked + " peer(s) up; no linearizable operation can complete"
                            + " at this coordinator");
        }
        super.quorumAvailability(available, handshaked);
    }

    /**
     * A completed handshake is the one event that ends every reason a peer was not usable, so it
     * clears them all rather than the one that happened to be raised. The set comes from
     * the per-status table, so the two cannot drift apart.
     */
    @Override
    public void peerHandshakeCompleted(final NodeId peer) {
        peerUpThrottle.info(log, "peer up: " + peer.value());
        attention.clear(OperatorState.PEER_DOWN, peer.value());
        for (final OperatorState state : HANDSHAKE_STATES.values()) {
            attention.clear(state, peer.value());
        }
        super.peerHandshakeCompleted(peer);
    }

    /**
     * Raised only past the notable-skew threshold, because every handshake reports a reading and
     * a mesh under NTP reports single-digit milliseconds. Cleared by a reading back under the bound,
     * which is what a fixed clock produces at the next handshake.
     */
    @Override
    public void peerClockSkewObserved(final NodeId peer, final long skewMillis) {
        if (Math.abs(skewMillis) >= NOTABLE_CLOCK_SKEW_MS) {
            attention.raise(OperatorState.CLOCK_SKEW, peer.value(),
                    "clock differs from ours by " + skewMillis + "ms");
        } else {
            attention.clear(OperatorState.CLOCK_SKEW, peer.value());
        }
        super.peerClockSkewObserved(peer, skewMillis);
    }

    @Override
    public void peerDisconnected(final NodeId peer, final String reason) {
        peerDownThrottle.info(log, "peer down: " + peer.value() + " (" + reason + ")");
        attention.raise(OperatorState.PEER_DOWN, peer.value(),
                "the handshaked connection went away: " + reason);
        super.peerDisconnected(peer, reason);
    }
}
