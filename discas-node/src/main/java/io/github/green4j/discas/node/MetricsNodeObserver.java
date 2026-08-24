/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.metrics.Counter;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.node.transport.PeerHelloRespStatus;

/**
 * Counts the node's events into a {@link MetricRegistry}, then forwards them.
 * <p>
 * Every counter is resolved once here in the constructor and held as a field, so a callback on the
 * event loop does an increment and nothing else -- no name lookup, no map probe, no allocation. That
 * matters because the callbacks this class cares most about ({@code roundCommitted},
 * {@code serializableRead}) fire on the request path of a store whose whole protocol is serialised
 * on one thread.
 * <p>
 * <b>Keys are not labels.</b> {@link NodeObserver#roundCommitted} and
 * {@link NodeObserver#keyRepaired} carry the key that was written, and attaching it would produce
 * one time series per key -- unbounded cardinality, the standard way to take down a metrics
 * backend. Rates are counted here; the key itself stays in the log record.
 */
public class MetricsNodeObserver extends DelegatingNodeObserver {

    /** One counter per {@link NodeState}, indexed by ordinal: transitions into that state. */
    private final Counter[] statesEntered;
    private final Counter requestsBeforeReady;
    private final Counter snapshotsStarted;
    private final Counter snapshotsCompleted;
    private final Counter snapshotsFailed;
    private final Counter walDegradations;
    private final Counter eventLoopTaskFailures;

    private final Counter roundsCommitted;
    private final Counter roundsFailed;
    private final Counter roundsRefusedNoMajority;
    /** Refusals for want of room, split by the two places they can happen. */
    private final Counter writesRefusedNoCapacity;
    private final Counter acceptsRefusedNoCapacity;
    private final Counter preparesRejected;
    private final Counter acceptsRejected;
    private final Counter externalBallotsObserved;
    private final Counter serializableReads;
    private final Counter prepareHandlerFailures;
    private final Counter acceptHandlerFailures;
    private final Counter scanFailures;

    private final Counter ceilingRequestFailures;
    private final Counter peerIncarnationChanges;

    private final Counter repairCycles;
    private final Counter keysRepaired;
    private final Counter tombstonesCollected;
    private final Counter unaccountedKeysDropped;
    private final Counter digestRequestFailures;
    private final Counter keysRequestFailures;

    private final Counter membersReloadsRejected;
    private final Counter membersReloadsAccepted;
    private final Counter peerHandshakesRejected;
    private final Counter peerHandshakesCompleted;
    private final Counter peerDisconnects;

    /**
     * Last observed clock skew per peer, in milliseconds. A family enumerated at scrape time rather
     * than a fixed set of gauges: the peer set is known at construction in principle, but this
     * observer is not told it, and a map written once per handshake is not on any hot path.
     * <p>
     * Bounded cardinality, unlike the key labels this class refuses: one series per member of a
     * cluster whose N is frozen.
     */
    private final Map<String, Long> peerClockSkewMillis = new ConcurrentHashMap<>();

    /**
     * The last sweep's readings, written on the event loop and read by a scrape on another thread.
     * Volatile rather than synchronised: each is a single value whose staleness is bounded by one
     * sweep interval, and no two of them have to agree with each other to be worth reading.
     */
    private volatile long tombstones;
    /** When the current run of blocked sweeps began, or 0 while nothing is waiting to be collected. */
    private volatile long blockedSinceNanos;
    /** Who stopped the last sweep, peer to answer -- replaced wholesale by each sweep. */
    private final Map<String, String> blockedBy = new ConcurrentHashMap<>();

    public MetricsNodeObserver(final MetricRegistry registry, final NodeObserver delegate) {
        super(delegate);
        // The start model, counted: discas_node_state_replaying_total, _awaiting_floor_total,
        // _serving_total, _closing_total, _failed_total. A node stuck out of the quorum shows up as
        // awaiting_floor rising without serving following it.
        final NodeState[] states = NodeState.values();
        statesEntered = new Counter[states.length];
        for (int i = 0; i < states.length; i++) {
            statesEntered[i] = registry.counter(
                    "discas_node_state_" + states[i].name().toLowerCase(Locale.ROOT)
                            + "_total",
                    "Transitions into " + states[i].name() + ".");
        }
        requestsBeforeReady = registry.counter("discas_node_requests_before_ready_total",
                "Requests refused because the node had not finished recovery.");
        snapshotsStarted = registry.counter("discas_node_snapshots_started_total",
                "Snapshot writes started.");
        snapshotsCompleted = registry.counter("discas_node_snapshots_completed_total",
                "Snapshot writes that completed.");
        snapshotsFailed = registry.counter("discas_node_snapshots_failed_total",
                "Snapshot writes that failed.");
        walDegradations = registry.counter("discas_node_wal_degradations_total",
                "Times the WAL was marked degraded.");
        eventLoopTaskFailures = registry.counter("discas_node_event_loop_task_failures_total",
                "Tasks that threw on the node event loop.");

        roundsCommitted = registry.counter("discas_node_rounds_committed_total",
                "CASPaxos rounds that reached a committing quorum.");
        roundsFailed = registry.counter("discas_node_rounds_failed_total",
                "CASPaxos rounds that failed or timed out.");
        // The refusal that runs no round at all: a rising rate here is this node shedding work to
        // coordinators that can still see a majority. It had neither a log line nor a counter, so
        // the only way to see it was /ready -- a boolean, on the node that already knows.
        roundsRefusedNoMajority = registry.counter("discas_node_rounds_refused_no_majority_total",
                "Linearizable operations refused up front because this node cannot reach a "
                        + "majority; no round was run.");

        // Two counters rather than one with a label, because they mean opposite things to a caller.
        // The first is a determinate refusal this node made before proposing anything; the second is
        // this node refusing an accept, which leaves the round's outcome unknown to whoever drove
        // it. A cluster where only the second moves has one member fuller than the rest.
        writesRefusedNoCapacity = registry.counter("discas_node_writes_refused_no_capacity_total",
                "Client writes this coordinator refused before running a round, for want of room.");
        acceptsRefusedNoCapacity = registry.counter("discas_node_accepts_refused_no_capacity_total",
                "Accepts this node refused for want of room; the guard that keeps the JVM alive.");
        preparesRejected = registry.counter("discas_node_prepares_rejected_total",
                "Prepare messages rejected because a higher ballot was already promised.");
        acceptsRejected = registry.counter("discas_node_accepts_rejected_total",
                "Accept messages rejected because a higher ballot was already promised.");
        externalBallotsObserved = registry.counter("discas_node_external_ballots_observed_total",
                "Times a peer's ballot counter exceeded ours and was adopted.");
        serializableReads = registry.counter("discas_node_serializable_reads_total",
                "Reads answered from local committed state with no consensus round.");
        prepareHandlerFailures = registry.counter("discas_node_prepare_handler_failures_total",
                "Prepare handlers that threw.");
        acceptHandlerFailures = registry.counter("discas_node_accept_handler_failures_total",
                "Accept handlers that threw.");
        // Local: the store or the ACL threw while a page was being filled. Never "no quorum" --
        // that judgement is the client's, and this node answers it by staying silent.
        scanFailures = registry.counter("discas_node_scan_failures_total",
                "Scan pages this node could not serve because serving one threw.");

        ceilingRequestFailures = registry.counter("discas_node_ceiling_request_failures_total",
                "Ceiling requests that could not be sent, or answers that could not be returned.");
        peerIncarnationChanges = registry.counter("discas_node_peer_incarnation_changes_total",
                "Members that reappeared with replaced storage; each recovers its own floor.");

        repairCycles = registry.counter("discas_node_repair_cycles_total",
                "Anti-entropy repair cycles started.");
        keysRepaired = registry.counter("discas_node_keys_repaired_total",
                "Keys reconciled by anti-entropy.");
        digestRequestFailures = registry.counter("discas_node_digest_request_failures_total",
                "Anti-entropy digest requests that failed.");
        keysRequestFailures = registry.counter("discas_node_keys_request_failures_total",
                "Anti-entropy key-page requests that failed.");

        membersReloadsRejected = registry.counter("discas_node_members_reloads_rejected_total",
                "Membership reloads refused, e.g. one that would change the frozen cluster size.");
        membersReloadsAccepted = registry.counter("discas_node_members_reloads_accepted_total",
                "Membership reloads applied; the pair to the refusals above, and what ends one.");
        peerHandshakesRejected = registry.counter("discas_node_peer_handshakes_rejected_total",
                "PEER_HELLO handshakes rejected by either side.");
        peerHandshakesCompleted = registry.counter("discas_node_peer_handshakes_completed_total",
                "PEER_HELLO handshakes that completed; a peer becoming usable for consensus.");
        peerDisconnects = registry.counter("discas_node_peer_disconnects_total",
                "Handshaked peers whose connection went away.");

        tombstonesCollected = registry.counter("discas_node_tombstones_collected_total",
                "Tombstones collected: every member agreed the value could no longer be resurrected.");
        unaccountedKeysDropped = registry.counter("discas_node_unaccounted_keys_dropped_total",
                "Keys dropped by a node that could not account for its own history and whose peers "
                        + "did not hold them.");

        // Collection, as three readings. The one worth waking somebody for is the first, and by its
        // trend against a key budget rather than by its level: a rising tombstone count is either a
        // block -- read the two below for whom to act on -- or a workload deleting faster than the
        // sweep interval collects, which is a capacity decision and not a fault.
        registry.gauge("discas_node_tombstones",
                "Keys held as tombstones: key space that cannot shrink until they are collected.",
                () -> tombstones);
        registry.gauge("discas_node_tombstone_collection_blocked_seconds",
                "How long tombstones have waited with none collected; zero when there are none. "
                        + "Alert at a day, never at a sweep.",
                () -> {
                    final long since = blockedSinceNanos;
                    return since == 0L ? 0L
                            : TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - since);
                });
        // One sample per blocking member, like the clock skew family below and for the same reason:
        // the action belongs to the member named here, not to collection. 'silent' is the
        // member-down row of FAILURE_MODES.md; 'retained' from an otherwise healthy member is a
        // replica that is merely behind, and anti-entropy ends that without an operator.
        registry.register(sink -> {
            for (final Map.Entry<String, String> entry : blockedBy.entrySet()) {
                sink.gauge("discas_node_tombstone_collection_blocked_by",
                        "Members that stopped the last tombstone sweep, and what they answered.",
                        1L, "peer", entry.getKey(), "answer", entry.getValue());
            }
        });

        // Enumerated at scrape time: one sample per peer this node has handshaked with, carrying
        // the last skew it saw. The spread across these is what bounds how well two clients can
        // agree about a lock lease, since each corrects against whichever coordinator it reached.
        registry.register(sink -> {
            for (final Map.Entry<String, Long> entry : peerClockSkewMillis.entrySet()) {
                sink.gauge("discas_node_peer_clock_skew_ms",
                        "Peer wall clock minus ours at the last handshake, in milliseconds.",
                        entry.getValue(), "peer", entry.getKey());
            }
        });
    }

    @Override
    public void nodeState(final NodeState from, final NodeState to,
                          final String detail, final Throwable cause) {
        // ordinal() as an array index, not as a code: statesEntered was built from
        // NodeState.values() above, so the two move together and nothing outside sees the number.
        statesEntered[to.ordinal()].increment();
        super.nodeState(from, to, detail, cause);
    }

    @Override
    public void requestBeforeReady(final boolean peerMessage) {
        requestsBeforeReady.increment();
        super.requestBeforeReady(peerMessage);
    }

    @Override
    public void snapshotStarted() {
        snapshotsStarted.increment();
        super.snapshotStarted();
    }

    @Override
    public void snapshotCompleted() {
        snapshotsCompleted.increment();
        super.snapshotCompleted();
    }

    @Override
    public void snapshotFailed(final Throwable error, final boolean committed) {
        snapshotsFailed.increment();
        super.snapshotFailed(error, committed);
    }

    @Override
    public void walDegraded(final String reason) {
        walDegradations.increment();
        super.walDegraded(reason);
    }

    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        eventLoopTaskFailures.increment();
        super.eventLoopTaskFailed(context, error);
    }

    @Override
    public void roundCommitted(final HashedBytes key, final Ballot ballot) {
        roundsCommitted.increment();
        super.roundCommitted(key, ballot);
    }

    @Override
    public void roundFailed(final HashedBytes key, final String reason) {
        roundsFailed.increment();
        super.roundFailed(key, reason);
    }

    @Override
    public void writeRefusedNoCapacity(final HashedBytes key, final boolean coordinator) {
        if (coordinator) {
            writesRefusedNoCapacity.increment();
        } else {
            acceptsRefusedNoCapacity.increment();
        }
        super.writeRefusedNoCapacity(key, coordinator);
    }

    // storeCapacity is deliberately not counted here. It fires on the flip, so a gauge fed from it
    // would sit at whatever it last saw -- frozen in exactly the cluster it exists for, which is the
    // mistake tombstoneSwept was reshaped to avoid. The flip is a condition, and it is reported as
    // one through the operator surface; the counters above are its rate.

    @Override
    public void roundRefusedNoMajority(final HashedBytes key) {
        roundsRefusedNoMajority.increment();
        super.roundRefusedNoMajority(key);
    }

    @Override
    public void prepareRejected(final Ballot promised) {
        preparesRejected.increment();
        super.prepareRejected(promised);
    }

    @Override
    public void acceptRejected(final Ballot promised) {
        acceptsRejected.increment();
        super.acceptRejected(promised);
    }

    @Override
    public void externalBallotObserved(final long counter) {
        externalBallotsObserved.increment();
        super.externalBallotObserved(counter);
    }

    @Override
    public void serializableRead(final HashedBytes key) {
        serializableReads.increment();
        super.serializableRead(key);
    }

    @Override
    public void prepareHandlerFailed(final HashedBytes key, final Throwable error) {
        prepareHandlerFailures.increment();
        super.prepareHandlerFailed(key, error);
    }

    @Override
    public void acceptHandlerFailed(final HashedBytes key, final Throwable error) {
        acceptHandlerFailures.increment();
        super.acceptHandlerFailed(key, error);
    }

    @Override
    public void scanFailed(final Throwable error) {
        scanFailures.increment();
        super.scanFailed(error);
    }

    @Override
    public void ceilingRequestFailed(final NodeId peer, final Throwable error) {
        ceilingRequestFailures.increment();
        super.ceilingRequestFailed(peer, error);
    }

    @Override
    public void peerIncarnationChanged(final NodeId peer, final String previousIncarnation,
                                       final String currentIncarnation) {
        peerIncarnationChanges.increment();
        super.peerIncarnationChanged(peer, previousIncarnation, currentIncarnation);
    }

    @Override
    public void repairCycleStarted() {
        repairCycles.increment();
        super.repairCycleStarted();
    }

    @Override
    public void keyRepaired(final HashedBytes key) {
        keysRepaired.increment();
        super.keyRepaired(key);
    }

    @Override
    public void digestRequestFailed(final NodeId peer, final Throwable error) {
        digestRequestFailures.increment();
        super.digestRequestFailed(peer, error);
    }

    @Override
    public void keysRequestFailed(final NodeId peer, final Throwable error) {
        keysRequestFailures.increment();
        super.keysRequestFailed(peer, error);
    }

    @Override
    public void membersReloadRejected(final String reason) {
        membersReloadsRejected.increment();
        super.membersReloadRejected(reason);
    }

    @Override
    public void membersReloadAccepted(final int members) {
        membersReloadsAccepted.increment();
        super.membersReloadAccepted(members);
    }

    @Override
    public void peerHandshakeRejected(final NodeId peer, final PeerHelloRespStatus status,
                                      final String cause) {
        peerHandshakesRejected.increment();
        super.peerHandshakeRejected(peer, status, cause);
    }

    @Override
    public void peerHandshakeCompleted(final NodeId peer) {
        peerHandshakesCompleted.increment();
        super.peerHandshakeCompleted(peer);
    }

    /**
     * Every collection series moves here, including on a sweep that had nothing to do -- which is
     * the point: a cluster that has stopped collecting is one whose readings would otherwise stop
     * moving at exactly the moment they start mattering.
     */
    @Override
    public void tombstoneSwept(final TombstoneSweep sweep) {
        tombstones = sweep.tombstones();
        if (sweep.collected()) {
            tombstonesCollected.increment();
        }
        if (sweep.blocked()) {
            if (blockedSinceNanos == 0L) {
                blockedSinceNanos = System.nanoTime();
            }
        } else {
            // Collected, or nothing was old enough to collect. Either way nothing is waiting.
            blockedSinceNanos = 0L;
        }
        blockedBy.clear();
        for (int i = 0; i < sweep.blockers().size(); i++) {
            final TombstoneSweep.Blocker blocker = sweep.blockers().get(i);
            blockedBy.put(blocker.peer().value(), blocker.label());
        }
        super.tombstoneSwept(sweep);
    }

    @Override
    public void unaccountedKeyDropped(final HashedBytes key, final Ballot accepted) {
        unaccountedKeysDropped.increment();
        super.unaccountedKeyDropped(key, accepted);
    }

    @Override
    public void peerClockSkewObserved(final NodeId peer, final long skewMillis) {
        peerClockSkewMillis.put(peer.value(), skewMillis);
        super.peerClockSkewObserved(peer, skewMillis);
    }

    @Override
    public void peerDisconnected(final NodeId peer, final String reason) {
        peerDisconnects.increment();
        super.peerDisconnected(peer, reason);
    }
}
