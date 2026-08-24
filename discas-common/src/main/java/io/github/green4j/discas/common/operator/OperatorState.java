/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.operator;

import java.time.Duration;

/**
 * Every state of a node or an agent that needs an operator to act, named once. The log line, the
 * metric sample and the documented row are all derived from the name, so they cannot drift apart.
 * <p>
 * <b>A state with no action is not here</b>, and every {@code error} line comes from here -- so
 * every {@code error} line names an action. A bad thing nobody can act on is a rate, and rates live
 * in the metric registry. One enum covers both node and agent: they share the taxonomy, and the
 * reload states outright.
 * <p>
 * <b>{@link #normalFor} is where transience is handled</b>, which is why there is no warning level.
 * A peer that goes away during a rolling restart raises {@link #PEER_DOWN} and clears it inside the
 * window, having written nothing; a peer that is gone for good becomes due, and its action is to
 * bring it back. A third level would mean "act, but not much", which no alert rule can use, and it
 * would put the judgement in the tone of the line rather than in the process -- which is the thing
 * that knows how long the condition has been true.
 *
 * @see OperatorAttention
 */
public enum OperatorState {


    /**
     * State that is present and contradictory: a corrupt WAL segment, or an unreadable marker. The
     * node enters {@code FAILED} and the loop shuts down.
     */
    STORAGE_UNREADABLE(OperatorGroup.STORAGE,
            "Delete what is broken under --wal-dir and restart -- the log, or the whole directory. "
                    + "Never bring this node back from a copy of that directory: a byte-continuous "
                    + "but older copy replays as a clean log and is the one damage nothing here can "
                    + "see. Keep it for forensics; a member with damaged storage joins empty."),

    /**
     * Snapshot writes are failing. Not fatal -- the WAL is the record and snapshots are an
     * optimisation over replaying it -- but replay time grows without bound until one succeeds.
     */
    SNAPSHOT_FAILING(OperatorGroup.STORAGE,
            "Check free space and permissions on --wal-dir. The node keeps serving from the WAL; "
                    + "what grows meanwhile is how long its next start takes. Rate: "
                    + "discas_node_snapshots_failed_total against _completed_total."),

    /**
     * The WAL has entered its unrecoverable degraded state. Appends refuse, the node NACKs
     * consensus, and {@code /health} answers 503.
     * <p>
     * Latches: nothing short of a restart can clear it, which is also the fix.
     */
    WAL_DEGRADED(OperatorGroup.STORAGE,
            "Clear the device under --wal-dir and restart this node; /health already answers 503. "
                    + "It refuses appends rather than accepting what it cannot log, and it answers "
                    + "'retained' to every purge check -- so the whole cluster stops collecting "
                    + "tombstones until this member is back (discas_node_wal_degradations_total)."),

    /** A lifecycle-bound resource threw while closing, so the process did not shut down cleanly. */
    SHUTDOWN_INCOMPLETE(OperatorGroup.STORAGE,
            "Confirm the process has exited and nothing still holds --wal-dir before starting it "
                    + "again. If shutdown is being cut short, --shutdown-await-timeout-ms is the "
                    + "budget it was given."),

    /**
     * The node dropped keys it could not account for: it took its promise floor from the cluster,
     * these keys sat at or below that floor, and no member holds them.
     * <p>
     * Data leaving a node without anyone deciding it should. The state was never chosen, so nothing
     * was lost that a reader could have seen -- but it is the visible trace of a member that came
     * back with less than it left with, and there is no second signal.
     */
    UNACCOUNTED_KEYS_DROPPED(OperatorGroup.STORAGE,
            "This member came back holding state nobody could confirm, and it has been dropped "
                    + "rather than repaired outward. Establish what happened to its storage; if it "
                    + "was restored from a copy, follow the procedure in FAILURE_MODES.md. How many "
                    + "went: discas_node_unaccounted_keys_dropped_total."),

    /** A member arrived presenting another member's {@code incarnation_id}: a copied directory. */
    STORAGE_CLONED(OperatorGroup.STORAGE,
            "One member's --wal-dir has been copied onto another. Stop the clone and start it empty "
                    + "-- it arrives holding the original's promises and reserved ballot range and "
                    + "asserts them as its own. Anti-entropy is how a replacement gets state."),

    /**
     * A member returned with the same {@code incarnation_id} and a lower promise ceiling than it
     * already proved: the same storage came back holding less than it left with.
     */
    PEER_STORAGE_ROLLED_BACK(OperatorGroup.STORAGE,
            "Stop that member now -- it is not behind, it is authoritatively pushing stale state, "
                    + "and every minute it runs spreads more of it. Keep its directory for forensics "
                    + "and start it empty. Do not lift the refusal; see FAILURE_MODES.md."),


    /**
     * The node has started without durable state and cannot get a promise floor: not enough members
     * have answered it. It stays out of the quorum and serves nothing until they do.
     */
    FLOOR_UNAVAILABLE(OperatorGroup.MEMBERSHIP, Duration.ofMinutes(2),
            "Bring members back: this node needs enough of them to bound the promises it has "
                    + "forgotten. /health is 200 and /ready is 503 meanwhile, which is correct -- it "
                    + "is running and must not be routed to. If a quorum's worth lost state at once "
                    + "the bound exists nowhere and nothing can derive it: read STARTING_A_NODE.md "
                    + "before clearing any directory."),

    /** A membership reload was refused; the node kept the previous member list. */
    MEMBERS_REJECTED(OperatorGroup.MEMBERSHIP,
            "Fix --members-file: it must define exactly --cluster-size members and include this "
                    + "node. The node is running on the last list it accepted, so what is on disk "
                    + "and what is in force now disagree about who the members are."),

    /** Two ends run builds whose peer protocol versions are incompatible. */
    VERSION_MISMATCH(OperatorGroup.MEMBERSHIP,
            "The two ends run incompatible builds. Finish the rollout or roll it back; they cannot "
                    + "carry protocol messages to each other meanwhile."),

    /** A peer presented a different cluster id: it belongs to another cluster. */
    FOREIGN_CLUSTER(OperatorGroup.MEMBERSHIP,
            "That node belongs to another cluster. Check --cluster-id on both ends and the address "
                    + "for it in --members-file: one of the two is pointed at the wrong deployment."),

    /**
     * The member list and the identity a peer presents disagree about who it is. The rejection's
     * cause says which of the four situations it was.
     */
    IDENTITY_MISMATCH(OperatorGroup.MEMBERSHIP,
            "The member list and the peer's identity disagree. Read the cause: 'cert-san' means the "
                    + "certificate in --tls-keystore names a different node or cluster, "
                    + "'expected-peer' means the address in --members-file belongs to a different "
                    + "node, 'self-claim' means two members were started with the same --node-id."),

    /** A peer handshaked that is not in this node's member list. */
    UNKNOWN_MEMBER(OperatorGroup.MEMBERSHIP,
            "That node is not in --members-file here. Add it if it belongs, and stop it if it does "
                    + "not -- it will keep dialling until one of the two is true."),

    /** Two members disagree about the frozen cluster size {@code N}. */
    CLUSTER_SIZE_MISMATCH(OperatorGroup.MEMBERSHIP,
            "Make --cluster-size agree on both ends; it is frozen for a node's lifetime, so this "
                    + "needs a restart of whichever one is wrong. Never resize N to make a quorum "
                    + "appear: a mixed-N cluster forms two quorums over one key, which this refusal "
                    + "exists to prevent."),

    /** A server reported a cluster size that cannot be true (zero, or beyond the supported bound). */
    CLUSTER_SIZE_INVALID(OperatorGroup.MEMBERSHIP,
            "That node reports a cluster size this client cannot work with. Check --cluster-size "
                    + "where it is running; until it is fixed the client cannot judge a quorum "
                    + "against it (discas_client_invalid_cluster_sizes_total)."),


    /**
     * This node's handshaked peers no longer reach the quorum line. Every linearizable operation
     * fails at this coordinator; serializable reads are still answered.
     */
    QUORUM_LOST(OperatorGroup.PEER,
            "Restore members until a majority is reachable from here; "
                    + "discas_node_peer_handshaked says which are missing and /ready is already 503. "
                    + "If this node is the minority side of a partition the guard is working and "
                    + "the majority is unaffected -- check the other side before acting. Never "
                    + "resize --cluster-size to fix quorum. Work shed meanwhile: "
                    + "discas_node_rounds_refused_no_majority_total."),

    /**
     * A member that had completed the peer handshake went away. One member down at N=3 leaves the
     * cluster writable, which is why the window is a minute rather than nothing: a rolling restart
     * must not page anyone.
     */
    PEER_DOWN(OperatorGroup.PEER, Duration.ofMinutes(1),
            "Bring that member back or replace it (discas_node_peer_handshaked, and its own "
                    + "/health). Quorum may well be intact, but running short is not a steady "
                    + "state: the next member to go takes the cluster's writes with it, and "
                    + "tombstone collection has already stopped cluster-wide."),

    /** A client or agent lost its connection to a node and has not got it back. */
    NODE_UNREACHABLE(OperatorGroup.PEER, Duration.ofMinutes(1),
            "Check that node and the path to it. Requests fail over to the others meanwhile "
                    + "(discas_client_requests_failed_over_total), so this is visible here before "
                    + "it is visible to any caller."),


    /**
     * A peer's wall clock differs from ours by enough to matter. Not a consensus concern -- ballots
     * are logical -- but clients express lock leases in cluster time taken from these clocks, so the
     * spread across the mesh is the accuracy ceiling of every lease in the system.
     */
    CLOCK_SKEW(OperatorGroup.CONFIG,
            "Run NTP on both nodes; discas_node_peer_clock_skew_ms carries the reading per peer. "
                    + "Clients correct their lock leases against these clocks, so the spread across "
                    + "the mesh is how far apart two clients can be about when a lease expires. "
                    + "Past five minutes the peer is refused outright."),

    /** A peer's clock is beyond the handshake's replay bound, so it cannot connect at all. */
    CLOCK_UNUSABLE(OperatorGroup.CONFIG,
            "Fix NTP on that node: its clock is more than five minutes from ours, which the "
                    + "handshake's replay guard refuses. It cannot join, and so does not appear in "
                    + "discas_node_peer_clock_skew_ms at all, until the clock is corrected."),

    /** A node refused this client's or agent's credentials. */
    ACCESS_DENIED(OperatorGroup.CONFIG,
            "This process's credentials are not accepted by that node. Check the token or client "
                    + "certificate it presents against the node's --client-auth mode, "
                    + "--client-token-file/--client-token-dir and --client-acl-file."),

    /**
     * A hot-reloaded file did not parse; the last good version stays in force. Not transient -- a
     * file caught mid-write is retried silently, so reaching here means what is on disk is malformed
     * and will stay malformed until somebody edits it.
     */
    RELOAD_FAILED(OperatorGroup.CONFIG,
            "Fix the file. It does not parse, so the process is running on the last version it "
                    + "accepted and what is on disk is no longer what is in force."),

    /**
     * The filesystem watch on a hot-reloaded file is gone and the safety poll is carrying it. Still
     * working, and slower: an edit now takes up to a poll interval to be seen instead of arriving as
     * an event.
     */
    RELOAD_NOT_WATCHED(OperatorGroup.CONFIG,
            "Check the host's filesystem watch limits (inotify on Linux). Reloads still happen on "
                    + "the safety poll, so nothing is broken -- an edit simply takes up to a poll "
                    + "interval to take effect instead of arriving as an event."),

    /** Loaded TLS material is approaching its expiry. */
    MATERIAL_EXPIRING(OperatorGroup.CONFIG,
            "Rotate this material before it expires; discas_reload_material_expires_seconds counts "
                    + "down. Writing the new files over --tls-keystore/--tls-truststore is enough, "
                    + "hot reload picks them up with no restart. Once it has expired the handshakes "
                    + "it protects begin to fail and members drop out of the quorum."),


    /**
     * The store's estimated heap footprint has reached this node's configured share.
     * <p>
     * Two shapes, one condition. A <b>serving</b> node refuses writes that would grow it, and
     * deletes still work, which is the way out. A node that hit it <b>during replay</b> does not
     * start at all -- there is no caller to refuse while recovering state it already owns, so it
     * stops rather than being killed by the JVM halfway through, and only more room helps.
     */
    STORE_FULL(OperatorGroup.CAPACITY,
            "Give this node more heap, or raise --store-heap-fraction, and restart it; if it is "
                    + "still serving, deleting keys works too. Do not delete anything under "
                    + "--wal-dir: the log is intact, it is the heap that is too small for it. Every "
                    + "replica holds the same keys, so a full node is a full cluster and the others "
                    + "are about to say the same. --store-heap-fraction is the share of max heap "
                    + "the store's "
                    + "estimated footprint may take; the rest has to cover WAL buffers, snapshots "
                    + "and in-flight requests. Rate: "
                    + "discas_node_writes_refused_no_capacity_total (refused before a "
                    + "round, nothing was written) and _accepts_refused_no_capacity_total (refused "
                    + "mid-round, the caller was told the outcome is unknown)."),


    /**
     * An exception the event loop caught and carried on from. One state for every such site,
     * scoped by the component that threw, since they all have the same action.
     */
    UNHANDLED_ERROR(OperatorGroup.INTERNAL,
            "Collect the stack trace and report it. The loop caught this and carried on, so the "
                    + "process is still running and the defect is not going to announce itself "
                    + "again -- only the first occurrence per component is logged.");

    private final OperatorGroup group;
    private final Duration normalFor;
    private final String action;

    OperatorState(final OperatorGroup group, final String action) {
        this(group, Duration.ZERO, action);
    }

    OperatorState(final OperatorGroup group, final Duration normalFor, final String action) {
        this.group = group;
        this.normalFor = normalFor;
        this.action = action;
    }

    /** Which part of the system the state belongs to. */
    public OperatorGroup group() {
        return group;
    }

    /**
     * How long this condition may persist before it means anything. Below the window a raised state
     * is neither logged nor exposed as a metric sample; at the window it becomes <em>due</em>, which
     * is the moment the line is written and the sample appears.
     * <p>
     * Zero for most states. It is non-zero exactly where the same condition is both an ordinary
     * moment in a healthy cluster and, if it lasts, a fault -- a restarting member, a node still
     * looking for a floor.
     */
    public Duration normalFor() {
        return normalFor;
    }

    /**
     * What the operator does about it, in the imperative. Written once here and printed on the log
     * line, so the line and the documented row cannot drift apart.
     */
    public String action() {
        return action;
    }
}
