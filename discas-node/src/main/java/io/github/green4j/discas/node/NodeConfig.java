/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;

import java.time.Duration;

/**
 * The transport-agnostic identity, quorum basis and timing a CAS node needs regardless of how peers
 * are wired: this node's {@link NodeId}, its {@link ClusterId}, the cluster size {@code N}, and
 * every deadline the consensus path and node lifecycle run on. Passed to
 * {@code DisCasNodeFactory} alongside a transport-specific bootstrap.
 * <p>
 * Every deadline is the deployer's to set, not a constant: {@link #roundTimeout()} caps a single
 * consensus round, so a cross-region cluster that raised only the client's per-attempt timeout
 * would still have its nodes give up early.
 * <p>
 * Peer <em>reconnect</em> backoff is not here -- it belongs to the transport, alongside the other
 * TCP tunables.
 * <p>
 * Identity is required and has no default; every timing field does have one, declared inline on
 * the corresponding {@link Builder} field -- that line is the single source of truth for both the
 * value and its documentation.
 */
public final class NodeConfig {

    /** This node's identity. Must appear in the member list the transport is built with. */
    public final NodeId nodeId;
    /** The cluster this node belongs to; a peer claiming a different one is rejected at HELLO. */
    public final ClusterId clusterId;
    /**
     * N, the quorum basis. Frozen for the node's lifetime and carried in the peer handshake, so
     * two members that disagree about it refuse to connect rather than forming quorums of
     * different sizes over the same keys.
     */
    public final int clusterSize;

    private final Duration roundTimeout;
    private final int maxRoundRetries;
    private final Duration proposalExpiry;
    private final Duration roundRetryBackoffBase;
    private final Duration roundRetryBackoffJitter;
    private final Duration noQuorumBackoff;
    private final Duration peerResponseTimeout;
    private final Duration tombstoneSweepInterval;
    private final Duration ceilingRecoveryRetryCap;
    private final Duration repairInterval;
    private final Duration shutdownAwaitTimeout;
    private final Duration snapshotInterval;
    private final Duration promiseEvictionInterval;
    private final Duration walForceInterval;
    private final double storeHeapFraction;

    /**
     * Identity with every timing default in place. Equivalent to
     * {@code builder().nodeId(..).clusterId(..).clusterSize(..).build()}.
     *
     * @throws IllegalArgumentException if an id is missing or {@code clusterSize} is outside [1, 255].
     */
    public NodeConfig(final NodeId nodeId,
                      final ClusterId clusterId,
                      final int clusterSize) {
        this(builder().nodeId(nodeId).clusterId(clusterId).clusterSize(clusterSize));
    }

    private NodeConfig(final Builder b) {
        if (b.nodeId == null) {
            throw new IllegalArgumentException("nodeId required");
        }
        if (b.clusterId == null) {
            throw new IllegalArgumentException("clusterId required");
        }
        if (b.clusterSize < 1 || b.clusterSize > 255) {
            throw new IllegalArgumentException("clusterSize must be in [1, 255], got " + b.clusterSize);
        }
        this.nodeId = b.nodeId;
        this.clusterId = b.clusterId;
        this.clusterSize = b.clusterSize;
        this.roundTimeout = b.roundTimeout;
        this.maxRoundRetries = b.maxRoundRetries;
        this.proposalExpiry = b.proposalExpiry;
        this.noQuorumBackoff = b.noQuorumBackoff;
        this.roundRetryBackoffBase = b.roundRetryBackoffBase;
        this.roundRetryBackoffJitter = b.roundRetryBackoffJitter;
        this.peerResponseTimeout = b.peerResponseTimeout;
        this.tombstoneSweepInterval = b.tombstoneSweepInterval;
        this.ceilingRecoveryRetryCap = b.ceilingRecoveryRetryCap;
        this.repairInterval = b.repairInterval;
        this.shutdownAwaitTimeout = b.shutdownAwaitTimeout;
        this.snapshotInterval = b.snapshotInterval;
        this.promiseEvictionInterval = b.promiseEvictionInterval;
        this.walForceInterval = b.walForceInterval;
        if (b.storeHeapFraction <= 0.0 || b.storeHeapFraction > 1.0) {
            throw new IllegalArgumentException(
                    "storeHeapFraction must be in (0, 1], got " + b.storeHeapFraction);
        }
        this.storeHeapFraction = b.storeHeapFraction;
    }

    /**
     * A builder carrying every timing default; set the identity and override what the deployment
     * needs. Also the way to read a default without inventing an identity -- {@code builder()}
     * alone is a valid source of every default value, which is how the CLI renders its help text.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** How long one consensus round may run before it is failed or retried (default 5s). */
    public Duration roundTimeout() {
        return roundTimeout;
    }

    /** How many times a failed round is retried before the caller sees the failure (default 3). */
    public int maxRoundRetries() {
        return maxRoundRetries;
    }

    /**
     * How long the coordinator may keep driving one write before abandoning it, measured from the
     * first attempt and <b>not</b> reset by retries (default 30s).
     * <p>
     * This is what gives an indeterminate answer an end. A client that gives up on a coordinator
     * does not stop that coordinator: it goes on retrying, and each retry restarts
     * {@link #roundTimeout()}, so without a bound the honest statement to a caller is "this write
     * may still apply at some point". With a bound it becomes "within {@code proposalExpiry} of
     * when you sent it, and never after" -- which is a statement a caller can act on. The check
     * sits between prepare and the Accept broadcast, so an expired write is determinate:
     * {@code PROPOSAL_EXPIRED} means nothing was proposed.
     * <p>
     * Whichever runs out first wins, so a value below
     * {@code roundTimeout x (maxRoundRetries + 1)} silently shortens the retry chain instead of
     * bounding it. Raise it, not lower it, when rounds are legitimately slow; the default leaves
     * headroom over the default chain.
     */
    public Duration proposalExpiry() {
        return proposalExpiry;
    }

    /**
     * How long a round that could not reach a majority keeps this node refusing linearizable work
     * before it optimistically tries again (default 2s).
     * <p>
     * The expiry is what stops a refusal becoming permanent. A node that refuses every request runs
     * no rounds, so it learns nothing new and would go on refusing long after the partition healed.
     * Letting the verdict lapse costs one slow request per window and buys automatic recovery.
     */
    public Duration noQuorumBackoff() {
        return noQuorumBackoff;
    }

    /** Base of the exponential backoff between round retries (default 50ms). */
    public Duration roundRetryBackoffBase() {
        return roundRetryBackoffBase;
    }

    /** Width of the random jitter added to each round retry backoff (default 50ms). */
    public Duration roundRetryBackoffJitter() {
        return roundRetryBackoffJitter;
    }

    /**
     * How long a background exchange waits for the peers it asked before deciding on what came back
     * (default 10s). One number for the two subsystems that ask everybody and then act on whoever
     * replied: anti-entropy's digest and key-page exchanges, and a tombstone sweep's purge checks.
     * <p>
     * <b>Not a failure detector</b>, unlike every timeout on the write path. Expiry does not fail
     * anything: anti-entropy repairs what the answers it has justify, and a sweep that is missing an
     * answer is blocked, which is the outcome a cluster with a member down is supposed to have. So
     * it costs nothing to be generous with, and shortening it buys nothing.
     * <p>
     * One asymmetry between its two users is worth knowing, because the symptom misleads: answering
     * a purge check with {@code HELD} means forcing the log first, so that exchange carries an
     * {@code fsync} on every member and an anti-entropy page does not. The default clears a healthy
     * device by four orders of magnitude, but a device slow enough to miss it would show up as
     * {@code blocked_by{answer="silent"}} from a member that is perfectly up -- which reads like the
     * member-down row and is not. Raising this is the answer if that is ever seen.
     */
    public Duration peerResponseTimeout() {
        return peerResponseTimeout;
    }

    /**
     * How long after one tombstone sweep is decided the next one starts (default 10s).
     * <p>
     * With one candidate per sweep, this <em>is</em> this node's collection rate: about 8,600 keys a
     * day at the default. A workload deleting faster than that grows the key space with no fault
     * anywhere, which is a capacity decision -- lower the interval, or accept the growth and watch
     * it. Scheduled after each decision rather than on a fixed repeat, so a cluster that cannot
     * collect asks again this often and no faster.
     */
    public Duration tombstoneSweepInterval() {
        return tombstoneSweepInterval;
    }

    /**
     * The longest a node that started without state waits between ceiling requests (default 1s).
     * <p>
     * A cap rather than a fixed interval: the first retries come far sooner and double up to this,
     * because the first attempt of a cold start is nearly always wasted on peers that have not
     * finished starting, and paying the full interval for that race would slow every cold start.
     * <p>
     * A cap rather than a deadline: there is nothing this node may do without a floor, so running out
     * of patience is not one of its options. It asks again for as long as it takes, which is the same
     * shape as every other "waiting for the cluster" state here -- no failure detector, no timeout
     * that turns "not answered yet" into a verdict.
     */
    public Duration ceilingRecoveryRetryCap() {
        return ceilingRecoveryRetryCap;
    }

    /** How often anti-entropy runs a full repair cycle (default 10m). */
    public Duration repairInterval() {
        return repairInterval;
    }

    /** How long {@code close()} waits for the event loop to drain (default 5s). */
    public Duration shutdownAwaitTimeout() {
        return shutdownAwaitTimeout;
    }

    /** How often the node takes a snapshot (default 1h). */
    public Duration snapshotInterval() {
        return snapshotInterval;
    }

    /** How often expired promises are evicted from the local store (default 5m). */
    public Duration promiseEvictionInterval() {
        return promiseEvictionInterval;
    }

    /** How often buffered WAL writes are forced to disk (default 1s). */
    public Duration walForceInterval() {
        return walForceInterval;
    }

    /**
     * The share of the JVM's maximum heap this node's store may be estimated to occupy before it
     * refuses to take more. Default 0.8.
     * <p>
     * <b>An estimate of the footprint, not of the payload and not a measurement.</b> Each key is
     * charged its own bytes plus {@code LocalStore.ENTRY_FOOTPRINT_BYTES} for the objects around it,
     * rounded up on purpose -- on small values those cost several times the pair. Measuring the
     * live heap instead would be worse rather than better: it moves with the collector, so the same
     * write would be refused before a GC and taken after one, and two replicas would disagree about
     * the same pair. This estimate is a function of what is stored, so every replica computes the
     * same answer for the same key.
     * <p>
     * <b>What is outside it:</b> WAL buffers, the snapshot writer, network buffers and in-flight
     * requests. The share left over has to cover those, which is the reason to lower this rather
     * than raise it.
     * <p>
     * A delete is never refused against this budget -- a store with no way to shrink has no way back.
     */
    public double storeHeapFraction() {
        return storeHeapFraction;
    }

    /** The budget in bytes: {@link #storeHeapFraction()} of this JVM's maximum heap. */
    public long storeCapacityBytes() {
        return (long) (Runtime.getRuntime().maxMemory() * storeHeapFraction);
    }

    /**
     * Fluent builder. Each field's initialiser below is the default and its documentation; the
     * setters validate individually, and {@link #build()} validates the identity that has no
     * default.
     */
    public static final class Builder {

        private NodeId nodeId;
        private ClusterId clusterId;
        private int clusterSize;

        // How long one consensus round gets before it is failed or retried. Bounds a single
        // prepare/accept attempt, independent of the client's own per-attempt timeout -- whichever
        // is shorter is what the caller actually experiences.
        private Duration roundTimeout = Duration.ofSeconds(5);

        // Retries of a failed round before the failure reaches the caller. Zero disables retrying.
        private int maxRoundRetries = 3;

        // How long one write may be driven in total, retries included, before the coordinator
        // abandons it. Bounds the whole operation where roundTimeout bounds one attempt, which is
        // what turns "may still be applied" into a statement with an end. 30s leaves headroom over
        // the default chain (4 attempts x 5s plus backoff, ~20.4s) so it bounds rather than truncates.
        private Duration proposalExpiry = Duration.ofSeconds(30);

        // Exponential backoff between round retries: base << attempt, plus a random jitter up to
        // the width below. Short, because a retried round is latency the caller is already feeling.
        private Duration roundRetryBackoffBase = Duration.ofMillis(50);
        private Duration roundRetryBackoffJitter = Duration.ofMillis(50);

        // How long a "cannot see a majority" verdict stands before the node optimistically serves
        // again. Short: the cost of being wrong is one slow request, while the cost of holding the
        // verdict too long is refusing work the node could do. Well under a round timeout, so a
        // healed partition is picked up within the latency of a single operation.
        private Duration noQuorumBackoff = Duration.ofSeconds(2);

        // How long an anti-entropy exchange or a tombstone sweep waits for the peers it asked.
        // Generous relative to a round: both are background work with no caller waiting, and giving
        // up early defers a repair or a collection rather than failing anything.
        private Duration peerResponseTimeout = Duration.ofSeconds(10);

        // How long after a tombstone sweep is decided the next one starts, and with one candidate
        // per sweep, the rate at which this node collects: ~8,600 keys a day. Background work with
        // no caller waiting on it, so it is paced for a cluster that is mostly not collecting.
        private Duration tombstoneSweepInterval = Duration.ofSeconds(10);

        // How long a tombstone must have been left alone before it is swept. Long enough that
        // traffic and a repair cycle (10m) have settled, because a check on a key that is about to
        // be rewritten collects nothing; not a safety margin, so nothing rests on its length.

        // The longest a node that started without state waits between ceiling requests; the first
        // ones are far sooner. Short even so: it is holding up its own startup, and answers cost one
        // message each.
        private Duration ceilingRecoveryRetryCap = Duration.ofSeconds(1);

        // How often a full anti-entropy repair cycle starts. Cycles are sequential -- the next one
        // is scheduled this long after the previous one completes, never overlapping it.
        private Duration repairInterval = Duration.ofMinutes(10);

        // How long close() waits for the event loop to terminate and in-flight work to drain.
        private Duration shutdownAwaitTimeout = Duration.ofSeconds(5);

        // How often the node snapshots its state, bounding WAL replay at recovery.
        private Duration snapshotInterval = Duration.ofHours(1);

        // How often expired promises are swept out of the local store.
        private Duration promiseEvictionInterval = Duration.ofMinutes(5);

        // How often buffered WAL writes are forced. This is the durability/throughput dial: discas
        // rests durability on quorum between forces rather than an fsync per write.
        private Duration walForceInterval = Duration.ofSeconds(1);

        // The share of max heap the store's estimated footprint may take before this node refuses
        // to grow. The remaining fifth is what WAL buffers, the snapshot writer and in-flight
        // requests have to fit in, which is what makes this a ceiling rather than a target.
        private double storeHeapFraction = 0.8;

        private Builder() {
        }

        /** @see NodeConfig#nodeId */
        public Builder nodeId(final NodeId value) {
            this.nodeId = value;
            return this;
        }

        /** @see NodeConfig#clusterId */
        public Builder clusterId(final ClusterId value) {
            this.clusterId = value;
            return this;
        }

        /** @see NodeConfig#clusterSize */
        public Builder clusterSize(final int value) {
            this.clusterSize = value;
            return this;
        }

        /** @see NodeConfig#roundTimeout() */
        public Builder roundTimeout(final Duration value) {
            this.roundTimeout = requirePositive(value, "roundTimeout");
            return this;
        }

        /** @see NodeConfig#maxRoundRetries() */
        public Builder maxRoundRetries(final int value) {
            if (value < 0) {
                throw new IllegalArgumentException("maxRoundRetries must be >= 0, was " + value);
            }
            this.maxRoundRetries = value;
            return this;
        }

        /** @see NodeConfig#proposalExpiry() */
        public Builder proposalExpiry(final Duration value) {
            this.proposalExpiry = requirePositive(value, "proposalExpiry");
            return this;
        }

        /** @see NodeConfig#proposalExpiry() */
        public Duration proposalExpiry() {
            return proposalExpiry;
        }

        /** @see NodeConfig#noQuorumBackoff() */
        public Builder noQuorumBackoff(final Duration value) {
            this.noQuorumBackoff = requirePositive(value, "noQuorumBackoff");
            return this;
        }

        /** @see NodeConfig#noQuorumBackoff() */
        public Duration noQuorumBackoff() {
            return noQuorumBackoff;
        }

        /** @see NodeConfig#roundRetryBackoffBase() */
        public Builder roundRetryBackoffBase(final Duration value) {
            this.roundRetryBackoffBase = requirePositive(value, "roundRetryBackoffBase");
            return this;
        }

        /** @see NodeConfig#roundRetryBackoffJitter() */
        public Builder roundRetryBackoffJitter(final Duration value) {
            this.roundRetryBackoffJitter = requirePositive(value, "roundRetryBackoffJitter");
            return this;
        }

        /** @see NodeConfig#peerResponseTimeout() */
        public Builder peerResponseTimeout(final Duration value) {
            this.peerResponseTimeout = requirePositive(value, "peerResponseTimeout");
            return this;
        }

        /** @see NodeConfig#tombstoneSweepInterval() */
        public Builder tombstoneSweepInterval(final Duration value) {
            this.tombstoneSweepInterval = requirePositive(value, "tombstoneSweepInterval");
            return this;
        }

        /** @see NodeConfig#tombstoneSweepInterval() */
        public Duration tombstoneSweepInterval() {
            return tombstoneSweepInterval;
        }

        /** @see NodeConfig#ceilingRecoveryRetryCap() */
        public Builder ceilingRecoveryRetryCap(final Duration value) {
            this.ceilingRecoveryRetryCap = requirePositive(value, "ceilingRecoveryRetryCap");
            return this;
        }

        /** @see NodeConfig#ceilingRecoveryRetryCap() */
        public Duration ceilingRecoveryRetryCap() {
            return ceilingRecoveryRetryCap;
        }

        /** @see NodeConfig#repairInterval() */
        public Builder repairInterval(final Duration value) {
            this.repairInterval = requirePositive(value, "repairInterval");
            return this;
        }

        /** @see NodeConfig#shutdownAwaitTimeout() */
        public Builder shutdownAwaitTimeout(final Duration value) {
            this.shutdownAwaitTimeout = requirePositive(value, "shutdownAwaitTimeout");
            return this;
        }

        /** @see NodeConfig#snapshotInterval() */
        public Builder snapshotInterval(final Duration value) {
            this.snapshotInterval = requirePositive(value, "snapshotInterval");
            return this;
        }

        /** @see NodeConfig#promiseEvictionInterval() */
        public Builder promiseEvictionInterval(final Duration value) {
            this.promiseEvictionInterval = requirePositive(value, "promiseEvictionInterval");
            return this;
        }

        /** @see NodeConfig#walForceInterval() */
        public Builder walForceInterval(final Duration value) {
            this.walForceInterval = requirePositive(value, "walForceInterval");
            return this;
        }

        /** @see NodeConfig#storeHeapFraction() */
        public Builder storeHeapFraction(final double value) {
            this.storeHeapFraction = value;
            return this;
        }

        /** @see NodeConfig#storeHeapFraction() */
        public double storeHeapFraction() {
            return storeHeapFraction;
        }

        /** @see NodeConfig#roundTimeout() */
        public Duration roundTimeout() {
            return roundTimeout;
        }

        /** @see NodeConfig#maxRoundRetries() */
        public int maxRoundRetries() {
            return maxRoundRetries;
        }

        /** @see NodeConfig#roundRetryBackoffBase() */
        public Duration roundRetryBackoffBase() {
            return roundRetryBackoffBase;
        }

        /** @see NodeConfig#roundRetryBackoffJitter() */
        public Duration roundRetryBackoffJitter() {
            return roundRetryBackoffJitter;
        }

        /** @see NodeConfig#peerResponseTimeout() */
        public Duration peerResponseTimeout() {
            return peerResponseTimeout;
        }

        /** @see NodeConfig#repairInterval() */
        public Duration repairInterval() {
            return repairInterval;
        }

        /** @see NodeConfig#shutdownAwaitTimeout() */
        public Duration shutdownAwaitTimeout() {
            return shutdownAwaitTimeout;
        }

        /** @see NodeConfig#snapshotInterval() */
        public Duration snapshotInterval() {
            return snapshotInterval;
        }

        /** @see NodeConfig#promiseEvictionInterval() */
        public Duration promiseEvictionInterval() {
            return promiseEvictionInterval;
        }

        /** @see NodeConfig#walForceInterval() */
        public Duration walForceInterval() {
            return walForceInterval;
        }

        /**
         * Validates the identity fields, which have no defaults, and returns an immutable config
         * that shares no state with this builder, so the builder may be reused.
         *
         * @throws IllegalArgumentException if an id is missing or {@code clusterSize} is outside [1, 255].
         */
        public NodeConfig build() {
            return new NodeConfig(this);
        }

        private static Duration requirePositive(final Duration value, final String name) {
            if (value == null) {
                throw new IllegalArgumentException(name + " is required");
            }
            if (value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be > 0, was " + value);
            }
            return value;
        }
    }
}
