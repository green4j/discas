/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

/**
 * Where a node is between process start and process end. One model, and the only one: readiness,
 * what traffic is answered, what the probes report and what the log says are all derived from it.
 *
 * <pre>
 *   REPLAYING --has state----------------------&gt; SERVING --&gt; CLOSING
 *      |                                          ^
 *      +--none--&gt; AWAITING_FLOOR --floor adopted--+
 *                    ^     |
 *                    +-----+ keeps asking
 *
 *   any state --&gt; FAILED   (nothing continues from here; the loop shuts down)
 * </pre>
 *
 * <p>The single distinction that makes {@link #AWAITING_FLOOR} a state rather than a detail of
 * startup: a node in it is <em>running and reachable but not a member of the quorum</em>. It answers
 * the one question that can move it forward and refuses everything else, because it does not yet
 * know what it may have promised in a previous life.
 */
public enum NodeState {

    /** Reading its own snapshot and WAL tail. Answers nothing; peers see silence, clients NOT_READY. */
    REPLAYING,

    /**
     * Replayed no durable state, so it has no promise floor of its own and is asking the cluster for
     * one. Answers ceiling requests -- its own replayed bounds are already final -- and nothing else.
     * A node that replayed state never enters this.
     */
    AWAITING_FLOOR,

    /** Has a floor and serves: consensus, client traffic, anti-entropy, snapshots. */
    SERVING,

    /** Shutting down. Pending work is drained and refused; nothing new is accepted. */
    CLOSING,

    /**
     * Stopped by something it cannot continue past -- a replay that threw, or a snapshot that failed
     * after committing. The event loop is shut down; only a restart leaves this state.
     */
    FAILED
}
