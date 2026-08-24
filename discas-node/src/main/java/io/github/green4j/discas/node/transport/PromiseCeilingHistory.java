/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.identity.NodeId;

import java.util.HashMap;
import java.util.Map;

/**
 * The highest promise ceiling each member has claimed, per run of its storage -- and the one
 * question that makes it worth remembering: <b>has this storage come back holding less than it left
 * with?</b>
 *
 * <p>It has to be an invariant rather than a check because a member cannot verify it about itself: a
 * volume restored from a copy is byte-continuous, internally consistent, and older, so its own
 * replay reports success and its {@code incarnation} marker -- a file <em>inside</em> the directory
 * -- was copied along with it. What one node cannot see about itself, though, two nodes can compare:
 * a ceiling only ever rises within a run, so <b>the same incarnation reporting a lower ceiling than
 * it did before is that invariant broken</b>, stated as something a handshake can test.
 *
 * <p><b>A detector, not a repair.</b> It fires on a connection, which is before that member votes
 * but after nothing else: state that already spread was re-decided by ordinary rounds and cannot be
 * rolled back. What follows a rejection is an operator procedure, not a recovery this class
 * performs.
 *
 * <p><b>It narrows the window rather than closing it</b>, because this memory is per process: a
 * rollback that coincides with a restart of every peer that remembered the old ceiling passes
 * unseen. Persisting it would turn a member-to-ceiling map into replicated membership state --
 * durable, needing its own agreement, and wrong in a new way whenever it disagreed with a member.
 *
 * <p>Confined to the event loop, like the rest of the transport's state.
 */
final class PromiseCeilingHistory {

    /** No rollback: what {@link #rolledBackFrom} returns when the claim is admissible. */
    static final long NO_ROLLBACK = -1L;

    /**
     * Whether a claim says anything at all. Both non-claims are negative
     * ({@code PeerTransport.PROMISE_CEILING_REPLAYING}, {@code PROMISE_CEILING_UNPROVEN}) and a
     * real ceiling never is, so one test covers both -- what differs between them is whether the
     * handshake is refused, which is the transport's decision and not this class's.
     */
    private static boolean isClaim(final long ceiling) {
        return ceiling >= 0;
    }

    private final Map<NodeId, Seen> byMember = new HashMap<>();

    /**
     * Whether {@code claimed} is lower than this member has already proved under the same
     * incarnation.
     *
     * <p>A member arriving with a <b>different</b> incarnation is not a rollback and never can be:
     * its storage was replaced, it has no promises to have lost, and it takes its floor from the
     * cluster before it serves anything. That is the case this check must not catch, and the reason
     * the history is keyed by incarnation rather than by member alone -- a legitimately wiped member
     * starts a fresh record rather than being measured against the ceiling of a disk it no longer
     * has.
     *
     * @return {@link #NO_ROLLBACK} when the claim is admissible; otherwise the higher ceiling this
     *         member proved earlier under this same incarnation, which is the whole diagnosis
     */
    long rolledBackFrom(final NodeId peer, final IncarnationId incarnation, final long claimed) {
        if (!isClaim(claimed)) {
            // Nothing was claimed, so nothing is contradicted. Saying "no rollback" is the only
            // honest answer here; whether such a peer is admitted at all is the transport's call.
            return NO_ROLLBACK;
        }
        final Seen previous = byMember.get(peer);
        if (previous == null || !previous.incarnation.equals(incarnation)) {
            return NO_ROLLBACK;
        }
        return claimed < previous.highestCeiling ? previous.highestCeiling : NO_ROLLBACK;
    }

    /**
     * Remember this claim, having admitted it. Keeps the highest under the current incarnation, and
     * starts over when the incarnation changes -- there is nothing to carry across a disk that was
     * replaced.
     */
    void record(final NodeId peer, final IncarnationId incarnation, final long claimed) {
        if (!isClaim(claimed)) {
            // A node that cannot prove its ceiling is not evidence about it either.
            return;
        }
        final Seen previous = byMember.get(peer);
        if (previous == null || !previous.incarnation.equals(incarnation)) {
            byMember.put(peer, new Seen(incarnation, claimed));
            return;
        }
        if (claimed > previous.highestCeiling) {
            previous.highestCeiling = claimed;
        }
    }

    /** Forget a member entirely -- used when it leaves the member list. */
    void forget(final NodeId peer) {
        byMember.remove(peer);
    }

    private static final class Seen {
        private final IncarnationId incarnation;
        private long highestCeiling;

        private Seen(final IncarnationId incarnation, final long highestCeiling) {
            this.incarnation = incarnation;
            this.highestCeiling = highestCeiling;
        }
    }
}
