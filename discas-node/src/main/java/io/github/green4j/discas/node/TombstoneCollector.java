/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerTransport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Decides one tombstone's collection: asks every member whether it can still resurrect the value the
 * tombstone suppresses, and broadcasts the purge when none of them can.
 *
 * <p>The rule is one sentence: <b>a tombstone may be collected only when no replica can still
 * resurrect the value it suppresses.</b> A quorum is the wrong answer here, and it is the one place
 * in this store where it is: the danger is precisely the replica <em>outside</em> the quorum, which
 * still holds the old value and would have anti-entropy copy it back once the tombstone is gone. So
 * the condition is every member, and a member that does not answer blocks the collection exactly
 * like one that refuses -- which is why a late answer and a missing one need no policy to tell
 * apart.
 *
 * <p><b>Coordinator here means what it means for a client request: a role for one sweep of one
 * key</b>, taken by whichever node swept the candidate. No election, no cluster-wide sweeper, and
 * two nodes deciding the same purge is not a case to prevent -- the decision is a function of the
 * key and the tombstone's ballot, and applying it is idempotent in that ballot
 * ({@link LocalStore#purge}). A partition cannot produce two decisions either, because only a node
 * that reaches every member can decide at all. That is where requiring all {@code N} stops being
 * expensive and starts being a simplification: there is nothing to fence and nothing to reconcile.
 *
 * <p><b>Nothing here is durable, and the decision is neither acknowledged nor retried.</b> A
 * half-applied purge re-decides by itself: the members that applied it answer
 * {@link PurgeAnswer#ABSENT}, the ones that missed it answer {@link PurgeAnswer#HELD}, and both
 * permit. A coordinator that restarts mid-sweep loses only the questions in flight.
 *
 * <p>Which node sweeps which key is a cost question and not a correctness one, and belongs to the
 * sweeper. This class decides one candidate it is handed and knows nothing about where it came
 * from.
 */
final class TombstoneCollector {

    /**
     * How one sweep ended, and -- when it did not collect -- who stopped it.
     * <p>
     * There is no third case: a sweep either collects the tombstone or leaves it exactly where it
     * was, and nothing about the attempt is remembered afterwards. The blockers travel with the
     * outcome rather than through a separate report, because they are the only thing a sweep learns
     * that nobody else can: who was asked and what they said.
     */
    public static final class Outcome {
        private static final Outcome COLLECTED = new Outcome(true, List.of());

        private final boolean collected;
        private final List<TombstoneSweep.Blocker> blockers;

        private Outcome(final boolean collected, final List<TombstoneSweep.Blocker> blockers) {
            this.collected = collected;
            this.blockers = blockers;
        }

        boolean collected() {
            return collected;
        }

        /** Empty when this sweep collected; otherwise every member that did not permit. */
        List<TombstoneSweep.Blocker> blockers() {
            return blockers;
        }

        @Override
        public String toString() {
            return collected ? "collected" : "blocked by " + blockers;
        }
    }

    private final NodeId nodeId;
    private final LocalStore store;
    private final PeerTransport transport;
    private final EventLoop loop;
    private final CorrelationIdGenerator correlationIdGenerator;
    private final Duration checkTimeout;

    /** The sweep in flight, or {@code null} when idle. One at a time, by construction. */
    private Sweep sweep;
    private boolean stopped;

    /**
     * @param checkTimeout how long one sweep waits for the members it asked. Not a failure detector:
     *                     when it expires the sweep is simply blocked, which is the outcome a
     *                     healthy cluster with a member down is supposed to have.
     */
    TombstoneCollector(final NodeId nodeId,
                              final LocalStore store,
                              final PeerTransport transport,
                              final EventLoop loop,
                              final CorrelationIdGenerator correlationIdGenerator,
                              final Duration checkTimeout) {
        this.nodeId = nodeId;
        this.store = store;
        this.transport = transport;
        this.loop = loop;
        this.correlationIdGenerator = correlationIdGenerator;
        this.checkTimeout = checkTimeout;
    }

    /**
     * Sweep one candidate: ask every member, and purge when every one of them permits.
     *
     * @param tombstoneBallot the ballot this node holds the tombstone at. The question, the decision
     *                        and what each member applies are all about that state and no other, so
     *                        a key written again above it is left alone everywhere.
     * @param onDecided       run once, on the event loop, when the sweep ends either way -- which is
     *                        how the sweeper knows it may pick the next candidate. Not run if the
     *                        node closes first: a node on its way down decides nothing.
     * @return whether the sweep started. {@code false} means one is already running or this
     *         collector is closed, and {@code onDecided} will never run.
     */
    public boolean collect(final HashedBytes key, final Ballot tombstoneBallot,
                           final Consumer<Outcome> onDecided) {
        if (stopped || sweep != null) {
            return false;
        }
        // One membership snapshot, read once and used for the whole decision. A sweep that asked one
        // member list and counted another is the one way this can be wrong with no fault at all
        // (TOMBSTONE_GC.md section 6), and the transport's list is live -- a membership reload
        // rewrites it in place.
        //
        // That this list plus self is every member -- which is the whole condition -- is the
        // transport's invariant, not one checked here: both implementations refuse a member list
        // that does not define exactly clusterSize() nodes including this one, at construction and
        // again on every reload. So counting these identities counts all N, and re-checking it
        // here would be a branch that cannot be taken.
        final Sweep current = new Sweep(correlationIdGenerator.next(), key, tombstoneBallot,
                List.copyOf(transport.peers()), onDecided);
        sweep = current;

        // Armed before anything is sent, as anti-entropy arms its own: a send that throws must not
        // leave a sweep with no timer, which is a collector that never sweeps again.
        current.deadline = loop.schedule(checkTimeout, () -> {
            if (sweep == current) {
                decide();
            }
        });

        // This node is a member and answers as one, off its own storage. HELD is a claim about a
        // disk wherever it is made, and this disk is not exempt -- purgeCheck forces before it says so.
        current.answers.put(nodeId, store.purgeCheck(key, tombstoneBallot));

        final PeerMessage.PurgeCheckReq request =
                new PeerMessage.PurgeCheckReq(nodeId, current.correlationId, key, tombstoneBallot);
        for (int i = 0; i < current.peers.size(); i++) {
            try {
                transport.send(current.peers.get(i), request);
            } catch (final Exception ignored) {
                // Per-peer, so one unreachable member does not stop the others being asked -- and
                // nothing more is needed, because silence already blocks: this member simply has no
                // answer when the sweep is decided, and is reported as the one holding it up.
            }
        }
        // For N=1 nothing was sent and this settles it immediately; otherwise it is the ordinary
        // path for a sweep whose answers all arrive before it returns (there are none today, and a
        // transport that delivered inline would have them).
        decideIfPermitted();
        return true;
    }

    /**
     * One member's answer to the sweep in flight. Answers to any other sweep are dropped: they
     * belong to one that has already been decided -- at its deadline, or in a previous incarnation
     * of this node, whose correlation ids come from a different epoch.
     */
    void onCheckResp(final PeerMessage.PurgeCheckResp response) {
        if (sweep == null || response.correlationId() != sweep.correlationId) {
            return;
        }
        // First answer per member wins. A member's answer is monotonic -- ballots only rise and a
        // tombstone once applied is never un-applied -- so a second one cannot contradict the first
        // in any way that matters, and nothing is gained by preferring either.
        sweep.answers.putIfAbsent(response.senderId(), response.answer());
        decideIfPermitted();
    }

    /**
     * Stop sweeping, without running the pending sweep's completion: there is nothing for a
     * sweeper to do next on a node that is shutting down, and a purge decided here would be
     * broadcast by a member on its way out of the cluster.
     */
    public void close() {
        stopped = true;
        if (sweep != null) {
            sweep.cancelDeadline();
            sweep = null;
        }
    }

    /**
     * Decide the sweep in flight now on the answers it has -- what the deadline does, without the
     * waiting. Package-private: it exists so a test can drive the blocked path without depending on
     * a clock, and there is nothing an operator or another component would use it for.
     */
    void expireNow() {
        if (sweep != null) {
            decide();
        }
    }

    /**
     * End the sweep early, and only ever towards collecting: once every member has permitted there
     * is nothing left to wait for. A sweep that is already blocked waits out its deadline instead,
     * which costs nothing and buys the operator the whole list of members holding it up rather than
     * whichever one refused first.
     */
    private void decideIfPermitted() {
        if (permitted(sweep)) {
            decide();
        }
    }

    /**
     * The decision, and the only place a sweep ends. Clears the sweep before running the completion,
     * so a sweeper that starts the next candidate from it finds this collector idle.
     */
    private void decide() {
        final Sweep current = sweep;
        sweep = null;
        current.cancelDeadline();

        if (permitted(current)) {
            broadcastAndApply(current);
            current.onDecided.accept(Outcome.COLLECTED);
            return;
        }
        current.onDecided.accept(new Outcome(false, blockers(current)));
    }

    /**
     * Every member permits, counted by identity rather than by answers received: an answer from
     * someone the membership snapshot does not name settles nothing about the members it does, and a
     * member with no answer at all has permitted nothing.
     */
    private boolean permitted(final Sweep current) {
        if (!permits(current, nodeId)) {
            return false;
        }
        for (int i = 0; i < current.peers.size(); i++) {
            if (!permits(current, current.peers.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean permits(final Sweep current, final NodeId member) {
        final PurgeAnswer answer = current.answers.get(member);
        return answer != null && answer.permitsCollection();
    }

    /** Who held this sweep up, and with what -- one entry per blocking member, silence included. */
    private List<TombstoneSweep.Blocker> blockers(final Sweep current) {
        final List<TombstoneSweep.Blocker> blocking = new ArrayList<>();
        if (!permits(current, nodeId)) {
            blocking.add(new TombstoneSweep.Blocker(nodeId, current.answers.get(nodeId)));
        }
        for (int i = 0; i < current.peers.size(); i++) {
            final NodeId peer = current.peers.get(i);
            if (!permits(current, peer)) {
                blocking.add(new TombstoneSweep.Blocker(peer, current.answers.get(peer)));
            }
        }
        return blocking;
    }

    /**
     * Tell every member, and this node with them. The broadcast expects no reply and gets no retry:
     * a member that misses it keeps the tombstone, which is where that key already was, and answers
     * HELD to the next sweep -- which permits. What it must not be is a decision
     * each member reaches on its own, because one that had collected and one that had not would
     * differ, and anti-entropy would call that divergence and repair it by sending the tombstone
     * back, forever.
     */
    private void broadcastAndApply(final Sweep current) {
        final PeerMessage.PurgeReq decision = new PeerMessage.PurgeReq(
                nodeId, current.correlationId, current.key, current.tombstoneBallot);
        for (int i = 0; i < current.peers.size(); i++) {
            try {
                transport.send(current.peers.get(i), decision);
            } catch (final Exception ignored) {
                // Same as a member that is down when the decision goes out, and handled the same way:
                // by the next sweep.
            }
        }
        // This node applies its own decision like any other member. A refusal here -- a log that has
        // gone degraded since it answered -- leaves this node holding the tombstone, which the next
        // sweep re-decides; there is nothing to undo, because the tombstone is what it was hiding.
        store.purge(current.key, current.tombstoneBallot);
    }

    /** One sweep of one key: what was asked, of whom, and what has come back. */
    private static final class Sweep {
        private final long correlationId;
        private final HashedBytes key;
        private final Ballot tombstoneBallot;
        /** The membership this decision reads, excluding this node. Snapshotted at the ask. */
        private final List<NodeId> peers;
        private final Map<NodeId, PurgeAnswer> answers = new HashMap<>();
        private final Consumer<Outcome> onDecided;
        private EventLoop.TimerHandle deadline;

        private Sweep(final long correlationId,
                      final HashedBytes key,
                      final Ballot tombstoneBallot,
                      final List<NodeId> peers,
                      final Consumer<Outcome> onDecided) {
            this.correlationId = correlationId;
            this.key = key;
            this.tombstoneBallot = tombstoneBallot;
            this.peers = peers;
            this.onDecided = onDecided;
        }

        private void cancelDeadline() {
            if (deadline != null) {
                deadline.cancel();
                deadline = null;
            }
        }
    }
}
