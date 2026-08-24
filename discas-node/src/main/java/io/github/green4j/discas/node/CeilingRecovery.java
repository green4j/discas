/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerTransport;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Gets a promise floor for a node that started with no durable state of its own.
 *
 * <p>The rule is one sentence: <b>a node must not promise below anything it may already have
 * promised, and that bound either survived somewhere or survived nowhere.</b>
 *
 * <ul>
 *   <li><b>It survived.</b> Any ballot that can still be committed was promised by a quorum, and
 *       every member of that quorum raised its own ceiling to at least that ballot with a forced
 *       write before answering. So the maximum over {@code N - quorum + 1} members that still hold
 *       their ceilings is above it, whichever quorum it was:
 *       {@code |R| + |Q| - N >= 1}. This node cannot be one of them -- its memory is what is
 *       missing.</li>
 *   <li><b>It survived nowhere.</b> Every member answers that it holds nothing. Then no earlier
 *       round can commit either: a proposer's reserved ballot was durable too, and it is gone with
 *       everything else, so there is no longer anyone able to complete a round at any ballot. The
 *       floor is zero. A cluster starting for the first time and one whose members all lost their
 *       state are the same situation, and neither needs to be told from the other.</li>
 * </ul>
 *
 * <p>Anything in between -- some members remember and too few of them do -- is not a verdict, and
 * the node keeps asking. Nothing here decides a member is gone for good; the witness that is
 * missing may be a member with an intact disk that has not started yet. There is no timeout that
 * ends this and no flag that skips it: an operator who has accepted the loss says so by removing
 * what is left, which turns the cluster into the second case above.
 *
 * <p>Answers report {@code max(promisedUpTo, reservedProposerBallot)}: both are reserved ahead of
 * use with a forced write, so neither can be short after an unclean shutdown. {@code maxBallotSeen}
 * is derived from Promise records, which are not forced, and would carry the very gap this exists
 * to close.
 */
final class CeilingRecovery {

    private final NodeId nodeId;
    private final LocalStore store;
    private final PeerTransport transport;
    private final EventLoop loop;
    private final CorrelationIdGenerator correlationIdGenerator;
    private final NodeObserver observer;
    private final Duration retryCap;
    private final int witnessesRequired;

    /**
     * First retry delay, doubled per attempt up to the configured cap. Shaping is not a knob.
     * <p>
     * Short only because the first attempt of a cold start races the peers' own startup, which
     * resolves in milliseconds; it is a cadence, not a deadline, so nothing fails when a slow link
     * or a busy machine makes an answer late. Late answers count in full -- see
     * {@link #firstAttemptId}.
     */
    private static final Duration RETRY_BASE = Duration.ofMillis(100);
    private static final int RETRY_MAX_SHIFT = 16;
    /** Reports that this is still waiting, with the counts behind it. The node owns the state. */
    private final Consumer<String> waiting;

    /** What each member has answered during this recovery; a member is counted once. */
    private final Map<NodeId, Answer> answers = new HashMap<>();

    /** What this node's own bounds are worth to a peer asking. Reported in every answer. */
    private CeilingEvidence evidence = CeilingEvidence.NONE;

    /**
     * Whether this node's floor rests on there having been no history anywhere -- the fact it then
     * passes on. Transferable because the sweep that establishes it requires an answer from every
     * member, which proves each of them was already in its current life when it was taken.
     */
    private boolean establishedNoHistory;

    private Consumer<String> onAdopted;

    /**
     * Whether this recovery is asking. One field rather than a {@code running}/{@code stopped}
     * pair, whose fourth combination -- closed while still running -- {@code close()} made
     * unreachable by clearing one as it set the other.
     * <p>
     * A recovery that has adopted its floor goes back to {@link Progress#NOT_RUNNING}, the state it
     * started in, because that is what the pair did and nothing starts one twice.
     */
    private enum Progress { NOT_RUNNING, RUNNING, CLOSED }

    private Progress progress = Progress.NOT_RUNNING;
    /**
     * The span of correlation ids this recovery has issued. An answer inside it is an answer to one
     * of our own requests, whichever attempt it belongs to -- and all of them are equally true, since
     * every one was read after this node lost its state and both bounds only grow. Matching the
     * latest attempt alone would tie progress to the retry cadence: on a link whose round trip is
     * longer than the interval, every answer arrives after the next request has gone out.
     */
    private long firstAttemptId;
    private long lastAttemptId;
    /** Also which attempt this is, so the first one is recognised without a sentinel id. */
    private int attempts;
    private EventLoop.TimerHandle retryTimer;

    CeilingRecovery(final NodeId nodeId,
                           final LocalStore store,
                           final PeerTransport transport,
                           final EventLoop loop,
                           final CorrelationIdGenerator correlationIdGenerator,
                           final NodeObserver observer,
                           final Duration retryCap,
                           final Consumer<String> waiting) {
        this.nodeId = nodeId;
        this.store = store;
        this.transport = transport;
        this.loop = loop;
        this.correlationIdGenerator = correlationIdGenerator;
        this.observer = observer == null ? NodeObserver.NONE : observer;
        this.retryCap = retryCap;
        this.waiting = waiting;
        final int clusterSize = transport.clusterSize();
        this.witnessesRequired = clusterSize - (clusterSize / 2 + 1) + 1;
    }

    /**
     * What the replay just established about this node's own bounds. A node that proved its ceiling
     * is a witness from the start; a node that did not becomes one when it adopts a floor.
     */
    void afterReplay(final boolean ceilingIsProven) {
        this.evidence = ceilingIsProven ? CeilingEvidence.WITNESS : CeilingEvidence.NONE;
    }

    /**
     * Ask, and keep asking until one of the two cases holds.
     *
     * @param onAdopted run once, on the event loop, when the floor is durable and in force, with
     *                  the numbers behind it. Until then this node must not promise, accept or
     *                  serve, and the caller is expected to hold it out of the quorum for exactly
     *                  that long.
     */
    public void start(final Consumer<String> onAdopted) {
        if (progress != Progress.NOT_RUNNING) {
            return;
        }
        this.onAdopted = onAdopted;
        this.progress = Progress.RUNNING;
        sendAttempt();
    }

    /**
     * Answer a peer with this node's two bounds, and with whether they are a witness.
     * <p>
     * Answered whether or not this node has finished its own recovery. A node that started empty
     * answers zero and marks it as no evidence -- which is what lets a cluster holding nothing
     * anywhere establish exactly that about itself, rather than every member waiting on every other.
     * <p>
     * Not answered before local replay finishes, or the bounds would understate a ceiling this node
     * does hold; {@code DisCasNode} registers the peer handler only once replay is complete.
     */
    void handleRequest(final PeerMessage.CeilingReq request) {
        try {
            transport.send(request.senderId(), new PeerMessage.CeilingResp(
                    nodeId, request.correlationId(),
                    store.promisedUpTo(), store.reservedProposerBallot(), evidence));
        } catch (final Exception e) {
            // As with a digest request: the asker retries, and a send failure must not take down a
            // node that is otherwise healthy.
            observer.ceilingRequestFailed(request.senderId(), e);
        }
    }

    public void onResponse(final PeerMessage.CeilingResp response) {
        if (progress != Progress.RUNNING
                || response.correlationId() < firstAttemptId
                || response.correlationId() > lastAttemptId) {
            // Not an answer to anything this recovery asked -- a reply to a previous incarnation of
            // this node, whose ids come from a different epoch.
            return;
        }
        final Answer answer = new Answer(response.ceiling(), response.evidence());
        final Answer previous = answers.get(response.senderId());
        if (previous == null || answer.supersedes(previous)) {
            answers.put(response.senderId(), answer);
        }
        evaluate();
    }

    /**
     * Issue the next request now instead of waiting for the timer -- what the retry does, without
     * the waiting. Package-private: it exists so a test can drive the retry path without depending
     * on a clock, and there is nothing an operator or another component would use it for.
     */
    void retryNow() {
        if (progress == Progress.RUNNING) {
            sendAttempt();
        }
    }

    public void close() {
        progress = Progress.CLOSED;
        cancelRetry();
        answers.clear();
    }

    /** Adopt a floor if the answers so far settle it either way; otherwise wait for more. */
    private void evaluate() {
        long witnessFloor = 0L;
        long highestSeen = 0L;
        int witnesses = 0;
        boolean noHistoryAnywhere = false;
        for (final Answer answer : answers.values()) {
            highestSeen = Math.max(highestSeen, answer.ceiling);
            if (answer.evidence.isWitness()) {
                witnesses++;
                witnessFloor = Math.max(witnessFloor, answer.ceiling);
            }
            noHistoryAnywhere |= answer.evidence == CeilingEvidence.NO_HISTORY;
        }
        if (witnesses >= witnessesRequired) {
            adopt(witnessFloor);
            return;
        }
        // The same fact, learned two ways: this node sweeps every member and finds nothing above
        // zero anywhere, or a member that already did so says so. The second way is what keeps a
        // cluster starting from nothing from deadlocking -- whichever member concludes it first
        // begins serving, and its own ballots carry its ceiling above zero, after which no one else
        // could ever repeat the sweep.
        //
        // A sweep includes N=1, where there is nobody to ask and nobody who could have held a
        // promise this node made: the same case, not a special one.
        final boolean sweptClean =
                highestSeen == 0L && answers.size() == transport.peers().size();
        if (sweptClean || noHistoryAnywhere) {
            establishedNoHistory = true;
            adopt(highestSeen);
        }
    }

    private void sendAttempt() {
        attempts++;
        lastAttemptId = correlationIdGenerator.next();
        if (attempts == 1) {
            // Not "if the id is unset": a correlation id is seeded from a node id and nanoTime and
            // is frequently negative, so no value of it is free to mean "none yet".
            firstAttemptId = lastAttemptId;
        }
        // Answers are kept per member across attempts: one that answered and then went away has
        // still said something true. It was read after this node lost its state, and both bounds
        // only grow, so it cannot understate anything that mattered by then.
        final PeerMessage.CeilingReq request = new PeerMessage.CeilingReq(nodeId, lastAttemptId);
        final List<NodeId> peers = transport.peers();
        for (int i = 0; i < peers.size(); i++) {
            try {
                transport.send(peers.get(i), request);
            } catch (final Exception e) {
                // A peer that has not finished starting has no endpoint yet. Per-peer, so one
                // unreachable member does not stop the others being asked.
                observer.ceilingRequestFailed(peers.get(i), e);
            }
        }
        armRetry();
        // For N=1 the loop above sent nothing and this settles it immediately.
        evaluate();
    }

    /**
     * Ask again, soon at first and then less often.
     * <p>
     * The first attempt is nearly always wasted -- a cluster starts together, and a peer that has
     * not finished starting has nothing to answer with -- so a flat interval spends its whole length
     * on a race that resolves in milliseconds, and every cold start pays it. Doubling from
     * {@link #RETRY_BASE} to {@code retryCap} makes the common case settle in tens of milliseconds
     * and still leaves a node that is genuinely waiting on a member asking once a second rather than
     * twenty times.
     * <p>
     * Jittered, like every other retry here: on a cluster starting from nothing every member is
     * asking every other at once, and an exact interval keeps them in lockstep.
     */
    private void armRetry() {
        cancelRetry();
        final long delay = Math.min(
                RETRY_BASE.toNanos() << Math.min(attempts, RETRY_MAX_SHIFT),
                retryCap.toNanos());
        final long jittered = delay + ThreadLocalRandom.current().nextLong(delay / 4 + 1);
        retryTimer = loop.schedule(Duration.ofNanos(jittered), () -> {
            if (progress != Progress.RUNNING) {
                return;
            }
            waiting.accept(witnessCount() + " of " + witnessesRequired
                    + " witnesses, " + answers.size() + " of " + transport.peers().size()
                    + " answered");
            sendAttempt();
        });
    }

    private void cancelRetry() {
        if (retryTimer != null) {
            retryTimer.cancel();
            retryTimer = null;
        }
    }

    private int witnessCount() {
        int witnesses = 0;
        for (final Answer answer : answers.values()) {
            if (answer.evidence.isWitness()) {
                witnesses++;
            }
        }
        return witnesses;
    }

    private void adopt(final long floor) {
        if (!store.adoptRecoveryFloor(floor)) {
            // The log refused the floor or failed to force it. Believing it anyway is the one thing
            // that must not happen: this node would promise on a bound it cannot reproduce after a
            // restart. It stays out of the quorum; the WAL reports the cause itself.
            waiting.accept("floor=" + floor + " could not be made durable");
            armRetry();
            return;
        }
        progress = Progress.NOT_RUNNING;
        evidence = establishedNoHistory ? CeilingEvidence.NO_HISTORY : CeilingEvidence.WITNESS;
        cancelRetry();
        final String detail = "floor=" + floor + " from " + witnessCount() + " witnesses";
        answers.clear();
        if (onAdopted != null) {
            final Consumer<String> completion = onAdopted;
            onAdopted = null;
            completion.accept(detail);
        }
    }

    /** One member's reply: the bound it reports, and what that bound is worth. */
    private static final class Answer {
        private final long ceiling;
        private final CeilingEvidence evidence;

        private Answer(final long ceiling, final CeilingEvidence evidence) {
            this.ceiling = ceiling;
            this.evidence = evidence;
        }

        /**
         * A member that finished its own recovery between attempts gains evidence, which outranks
         * any ceiling; a member whose storage was replaced loses it, and what it said earlier still
         * stands. Otherwise the higher bound wins.
         */
        private boolean supersedes(final Answer previous) {
            if (evidence != previous.evidence) {
                return evidence.compareTo(previous.evidence) > 0;
            }
            return ceiling > previous.ceiling;
        }
    }
}
