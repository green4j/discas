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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Drives one CASPaxos round per operation: prepare a ballot, collect promises from a quorum, apply
 * the caller's transform to the highest accepted value it saw, and broadcast the accept.
 * <p>
 * Rounds are bounded ({@code MAX_PENDING}) and retried with jittered backoff, and each carries a
 * {@link RoundFailure} saying which phase it died in -- the difference between a determinate
 * failure and one whose outcome nobody knows.
 */
final class Proposer {

    /** What a round settled on: the value, its version, and whether a fence matched. */
    public static class CasResult {
        private final HashedBytes value;
        private final boolean tombstone;
        private final boolean keyExists;
        private final Ballot version;
        private final boolean fenceMatched;

        CasResult(final HashedBytes value,
                  final boolean tombstone,
                  final boolean keyExists,
                  final Ballot version) {
            this(value, tombstone, keyExists, version, true);
        }

        CasResult(final HashedBytes value,
                  final boolean tombstone,
                  final boolean keyExists,
                  final Ballot version,
                  final boolean fenceMatched) {
            this.value = value;
            this.tombstone = tombstone;
            this.keyExists = keyExists;
            this.version = version;
            this.fenceMatched = fenceMatched;
        }

        /**
         * False when the round carried an expected version that did not match the quorum-agreed
         * accepted ballot. Nothing was proposed, and {@link #version()} carries the version that
         * was found instead -- enough for the caller to recompute without a second round trip.
         * <p>
         * Always true for an unfenced round: there was no comparison to lose.
         */
        boolean fenceMatched() {
            return fenceMatched;
        }

        public HashedBytes value() {
            return value;
        }

        public boolean tombstone() {
            return tombstone;
        }

        /**
         * True if the key had been previously committed (even as tombstone)
         * False if no acceptor in the quorum ever accepted a value for this key
         */
        boolean keyExists() {
            return keyExists;
        }

        /**
         * The accepted ballot of the value this result reflects -- the per-key version
         * (monotonic, advances on every commit including tombstones). {@link Ballot#ZERO}
         * when the key never existed. See the "watching keys" design note.
         */
        public Ballot version() {
            return version;
        }
    }

    private final NodeId nodeId;
    private final PeerTransport transport;
    private final Acceptor selfAcceptor;
    private final EventLoop loop;
    private final LocalStore store;
    private final CorrelationIdGenerator correlationIdGenerator;
    private final NodeObserver observer;
    private final int quorumSize;
    private final int clusterSize;

    private long ballotCounter;
    private static final long BALLOT_RESERVATION_CHUNK = 1024;

    private final Map<Long, PendingRound> pending = new HashMap<>();

    /**
     * Tracks parent futures that are in backoff-retry state
     * failOrRetry() removes the round from {@code pending} before scheduling
     * a retry timer. If shutdown occurs in that window, drainAllPending()
     * cannot see the future. This list makes them reachable for cleanup.
     * Accessed only from the event loop thread
     */
    private final List<PendingRetry> pendingRetries = new ArrayList<>();

    private final Duration roundTimeout;
    private final int maxRetries;
    private final Duration retryBackoffBase;
    private final Duration retryBackoffJitter;
    private final long noQuorumBackoffNanos;

    /**
     * The wall-clock budget for one write, retries included. {@link #roundTimeout} bounds a single
     * attempt and every retry restarts it, so on its own it bounds nothing a caller can be told:
     * an abandoned proposal may keep trying to commit for the whole chain. This bounds the chain,
     * and is checked before the Accept broadcast so an expired write is provably not applied.
     */
    private final long proposalExpiryNanos;

    /**
     * The last observed verdict on whether this node can see a majority, and when it was formed.
     * <p>
     * Derived entirely from what rounds actually did -- no probe, no heartbeat, no failure
     * detector. A round either collected {@code quorumSize} promises or it did not, and that is a
     * better answer than any liveness signal because it measures the thing a caller cares about:
     * whether a round can complete right now.
     */
    private boolean noMajorityObserved;
    private long noMajorityAtNanos;

    /**
     * Pending rounds are bounded, so an overloaded node applies back-pressure to clients rather
     * than growing without limit. Each pending round holds 600-800 bytes -- key, ballot, transform
     * closure, future, dedup sets, timer entry -- so the cap costs 6-8 MB of heap. Hitting it
     * consistently means rounds are timing out faster than they complete, which more memory does
     * not fix.
     */
    private static final int MAX_PENDING = 10_000;

    Proposer(final NodeId nodeId, final PeerTransport transport,
                    final Acceptor selfAcceptor, final EventLoop loop,
                    final LocalStore store, final CorrelationIdGenerator correlationIdGenerator) {
        this(nodeId, transport, selfAcceptor, loop, store, correlationIdGenerator, NodeObserver.NONE);
    }

    /**
     * Round timing defaults to {@link NodeConfig}'s -- a bare builder is the source of every
     * default, so no value is duplicated here as a constant.
     */
    Proposer(final NodeId nodeId, final PeerTransport transport,
                    final Acceptor selfAcceptor, final EventLoop loop,
                    final LocalStore store, final CorrelationIdGenerator correlationIdGenerator,
                    final NodeObserver observer) {
        this(nodeId, transport, selfAcceptor, loop, store, correlationIdGenerator, observer,
                NodeConfig.builder());
    }

    /** @param timing supplies the round timeout, retry count and retry backoff. */
    Proposer(final NodeId nodeId, final PeerTransport transport,
                    final Acceptor selfAcceptor, final EventLoop loop,
                    final LocalStore store, final CorrelationIdGenerator correlationIdGenerator,
                    final NodeObserver observer, final NodeConfig timing) {
        this(nodeId, transport, selfAcceptor, loop, store, correlationIdGenerator, observer,
                timing.roundTimeout(), timing.maxRoundRetries(),
                timing.roundRetryBackoffBase(), timing.roundRetryBackoffJitter(),
                timing.noQuorumBackoff(), timing.proposalExpiry());
    }

    private Proposer(final NodeId nodeId, final PeerTransport transport,
                     final Acceptor selfAcceptor, final EventLoop loop,
                     final LocalStore store, final CorrelationIdGenerator correlationIdGenerator,
                     final NodeObserver observer, final NodeConfig.Builder timing) {
        this(nodeId, transport, selfAcceptor, loop, store, correlationIdGenerator, observer,
                timing.roundTimeout(), timing.maxRoundRetries(),
                timing.roundRetryBackoffBase(), timing.roundRetryBackoffJitter(),
                timing.noQuorumBackoff(), timing.proposalExpiry());
    }

    private Proposer(final NodeId nodeId, final PeerTransport transport,
                     final Acceptor selfAcceptor, final EventLoop loop,
                     final LocalStore store, final CorrelationIdGenerator correlationIdGenerator,
                     final NodeObserver observer,
                     final Duration roundTimeout, final int maxRetries,
                     final Duration retryBackoffBase, final Duration retryBackoffJitter,
                     final Duration noQuorumBackoff, final Duration proposalExpiry) {
        this.roundTimeout = roundTimeout;
        this.maxRetries = maxRetries;
        this.retryBackoffBase = retryBackoffBase;
        this.retryBackoffJitter = retryBackoffJitter;
        this.noQuorumBackoffNanos = noQuorumBackoff.toNanos();
        this.proposalExpiryNanos = proposalExpiry.toNanos();
        this.nodeId = nodeId;
        this.transport = transport;
        this.selfAcceptor = selfAcceptor;
        this.loop = loop;
        this.store = store;
        this.correlationIdGenerator = correlationIdGenerator;
        this.observer = observer == null ? NodeObserver.NONE : observer;
        this.clusterSize = transport.clusterSize();
        this.quorumSize = clusterSize / 2 + 1;
        this.ballotCounter = Math.max(
                store.reservedProposerBallot(),
                store.maxBallotSeen());
    }

    /**
     * Whether this node believes it can still reach a majority, as of its most recent round.
     * <p>
     * <b>Optimistic by default.</b> Having formed no opinion means <em>yes</em>: a freshly started
     * or idle cluster has run no rounds and must not refuse work on the strength of knowing
     * nothing. The verdict turns negative only on positive evidence -- a round that timed out
     * without a quorum of promises -- and lapses again after {@code noQuorumBackoff} so a healed
     * partition is rediscovered without anything having to prompt it.
     * <p>
     * Advisory only. It never narrows {@link #quorumSize}, and a round still needs
     * {@code N/2 + 1} genuine acks whatever this says. Being wrong optimistically costs one slow
     * request; being wrong pessimistically costs one needless failover.
     */
    boolean canReachMajority() {
        return !noMajorityObserved
                || System.nanoTime() - noMajorityAtNanos >= noQuorumBackoffNanos;
    }

    /** A round proved a majority is out of reach. Starts the {@code noQuorumBackoff} window. */
    private void recordNoMajority() {
        noMajorityObserved = true;
        noMajorityAtNanos = System.nanoTime();
    }

    /** A round collected a quorum of promises: whatever we believed, we can reach a majority. */
    private void recordMajorityReached() {
        noMajorityObserved = false;
    }

    /**
     * An unconditional round: apply {@code transform} to whatever the quorum agrees is current and
     * commit the result. Not a {@code cas} -- it compares nothing.
     */
    public CompletableFuture<CasResult> write(
            final HashedBytes key, final Function<HashedBytes, HashedBytes> transform) {
        return startRound(key, transform, 0, true, null);
    }

    /**
     * A CAS fenced on the key's accepted ballot instead of on its bytes.
     * <p>
     * The comparison happens against the quorum-agreed state, after prepare and before anything
     * is proposed: that is the only point at which "the current version" is a fact rather than a
     * guess. A mismatch completes the round without an Accept phase, so a stale attempt costs one
     * prepare and changes nothing.
     * <p>
     * This is what makes a write safe to re-send after a coordinator stops answering.
     *
     * @param expectedVersion the ballot the caller last observed; {@link Ballot#ZERO} means
     *                        "the key has no committed value" (create-if-absent)
     */
    public CompletableFuture<CasResult> cas(
            final HashedBytes key, final Ballot expectedVersion,
            final Function<HashedBytes, HashedBytes> transform) {
        if (expectedVersion == null) {
            throw new IllegalArgumentException("expectedVersion is required");
        }
        return startRound(key, transform, 0, true, expectedVersion);
    }

    CompletableFuture<CasResult> identityRound(final HashedBytes key) {
        return startRound(key, Function.identity(), 0, true, null);
    }

    /**
     * Repair round that always broadcasts AcceptReq even when the
     * value is unchanged. Used by AntiEntropy to push state to laggard
     * replicas that missed the original Accept broadcast
     */
    CompletableFuture<CasResult> repairRound(final HashedBytes key) {
        return startRound(key, Function.identity(), 0, false, null);
    }

    /**
     * Response handlers deduplicate by senderId so duplicate delivery
     * cannot form a false quorum
     */
    void onPrepareResp(final PeerMessage.PrepareResp response) {
        final PendingRound round = pending.get(response.correlationId());
        if (round == null || round.phase != Phase.PREPARE) {
            return;
        }

        // Validate ballot BEFORE consuming the dedup slot. A stale
        // cross-incarnation response (F3) whose ballot doesn't match must not
        // occupy the senderId slot, otherwise the peer's fresh response is
        // silently dropped and the round stalls until timeout
        if (response.promised() == null) {
            return;
        }
        if (response.ok() && !response.promised().equals(round.ballot)) {
            return;
        }
        // A legitimate NACK must carry a promised ballot strictly
        // greater than the round's ballot (that's why the prepare was
        // rejected). After restart, correlation IDs reset and a stale
        // pre-restart NACK can collide with a new round's ID. Since
        // post-restart ballots are strictly higher (WAL reservation), the
        // stale NACK's promised ballot is lower -- this check catches it
        // without requiring a Message schema change
        if (!response.ok() && response.promised().compareTo(round.ballot) <= 0) {
            return;
        }

        if (!round.prepareResponders.add(response.senderId())) {
            return;
        }

        if (response.ok()) {
            round.promises.put(response.senderId(), response);
            if (round.promises.size() >= quorumSize) {
                advanceToAccept(round);
            }
        } else {
            observer.prepareRejected(response.promised());
            observeExternalBallot(response.promised());
            round.prepareNackCount++;
            if (round.prepareNackCount > clusterSize - quorumSize) {
                failOrRetry(round, RoundFailure.BALLOT_NACK,
                        "Prepare rejected: " + response.promised());
            }
        }
    }

    void onAcceptResp(final PeerMessage.AcceptResp response) {
        final PendingRound round = pending.get(response.correlationId());
        if (round == null || round.phase != Phase.ACCEPT) {
            return;
        }

        // Validate ballot before dedup (OK responses)
        if (response.promised() == null) {
            return;
        }
        if (response.ok() && !response.promised().equals(round.ballot)) {
            return;
        }
        // Reject stale NACKs (same rationale as onPrepareResp)
        if (!response.ok() && response.promised().compareTo(round.ballot) <= 0) {
            return;
        }

        if (!round.acceptResponders.add(response.senderId())) {
            return;
        }

        if (response.ok()) {
            round.acceptOkCount++;
            if (round.acceptOkCount >= quorumSize) {
                complete(round);
            }
        } else {
            observer.acceptRejected(response.promised());
            observeExternalBallot(response.promised());
            round.acceptNackCount++;
            if (round.acceptNackCount > clusterSize - quorumSize) {
                failOrRetry(round, RoundFailure.BALLOT_NACK,
                        "Accept rejected: " + response.promised());
            }
        }
    }

    /** First attempt of an operation: starts the {@code proposalExpiry} clock. */
    private CompletableFuture<CasResult> startRound(
            final HashedBytes key, final Function<HashedBytes, HashedBytes> transform,
            final int attempt, final boolean allowReadOnlyShortCircuit,
            final Ballot expectedVersion) {
        return startRound(key, transform, attempt, allowReadOnlyShortCircuit, expectedVersion,
                System.nanoTime() + proposalExpiryNanos);
    }

    /**
     * @param expiryNanos the operation's absolute deadline, inherited by every retry. Passing the
     *                    previous attempt's value is what makes the budget belong to the operation
     *                    rather than to each attempt separately.
     */
    private CompletableFuture<CasResult> startRound(
            final HashedBytes key, final Function<HashedBytes, HashedBytes> transform,
            final int attempt, final boolean allowReadOnlyShortCircuit,
            final Ballot expectedVersion, final long expiryNanos) {

        final CompletableFuture<CasResult> future = new CompletableFuture<>();

        if (pending.size() >= MAX_PENDING) {
            future.completeExceptionally(
                    new RuntimeException("Proposer overloaded (" + pending.size() + " pending)"));
            return future;
        }

        long correlationId = -1;
        PendingRound round = null;
        try {
            correlationId = correlationIdGenerator.next();
            final Ballot ballot = nextBallot();

            round = new PendingRound(correlationId, key, ballot, transform, attempt, future,
                    allowReadOnlyShortCircuit, expectedVersion, expiryNanos);
            pending.put(correlationId, round);

            final PendingRound scheduledRound = round;
            final long scheduledCorrelationId = correlationId;
            round.timerHandle = loop.schedule(roundTimeout, () -> {
                if (pending.remove(scheduledCorrelationId) != null) {
                    failOrRetry(scheduledRound, timeoutFailure(scheduledRound), "Timeout");
                }
            });

            final PeerMessage.PrepareReq prepareRequest =
                    new PeerMessage.PrepareReq(nodeId, correlationId, key, ballot);

            final PeerMessage.PrepareResp selfPrepareResponse = selfAcceptor.handlePrepare(prepareRequest);
            round.prepareResponders.add(selfPrepareResponse.senderId());

            if (selfPrepareResponse.ok()) {
                round.promises.put(selfPrepareResponse.senderId(), selfPrepareResponse);
                if (round.promises.size() >= quorumSize) {
                    advanceToAccept(round);
                    return future;
                }
            } else {
                observeExternalBallot(selfPrepareResponse.promised());
                round.prepareNackCount++;
                if (round.prepareNackCount > clusterSize - quorumSize) {
                    failOrRetry(round, RoundFailure.BALLOT_NACK,
                            "Prepare rejected (self): " + selfPrepareResponse.promised());
                    return future;
                }
            }

            broadcast(prepareRequest);
        } catch (final Exception e) {
            if (round != null && correlationId >= 0) {
                pending.remove(correlationId);
                if (round.timerHandle != null) {
                    round.timerHandle.cancel();
                }
            }
            if (!future.isDone()) {
                future.completeExceptionally(e);
            }
        }

        return future;
    }

    private void advanceToAccept(final PendingRound round) {
        round.phase = Phase.ACCEPT;
        // Reaching here means quorumSize promises arrived, which is the only proof of majority
        // reachability there is -- and the only place it can be observed for free.
        recordMajorityReached();

        // The expiry is enforced here, on the last instant at which nothing has been proposed yet.
        // Enforcing it later would leave the very ambiguity it exists to remove: an Accept already
        // broadcast can be adopted and completed by another proposer, so abandoning after that
        // point would not make the write not-happen, it would only stop us watching. Abandoning
        // before it does, which is what lets PROPOSAL_EXPIRED be reported as determinate.
        if (System.nanoTime() - round.expiryNanos >= 0) {
            failOrRetry(round, RoundFailure.PROPOSAL_EXPIRED,
                    "Abandoned before proposing: the operation outlived its proposalExpiry");
            return;
        }

        HashedBytes currentValue = null;
        boolean currentTombstone = false;
        Ballot highestAccepted = Ballot.ZERO;
        for (final PeerMessage.PrepareResp promise : round.promises.values()) {
            if (promise.accepted().compareTo(highestAccepted) > 0) {
                highestAccepted = promise.accepted();
                currentValue = promise.value();
                currentTombstone = promise.tombstone();
            }
        }

        final boolean keyPreviouslyCommitted = !highestAccepted.isZero();

        // The fence, checked against quorum-agreed state and before anything is proposed. A stale
        // attempt -- an abandoned round that comes back after the register moved on, or a caller
        // retrying against a version that has since been overtaken -- ends here, having cost one
        // prepare and changed nothing.
        if (round.expectedVersion != null && !highestAccepted.equals(round.expectedVersion)) {
            pending.remove(round.correlationId);
            if (round.timerHandle != null) {
                round.timerHandle.cancel();
            }
            round.future.complete(new CasResult(
                    currentValue, currentTombstone, keyPreviouslyCommitted, highestAccepted,
                    false));
            return;
        }

        final HashedBytes transformInput = currentTombstone ? null : currentValue;
        final HashedBytes transformOutput;
        try {
            transformOutput = round.transform.apply(transformInput);
        } catch (final Exception ex) {
            pending.remove(round.correlationId);
            if (round.timerHandle != null) {
                round.timerHandle.cancel();
            }
            round.future.completeExceptionally(ex);
            return;
        }

        if (!keyPreviouslyCommitted && transformOutput == null) {
            pending.remove(round.correlationId);
            if (round.timerHandle != null) {
                round.timerHandle.cancel();
            }
            round.future.complete(new CasResult(null, false, false, Ballot.ZERO));
            return;
        }

        if (keyPreviouslyCommitted) {
            final boolean newTombstone = (transformOutput == null);
            final boolean stateUnchanged;
            if (newTombstone && currentTombstone) {
                stateUnchanged = true;
            } else if (!newTombstone && !currentTombstone
                    && transformOutput.equals(currentValue)) {
                stateUnchanged = true;
            } else {
                stateUnchanged = false;
            }

            // Skip read-only short-circuit when disabled (repairRound)
            // Repair rounds must always broadcast AcceptReq to push state
            // to laggard replicas
            if (stateUnchanged && round.allowReadOnlyShortCircuit) {
                int confirmedCount = 0;
                for (final PeerMessage.PrepareResp promise : round.promises.values()) {
                    if (promise.accepted().equals(highestAccepted)) {
                        confirmedCount++;
                    }
                }
                if (confirmedCount >= quorumSize) {
                    pending.remove(round.correlationId);
                    if (round.timerHandle != null) {
                        round.timerHandle.cancel();
                    }
                    round.future.complete(new CasResult(
                            currentValue, currentTombstone, true, highestAccepted));
                    return;
                }
            }
        }

        round.newValue = transformOutput;
        round.newTombstone = (transformOutput == null);

        try {
            final PeerMessage.AcceptReq acceptRequest = new PeerMessage.AcceptReq(
                    nodeId, round.correlationId, round.key,
                    round.ballot, round.newValue, round.newTombstone);

            final PeerMessage.AcceptResp selfAcceptResponse = selfAcceptor.handleAccept(acceptRequest);
            round.acceptResponders.add(selfAcceptResponse.senderId());

            if (selfAcceptResponse.ok()) {
                round.acceptOkCount++;
                if (round.acceptOkCount >= quorumSize) {
                    complete(round);
                    return;
                }
            } else {
                observeExternalBallot(selfAcceptResponse.promised());
                round.acceptNackCount++;
                if (round.acceptNackCount > clusterSize - quorumSize) {
                    failOrRetry(round, RoundFailure.BALLOT_NACK,
                            "Accept rejected (self): " + selfAcceptResponse.promised());
                    return;
                }
            }

            broadcast(acceptRequest);
        } catch (final Exception e) {
            pending.remove(round.correlationId);
            if (round.timerHandle != null) {
                round.timerHandle.cancel();
            }
            round.future.completeExceptionally(e);
        }
    }

    private void complete(final PendingRound round) {
        pending.remove(round.correlationId);
        if (round.timerHandle != null) {
            round.timerHandle.cancel();
        }
        observer.roundCommitted(round.key, round.ballot);
        round.future.complete(new CasResult(round.newValue, round.newTombstone, true, round.ballot));
    }

    /**
     * Classify a round that ran out of time by how far it got.
     * <p>
     * Only a Prepare that never reached quorum proves this node cannot see a majority -- fewer than
     * {@code quorumSize} acceptors answered at all, and the same peers will be just as silent on
     * the next attempt. A round already in Accept heard from a quorum moments ago, so its stall is
     * not evidence of unreachability and stays retryable.
     */
    private RoundFailure timeoutFailure(final PendingRound round) {
        if (round.phase == Phase.PREPARE && round.promises.size() < quorumSize) {
            return RoundFailure.INSUFFICIENT_RESPONDERS;
        }
        return RoundFailure.ACCEPT_TIMEOUT;
    }

    /**
     * Send to every peer, best-effort: a peer we cannot reach yet (connecting, or in backoff) must
     * not abort the round -- a quorum tolerates missing acks and the round's own timer retries.
     */
    private void broadcast(final PeerMessage request) {
        final List<NodeId> peers = transport.peers();
        for (int i = 0; i < peers.size(); i++) {
            try {
                transport.send(peers.get(i), request);
            } catch (final Exception sendError) {
                // Deliberately swallowed; see above.
            }
        }
    }

    private void failOrRetry(final PendingRound round, final RoundFailure failure,
                             final String reason) {
        pending.remove(round.correlationId);
        if (round.timerHandle != null) {
            round.timerHandle.cancel();
        }
        if (failure == RoundFailure.INSUFFICIENT_RESPONDERS) {
            recordNoMajority();
        }

        if (failure.retryable() && round.attempt < maxRetries) {
            final Duration backoff = Duration.ofMillis(
                    (retryBackoffBase.toMillis() << round.attempt)
                            + ThreadLocalRandom.current().nextLong(retryBackoffJitter.toMillis()));

            // No budget left for another attempt. Reported as PROPOSAL_EXPIRED only when the
            // failure that got us here proved nothing was proposed; an accept that stalled is
            // indeterminate whatever the clock says, and relabelling it determinate would be the
            // exact loss of phase information this whole design removes.
            if (System.nanoTime() + backoff.toNanos() - round.expiryNanos >= 0) {
                final RoundFailure terminal = failure == RoundFailure.ACCEPT_TIMEOUT
                        ? failure : RoundFailure.PROPOSAL_EXPIRED;
                giveUp(round, terminal, reason + " (proposalExpiry reached)");
                return;
            }

            // Track the retry so drainAllPending() can reach the parent
            // future during shutdown. Without this, the future is invisible
            // between pending.remove() above and the retry timer firing.
            final EventLoop.TimerHandle retryHandle = loop.schedule(backoff, () -> {
                try {
                    startRound(round.key, round.transform, round.attempt + 1,
                            round.allowReadOnlyShortCircuit, round.expectedVersion,
                            round.expiryNanos)
                            .whenCompleteAsync((value, exception) -> {
                                pendingRetries.removeIf(pr -> pr.future == round.future);
                                if (exception != null) {
                                    round.future.completeExceptionally(exception);
                                } else {
                                    round.future.complete(value);
                                }
                            }, loop.rejectingExecutor());
                } catch (final Exception e) {
                    pendingRetries.removeIf(pr -> pr.future == round.future);
                    if (!round.future.isDone()) {
                        round.future.completeExceptionally(e);
                    }
                }
            });
            pendingRetries.add(new PendingRetry(round.future, retryHandle));
        } else {
            giveUp(round, failure, reason);
        }
    }

    /** Report a failure to the caller, no further attempt being made. */
    private void giveUp(final PendingRound round, final RoundFailure failure, final String reason) {
        observer.roundFailed(round.key, reason);
        round.future.completeExceptionally(new RoundFailedException(failure,
                (failure.retryable() ? "CAS failed after retries: " : "CAS failed: ") + reason));
    }

    private Ballot nextBallot() {
        final long next = ballotCounter + 1;
        if (next > store.reservedProposerBallot()) {
            final long newReservation = next + BALLOT_RESERVATION_CHUNK - 1;
            store.reserveProposerBallot(newReservation);
        }
        ballotCounter = next;
        return new Ballot(ballotCounter, nodeId);
    }

    private void observeExternalBallot(final Ballot seen) {
        if (seen.counter() > ballotCounter) {
            observer.externalBallotObserved(seen.counter());
            ballotCounter = seen.counter();
            if (ballotCounter > store.reservedProposerBallot()) {
                store.reserveProposerBallot(
                        ballotCounter + BALLOT_RESERVATION_CHUNK);
            }
        }
    }

    /**
     * Rounds started and not yet settled. Read only from the event loop; it exists so a test can
     * wait for a round to be in flight rather than sleep at it.
     */
    int pendingRounds() {
        return pending.size();
    }

    /**
     * Lift the ballot counter to everything recovery established, once recovery has finished.
     * <p>
     * The recovery floor is in the maximum because this node's own acceptor refuses at or below it.
     * A proposer starting under the floor would have every round NACKed by itself, walk up one
     * refusal at a time, and cost the caller a round for nothing -- and for a floor adopted from
     * the cluster, that band is as wide as the cluster's ballot history.
     */
    void rehydrateAfterRecovery() {
        final long target = Math.max(
                store.recoveryPromiseFloor(),
                Math.max(store.reservedProposerBallot(), store.maxBallotSeen()));
        if (target > ballotCounter) {
            ballotCounter = target;
        }
    }

    private enum Phase {
        PREPARE,
        ACCEPT
    }

    /**
     * Tracks responders by senderId so duplicate messages cannot yield a false quorum
     */
    private static final class PendingRound {
        final long correlationId;
        final HashedBytes key;
        final Ballot ballot;
        final Function<HashedBytes, HashedBytes> transform;
        final int attempt;
        final CompletableFuture<CasResult> future;
        final boolean allowReadOnlyShortCircuit;
        /** The ballot this round requires the key to be at, or null for an unfenced round. */
        final Ballot expectedVersion;
        /**
         * {@code System.nanoTime()} past which the operation is abandoned. Set on the first attempt
         * and copied into every retry, so the budget covers the chain rather than each link.
         */
        final long expiryNanos;

        Phase phase = Phase.PREPARE;

        /**
         * Nodes that responded to Prepare (OK or NACK), deduplicated by sender
         */
        final Set<NodeId> prepareResponders = new HashSet<>();
        /**
         * OK responses keyed by sender; used to read promised values
         */
        final Map<NodeId, PeerMessage.PrepareResp> promises = new HashMap<>();
        /**
         * Distinct NACK count during Prepare
         */
        int prepareNackCount = 0;

        /**
         * Nodes that responded to Accept (OK or NACK), deduplicated by sender
         */
        final Set<NodeId> acceptResponders = new HashSet<>();
        int acceptOkCount = 0;
        int acceptNackCount = 0;

        HashedBytes newValue;
        boolean newTombstone;
        EventLoop.TimerHandle timerHandle;

        PendingRound(final long correlationId, final HashedBytes key, final Ballot ballot,
                     final Function<HashedBytes, HashedBytes> transform, final int attempt,
                     final CompletableFuture<CasResult> future,
                     final boolean allowReadOnlyShortCircuit,
                     final Ballot expectedVersion,
                     final long expiryNanos) {
            this.expiryNanos = expiryNanos;
            this.correlationId = correlationId;
            this.key = key;
            this.ballot = ballot;
            this.transform = transform;
            this.attempt = attempt;
            this.future = future;
            this.allowReadOnlyShortCircuit = allowReadOnlyShortCircuit;
            this.expectedVersion = expectedVersion;
        }
    }

    /**
     * Fail all in-flight rounds with the given cause. Called during
     * node shutdown so no client future is left hanging
     * Must be called on the event loop thread
     */
    void drainAllPending(final RuntimeException cause) {
        // Drain in-backoff retries first. These futures are not in
        // pending (removed by failOrRetry before scheduling the retry timer)
        // Without this, shutdown during backoff leaves the parent future hanging
        for (int i = 0; i < pendingRetries.size(); i++) {
            final PendingRetry retry = pendingRetries.get(i);
            retry.timerHandle.cancel();
            if (!retry.future.isDone()) {
                retry.future.completeExceptionally(cause);
            }
        }
        pendingRetries.clear();

        for (final PendingRound round : pending.values()) {
            if (round.timerHandle != null) {
                round.timerHandle.cancel();
            }
            if (!round.future.isDone()) {
                round.future.completeExceptionally(cause);
            }
        }
        pending.clear();
    }

    /**
     * Pairs a parent future with its backoff retry timer handle
     */
    private static final class PendingRetry {
        final CompletableFuture<CasResult> future;
        final EventLoop.TimerHandle timerHandle;

        PendingRetry(final CompletableFuture<CasResult> future,
                     final EventLoop.TimerHandle timerHandle) {
            this.future = future;
            this.timerHandle = timerHandle;
        }
    }

    PeerTransport transport() {
        return transport;
    }
}
