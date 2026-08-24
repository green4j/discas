/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How a round classifies its own failure, and what that costs the caller.
 * <p>
 * The split is the point: a round rejected by a higher ballot lost a duel and retrying resolves it,
 * while a round that never collected a quorum of promises is looking at peers that will be just as
 * silent next time. Retrying the second case costs the whole budget --
 * {@code roundTimeout x (1 + maxRoundRetries)} -- before the caller can switch coordinators.
 */
@DisplayName("Proposer -- round failure classification")
class RoundFailureTest {

    private static final Duration ROUND_TIMEOUT = Duration.ofMillis(300);
    private static final int MAX_RETRIES = 3;
    private static final Duration NO_QUORUM_BACKOFF = Duration.ofSeconds(30);

    private EventLoop loop;

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    @BeforeEach
    void setUp() {
        loop = new EventLoop("round-failure");
        loop.start();
    }

    @AfterEach
    void tearDown() {
        loop.shutdown();
        loop.awaitTermination(Duration.ofSeconds(2));
    }

    private static NodeConfig timing(final int clusterSize, final Duration noQuorumBackoff) {
        return timing(clusterSize, noQuorumBackoff, ROUND_TIMEOUT,
                NodeConfig.builder().proposalExpiry());
    }

    private static NodeConfig timing(final int clusterSize, final Duration noQuorumBackoff,
                                     final Duration roundTimeout, final Duration proposalExpiry) {
        return NodeConfig.builder()
                .nodeId(nid(1))
                .clusterId(ClusterId.of("round-failure"))
                .clusterSize(clusterSize)
                .roundTimeout(roundTimeout)
                .maxRoundRetries(MAX_RETRIES)
                .noQuorumBackoff(noQuorumBackoff)
                .proposalExpiry(proposalExpiry)
                .build();
    }

    private Proposer proposer(final PeerTransport transport, final Acceptor acceptor,
                              final LocalStore store, final int clusterSize) {
        return proposer(transport, acceptor, store, clusterSize, NO_QUORUM_BACKOFF);
    }

    private Proposer proposer(final PeerTransport transport, final Acceptor acceptor,
                              final LocalStore store, final int clusterSize,
                              final Duration noQuorumBackoff) {
        return proposer(transport, acceptor, store, timing(clusterSize, noQuorumBackoff));
    }

    private Proposer proposer(final PeerTransport transport, final Acceptor acceptor,
                              final LocalStore store, final NodeConfig timing) {
        return new Proposer(nid(1), transport, acceptor, loop, store,
                new CorrelationIdGenerator(nid(1)), NodeObserver.NONE, timing);
    }

    /** Peers that are configured but never answer anything -- the partitioned-away majority. */
    private static PeerTransport silent(final int clusterSize, final List<NodeId> peers) {
        return new PeerTransport() {
            @Override
            public void send(final NodeId targetNodeId, final PeerMessage message) {
            }

            @Override
            public void register(final Consumer<PeerMessage> handler) {
            }

            @Override
            public List<NodeId> peers() {
                return peers;
            }

            @Override
            public int clusterSize() {
                return clusterSize;
            }
        };
    }

    private static RoundFailedException failureOf(final CompletableFuture<Proposer.CasResult> f)
            throws Exception {
        final ExecutionException thrown =
                assertThrows(ExecutionException.class, () -> f.get(30, TimeUnit.SECONDS));
        return assertInstanceOf(RoundFailedException.class, thrown.getCause());
    }

    @Test
    @DisplayName("Prepare without quorum fails after one round timeout and is not retried")
    void insufficientRespondersFailsFast() throws Exception {
        final LocalStore store = new LocalStore(new InMemoryWal());
        final Proposer proposer = proposer(
                silent(3, List.of(nid(2), nid(3))), new Acceptor(nid(1), store), store, 3);

        final long startedAt = System.nanoTime();
        final RoundFailedException failure = failureOf(
                proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));
        final Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertEquals(RoundFailure.INSUFFICIENT_RESPONDERS, failure.failure());
        // The whole point: one timeout, not (1 + MAX_RETRIES) of them. Two is generous headroom
        // for a loaded CI box while still failing if the retry path runs at all.
        assertTrue(elapsed.compareTo(ROUND_TIMEOUT.multipliedBy(2)) < 0,
                "Expected ~one round timeout, took " + elapsed.toMillis() + "ms");
    }

    @Test
    @DisplayName("A ballot NACK still retries -- the round wins on the next, higher ballot")
    void ballotNackRetries() throws Exception {
        final LocalStore store = new LocalStore(new InMemoryWal());
        final Acceptor acceptor = new Acceptor(nid(1), store);
        // A single-node cluster makes the self-NACK alone exceed the tolerated NACK count
        // (clusterSize - quorumSize == 0), so the round takes the BALLOT_NACK path immediately
        // rather than waiting for a timeout.
        acceptor.handlePrepare(new PeerMessage.PrepareReq(
                nid(99), 0L, TestBytes.hashed("k"), new Ballot(999_999L, nid(99))));

        final Proposer proposer = proposer(silent(1, List.of()), acceptor, store, 1);

        // Succeeds only because the NACK was retried: the retry observes the foreign ballot,
        // bumps past it, and wins. No retry would mean a failed future here.
        final Proposer.CasResult result = proposer
                .write(TestBytes.hashed("k"), current -> TestBytes.hashed("v"))
                .get(30, TimeUnit.SECONDS);

        assertEquals(TestBytes.hashed("v"), result.value());
        assertTrue(result.version().counter() > 999_999L,
                "Retry should have bumped past the ballot that NACKed it");
    }

    @Test
    @DisplayName("A stalled accept is retried -- reaching prepare quorum is not unreachability")
    void acceptTimeoutRetries() throws Exception {
        final LocalStore store = new LocalStore(new InMemoryWal());
        final PrepareOnlyTransport transport = new PrepareOnlyTransport(3, List.of(nid(2), nid(3)));
        final Proposer proposer = proposer(transport, new Acceptor(nid(1), store), store, 3);
        // Route the synthesised promises back into the proposer, as DisCasNode does in production.
        transport.register(message -> loop.execute(() -> {
            if (message instanceof PeerMessage.PrepareResp) {
                proposer.onPrepareResp((PeerMessage.PrepareResp) message);
            } else if (message instanceof PeerMessage.AcceptResp) {
                proposer.onAcceptResp((PeerMessage.AcceptResp) message);
            }
        }));

        final long startedAt = System.nanoTime();
        final RoundFailedException failure = failureOf(
                proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));
        final Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertEquals(RoundFailure.ACCEPT_TIMEOUT, failure.failure());
        // Must NOT be mistaken for unreachability: these acceptors answered prepare moments ago,
        // so the round is expected to spend its retries rather than fail on the first timeout.
        assertTrue(elapsed.compareTo(ROUND_TIMEOUT.multipliedBy(2)) > 0,
                "Expected the retry budget to be spent, took only " + elapsed.toMillis() + "ms");
    }

    @Test
    @DisplayName("Optimistic before any round has run -- knowing nothing is not a reason to refuse")
    void optimisticBeforeAnyRound() {
        final LocalStore store = new LocalStore(new InMemoryWal());
        final Proposer proposer = proposer(
                silent(3, List.of(nid(2), nid(3))), new Acceptor(nid(1), store), store, 3);

        assertTrue(proposer.canReachMajority(),
                "A proposer that has run no rounds must not refuse work");
    }

    @Test
    @DisplayName("A failed round sets the verdict; a successful one clears it")
    void verdictSetAndCleared() throws Exception {
        final LocalStore store = new LocalStore(new InMemoryWal());
        final PrepareOnlyTransport transport = new PrepareOnlyTransport(3, List.of(nid(2), nid(3)));
        // Start with peers silent so the first round proves unreachability.
        transport.answering = false;
        final Proposer proposer = proposer(transport, new Acceptor(nid(1), store), store, 3);
        transport.register(message -> loop.execute(() -> {
            if (message instanceof PeerMessage.PrepareResp) {
                proposer.onPrepareResp((PeerMessage.PrepareResp) message);
            }
        }));

        failureOf(proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));
        assertFalse(proposer.canReachMajority(), "An unreachable majority must be remembered");

        // Peers come back. The next round reaches prepare quorum, which is the only proof there is.
        transport.answering = true;
        failureOf(proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));
        assertTrue(proposer.canReachMajority(),
                "Reaching prepare quorum must clear the verdict even if the round later stalls");
    }

    @Test
    @DisplayName("The verdict lapses on its own -- a refusing node cannot refuse forever")
    void verdictExpires() throws Exception {
        final Duration backoff = Duration.ofMillis(400);
        final LocalStore store = new LocalStore(new InMemoryWal());
        final Proposer proposer = proposer(
                silent(3, List.of(nid(2), nid(3))), new Acceptor(nid(1), store), store, 3, backoff);

        failureOf(proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));
        assertFalse(proposer.canReachMajority());

        // Nothing prompts it: a node that refuses everything runs no rounds, so without the expiry
        // it would never learn the partition healed. Waited for rather than slept past, so a busy
        // machine makes this slower instead of red.
        TestAwait.until("the no-majority verdict to lapse", Duration.ofSeconds(30), () -> {
            if (!proposer.canReachMajority()) {
                throw new IllegalStateException("Still refusing");
            }
        });
        assertTrue(proposer.canReachMajority(),
                "The verdict must lapse so the next request re-probes");
    }

    @Test
    @DisplayName("proposalExpiry ends the retry chain instead of letting it run to maxRoundRetries")
    void proposalExpiryEndsTheRetryChain() throws Exception {
        // Room for one retry and not four: the chain must stop because the budget ran out, which is
        // only visible if maxRoundRetries alone would have kept going.
        final Duration expiry = ROUND_TIMEOUT.multipliedBy(2);
        final LocalStore store = new LocalStore(new InMemoryWal());
        final PrepareOnlyTransport transport = new PrepareOnlyTransport(3, List.of(nid(2), nid(3)));
        final Proposer proposer = proposer(transport, new Acceptor(nid(1), store), store,
                timing(3, NO_QUORUM_BACKOFF, ROUND_TIMEOUT, expiry));
        transport.register(message -> loop.execute(() -> {
            if (message instanceof PeerMessage.PrepareResp) {
                proposer.onPrepareResp((PeerMessage.PrepareResp) message);
            } else if (message instanceof PeerMessage.AcceptResp) {
                proposer.onAcceptResp((PeerMessage.AcceptResp) message);
            }
        }));

        final long startedAt = System.nanoTime();
        final RoundFailedException failure = failureOf(
                proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));
        final Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        // Still ACCEPT_TIMEOUT, deliberately. Accepts went out before the budget ran out, so the
        // outcome is indeterminate and relabelling it PROPOSAL_EXPIRED -- a determinate code --
        // would tell the caller "did not happen" about a write that may yet land.
        assertEquals(RoundFailure.ACCEPT_TIMEOUT, failure.failure(),
                "An accept was already broadcast, so the answer must stay indeterminate");
        assertTrue(elapsed.compareTo(ROUND_TIMEOUT.multipliedBy(1 + MAX_RETRIES)) < 0,
                "Expected the expiry to cut the chain short, took " + elapsed.toMillis() + "ms");
    }

    @Test
    @DisplayName("An expired ballot duel is determinate -- nothing was proposed, so PROPOSAL_EXPIRED")
    void expiredBallotDuelIsDeterminate() throws Exception {
        final LocalStore store = new LocalStore(new InMemoryWal());
        final Acceptor acceptor = new Acceptor(nid(1), store);

        // A budget too small for even the first backoff, so the retry that ballotNackRetries
        // depends on is refused. Built before the foreign promise below, so its ballot counter
        // starts under that ballot and the self-prepare is genuinely NACKed -- a proposer that
        // already knew the higher ballot would simply out-bid it and win.
        final Proposer proposer = proposer(silent(1, List.of()), acceptor, store,
                timing(1, NO_QUORUM_BACKOFF, ROUND_TIMEOUT, Duration.ofMillis(1)));

        // A single-node cluster turns the self-NACK into an immediate BALLOT_NACK
        // (clusterSize - quorumSize == 0), so the round reaches failOrRetry without a timeout.
        acceptor.handlePrepare(new PeerMessage.PrepareReq(
                nid(99), 0L, TestBytes.hashed("k"), new Ballot(999_999L, nid(99))));

        final RoundFailedException failure = failureOf(
                proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));

        // A NACKed prepare never reaches Accept, so no acceptor saw the value: the caller is owed a
        // definite "did not happen" rather than the indeterminate answer a bare timeout would give.
        assertEquals(RoundFailure.PROPOSAL_EXPIRED, failure.failure());
        assertFalse(RoundFailure.PROPOSAL_EXPIRED.retryable(),
                "The budget being retried against is what ran out");
    }

    @Test
    @DisplayName("A round whose prepare quorum arrives after the expiry proposes nothing at all")
    void expiryAbandonsBeforeProposing() throws Exception {
        final Duration expiry = Duration.ofMillis(200);
        final Duration promiseDelay = Duration.ofMillis(500);
        final LocalStore store = new LocalStore(new InMemoryWal());
        // Round timeout generous, so the round is still alive when the promises land and the expiry
        // is what ends it rather than the per-attempt timer.
        final SlowPromiseTransport transport = new SlowPromiseTransport(
                3, List.of(nid(2), nid(3)), promiseDelay);
        final Proposer proposer = proposer(transport, new Acceptor(nid(1), store), store,
                timing(3, NO_QUORUM_BACKOFF, Duration.ofSeconds(10), expiry));
        transport.register(message -> loop.execute(() -> {
            if (message instanceof PeerMessage.PrepareResp) {
                proposer.onPrepareResp((PeerMessage.PrepareResp) message);
            } else if (message instanceof PeerMessage.AcceptResp) {
                proposer.onAcceptResp((PeerMessage.AcceptResp) message);
            }
        }));

        final RoundFailedException failure = failureOf(
                proposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));

        assertEquals(RoundFailure.PROPOSAL_EXPIRED, failure.failure());
        // The assertion the design rests on. PROPOSAL_EXPIRED is reported as determinate only
        // because the check sits before the Accept broadcast; if an AcceptReq had gone out, some
        // acceptor could hold the value and a later proposer could complete it.
        assertEquals(0, transport.acceptsSent(),
                "An expired round must not propose -- otherwise the write is not provably absent");
    }

    /**
     * Promises that arrive late: prepare reaches quorum, but only after the operation's budget has
     * run out. Isolates the expiry check in {@code advanceToAccept} from the per-round timer.
     */
    private static final class SlowPromiseTransport implements PeerTransport {
        private final int clusterSize;
        private final List<NodeId> peers;
        private final Duration promiseDelay;
        private Consumer<PeerMessage> handler;
        private final AtomicInteger acceptsSent = new AtomicInteger();

        private SlowPromiseTransport(final int clusterSize, final List<NodeId> peers,
                                     final Duration promiseDelay) {
            this.clusterSize = clusterSize;
            this.peers = peers;
            this.promiseDelay = promiseDelay;
        }

        int acceptsSent() {
            return acceptsSent.get();
        }

        @Override
        public void send(final NodeId targetNodeId, final PeerMessage message) {
            if (message instanceof PeerMessage.AcceptReq) {
                acceptsSent.incrementAndGet();
                return;
            }
            if (!(message instanceof PeerMessage.PrepareReq)) {
                return;
            }
            final PeerMessage.PrepareReq request = (PeerMessage.PrepareReq) message;
            final Thread late = new Thread(() -> {
                try {
                    Thread.sleep(promiseDelay.toMillis());
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                handler.accept(new PeerMessage.PrepareResp(
                        targetNodeId, request.correlationId(), true, request.ballot(),
                        Ballot.ZERO, null, false));
            }, "late-promise-" + targetNodeId);
            late.setDaemon(true);
            late.start();
        }

        @Override
        public void register(final Consumer<PeerMessage> messageHandler) {
            this.handler = messageHandler;
        }

        @Override
        public List<NodeId> peers() {
            return peers;
        }

        @Override
        public int clusterSize() {
            return clusterSize;
        }
    }

    /**
     * Peers that promise but never accept: prepare reaches quorum, phase 2 stalls. This is the
     * case that must not be classified as unreachability.
     */
    private static final class PrepareOnlyTransport implements PeerTransport {
        private final int clusterSize;
        private final List<NodeId> peers;
        private Consumer<PeerMessage> handler;
        /** Flipped by tests that need the peers to go away and come back. */
        private volatile boolean answering = true;

        private PrepareOnlyTransport(final int clusterSize, final List<NodeId> peers) {
            this.clusterSize = clusterSize;
            this.peers = peers;
        }

        @Override
        public void send(final NodeId targetNodeId, final PeerMessage message) {
            if (!answering || !(message instanceof PeerMessage.PrepareReq)) {
                return; // AcceptReq is dropped -- that is the stall being simulated
            }
            final PeerMessage.PrepareReq request = (PeerMessage.PrepareReq) message;
            handler.accept(new PeerMessage.PrepareResp(
                    targetNodeId, request.correlationId(), true, request.ballot(),
                    Ballot.ZERO, null, false));
        }

        @Override
        public void register(final Consumer<PeerMessage> messageHandler) {
            this.handler = messageHandler;
        }

        @Override
        public List<NodeId> peers() {
            return peers;
        }

        @Override
        public int clusterSize() {
            return clusterSize;
        }
    }
}
