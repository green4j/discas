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
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When a node without a floor may adopt one, and what it adopts. Drives {@link CeilingRecovery}
 * with hand-made answers, so each case is one exchange with no cluster, no timing and no race.
 * <p>
 * {@code CeilingRecoveryMatrixTest} covers the same mechanism from the other end -- whole clusters,
 * every start combination -- which is where an answer that only appears under real traffic shows up.
 * Both are needed and neither repeats the other: this one says what the rule <em>is</em>, that one
 * says what a cluster <em>does</em>.
 *
 * @see CeilingEvidence
 */
@DisplayName("Ceiling recovery -- when a floor may be adopted")
class CeilingVerdictTest {

    private static final NodeId SELF = NodeId.of("self");
    private static final NodeId PEER_A = NodeId.of("a");
    private static final NodeId PEER_B = NodeId.of("b");

    /** Records what was sent and answers nothing on its own. */
    private static final class RecordingTransport implements PeerTransport {
        private final List<PeerMessage> sent = new ArrayList<>();
        private final List<NodeId> peers;
        private final int clusterSize;

        private RecordingTransport(final int clusterSize, final NodeId... peers) {
            this.clusterSize = clusterSize;
            this.peers = List.of(peers);
        }

        @Override
        public void send(final NodeId target, final PeerMessage message) {
            sent.add(message);
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
    }

    private final RecordingTransport transport = new RecordingTransport(3, PEER_A, PEER_B);
    private final EventLoop loop = new EventLoop("verdict");
    private final InMemoryWal wal = new InMemoryWal();
    private final LocalStore store = new LocalStore(wal);
    private final List<String> adopted = new ArrayList<>();

    /**
     * Deliberately never started: {@code schedule} only queues, so the retry timer never fires and
     * every case is driven entirely by the answers it is given.
     */
    private CeilingRecovery recovering() {
        final CeilingRecovery recovery = new CeilingRecovery(
                SELF, store, transport, loop, new CorrelationIdGenerator(SELF), NodeObserver.NONE,
                Duration.ofSeconds(1), detail -> { });
        recovery.afterReplay(false);   // this node replayed nothing
        recovery.start(adopted::add);
        return recovery;
    }

    private long askedCorrelationId() {
        return transport.sent.get(transport.sent.size() - 1).correlationId();
    }

    private void answer(final CeilingRecovery recovery, final NodeId from,
                        final long ceiling, final CeilingEvidence evidence) {
        recovery.onResponse(new PeerMessage.CeilingResp(
                from, askedCorrelationId(), ceiling, 0L, evidence));
    }

    private void assertAdopted(final long floor) {
        assertEquals(1, adopted.size(), "Expected exactly one adoption, got " + adopted);
        assertEquals(floor, store.recoveryPromiseFloor());
    }

    private void assertNotAdopted() {
        assertTrue(adopted.isEmpty(), "Must not adopt yet: " + adopted);
        assertFalse(wal.countEntries(Wal.Entry.PromiseCeiling.class) > 0,
                "Nothing may be written until a floor is settled");
    }

    @Test
    @DisplayName("Enough witnesses, and the floor is their maximum")
    void enoughWitnessesGiveTheirMaximum() {
        final CeilingRecovery recovery = recovering();
        answer(recovery, PEER_A, 4096L, CeilingEvidence.WITNESS);
        assertNotAdopted();

        answer(recovery, PEER_B, 9001L, CeilingEvidence.WITNESS);
        assertAdopted(9001L);
    }

    @Test
    @DisplayName("One witness is not enough -- it may not be in the quorum that took the promise")
    void oneWitnessIsNotEnough() {
        // N=3 needs both peers: with one answer there is a quorum -- this node and the peer that
        // did not answer -- that the answer does not intersect.
        final CeilingRecovery recovery = recovering();
        answer(recovery, PEER_A, 4096L, CeilingEvidence.WITNESS);
        assertNotAdopted();
    }

    @Test
    @DisplayName("An amnesiac's honest zero is an answer, not a witness")
    void amnesiacZeroIsNotEvidence() {
        final CeilingRecovery recovery = recovering();
        answer(recovery, PEER_A, 4096L, CeilingEvidence.WITNESS);
        answer(recovery, PEER_B, 0L, CeilingEvidence.NONE);

        // Both peers answered, so the sweep is complete -- but it is not clean: a member reports a
        // bound above zero, so there is history, and one witness cannot bound what this node may
        // have promised.
        assertNotAdopted();
    }

    @Test
    @DisplayName("A clean sweep of every member is a floor of zero")
    void cleanSweepGivesZero() {
        final CeilingRecovery recovery = recovering();
        answer(recovery, PEER_A, 0L, CeilingEvidence.NONE);
        assertNotAdopted();

        answer(recovery, PEER_B, 0L, CeilingEvidence.NONE);
        assertAdopted(0L);
    }

    @Test
    @DisplayName("A member that swept clean before us passes the conclusion on")
    void theNoHistoryVerdictIsTransferable() {
        // The deadlock this exists to prevent: on a cluster starting from nothing, whichever member
        // concludes first starts serving and its own ballots carry its ceiling above zero. A sweep
        // can never come back clean again, and one witness is not enough -- so without the
        // conclusion travelling, every other member waits forever.
        final CeilingRecovery recovery = recovering();
        answer(recovery, PEER_A, 8192L, CeilingEvidence.NO_HISTORY);

        // Adopted on one answer, and the sweep it rests on is what justifies it: that sweep needed
        // an answer from this node too, so it was taken after this node lost whatever it lost.
        assertAdopted(8192L);
    }

    @Test
    @DisplayName("A node with nobody to ask has nobody who could hold its promises")
    void singleNodeClusterAdoptsImmediately() {
        final RecordingTransport alone = new RecordingTransport(1);
        final CeilingRecovery recovery = new CeilingRecovery(
                SELF, store, alone, loop, new CorrelationIdGenerator(SELF), NodeObserver.NONE,
                Duration.ofSeconds(1), detail -> { });
        recovery.afterReplay(false);
        recovery.start(adopted::add);

        assertAdopted(0L);
    }

    @Test
    @DisplayName("An answer to an earlier attempt counts as much as one to the latest")
    void lateAnswersStillCount() {
        // Both bounds only grow and every answer was read after this node lost its state, so an
        // answer stays true however late it lands. Requiring the latest attempt would tie progress
        // to the retry cadence: on a link whose round trip is longer than the interval, every answer
        // arrives after the next request has gone out, and a member restarting one at a time could
        // never assemble a full set.
        final CeilingRecovery recovery = recovering();
        final long firstAttempt = askedCorrelationId();
        answer(recovery, PEER_A, 4096L, CeilingEvidence.WITNESS);
        assertNotAdopted();

        recovery.retryNow();   // a second request goes out before A's peer answers the first
        assertTrue(askedCorrelationId() > firstAttempt, "A retry must carry a new correlation id");

        recovery.onResponse(new PeerMessage.CeilingResp(
                PEER_B, firstAttempt, 9001L, 0L, CeilingEvidence.WITNESS));
        assertAdopted(9001L);
    }

    @Test
    @DisplayName("A reply carrying an id this recovery never issued is not an answer")
    void foreignRepliesAreIgnored() {
        // A message from a previous incarnation of this node: its ids come from a different epoch.
        final CeilingRecovery recovery = recovering();
        answer(recovery, PEER_A, 4096L, CeilingEvidence.WITNESS);
        recovery.onResponse(new PeerMessage.CeilingResp(
                PEER_B, askedCorrelationId() + 1_000_000L, 9001L, 0L, CeilingEvidence.WITNESS));

        assertNotAdopted();
    }
}
