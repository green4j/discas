/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When a tombstone may be collected, and what happens when it may not. Drives
 * {@link TombstoneCollector} with hand-made answers, so each case is one exchange with no cluster,
 * no timing and no race.
 * <p>
 * The matrix is the point: every member held it, one member was replaced, one member never answered,
 * one member is merely behind. Only the last two block, and they block for the same reason -- the
 * replica that can still resurrect the value is exactly the one anti-entropy would copy it back off,
 * and a member that says nothing might be that replica.
 * <p>
 * {@code PurgeCheckTest} covers the other end -- one member's own answer about its own storage.
 * This one says what the decision <em>is</em>.
 */
@DisplayName("Purge decision -- when every member permits, and what happens when one does not")
class PurgeDecisionTest {

    private static final NodeId SELF = NodeId.of("self");
    private static final NodeId PEER_A = NodeId.of("a");
    private static final NodeId PEER_B = NodeId.of("b");
    private static final NodeId STRANGER = NodeId.of("stranger");
    private static final HashedBytes KEY = TestBytes.hashed("k");
    private static final Ballot VALUE_BALLOT = new Ballot(4L, SELF);
    private static final Ballot TOMBSTONE = new Ballot(7L, SELF);

    /** Records what was sent and answers nothing on its own. */
    private static final class RecordingTransport implements PeerTransport {
        private final List<PeerMessage> sent = new ArrayList<>();
        private final List<NodeId> peers;
        private final int clusterSize;

        private RecordingTransport(final int clusterSize, final NodeId... peers) {
            this.clusterSize = clusterSize;
            this.peers = new ArrayList<>(List.of(peers));
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

    /**
     * Deliberately never started: {@code schedule} only queues, so the sweep's deadline never fires
     * and every case is driven entirely by the answers it is given. {@code expireNow()} is the
     * deadline, without the clock.
     */
    private final EventLoop loop = new EventLoop("purge-decision");
    private final RecordingTransport transport = new RecordingTransport(3, PEER_A, PEER_B);
    private final InMemoryWal wal = new InMemoryWal();
    /**
     * What each sweep decided, and -- when it decided nothing -- who stopped it. The blockers travel
     * with the outcome rather than through a separate report, so a caller that has to act on them
     * (the sweeper, and through it item 7's operator surface) gets them where it gets the decision.
     */
    private final List<TombstoneCollector.Outcome> outcomes = new ArrayList<>();

    private LocalStore store;

    private static LocalStore recovered(final Wal wal) {
        final LocalStore store = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        while (loader.loadBatch(1024)) {
            // drain
        }
        return store;
    }

    /** This node in the ordinary case: it holds the tombstone the sweep is about, durably. */
    private TombstoneCollector sweeping() {
        store = recovered(wal);
        store.writeAccept(KEY, VALUE_BALLOT, TestBytes.hashed("v"), false);
        store.writeAccept(KEY, TOMBSTONE, null, true);
        wal.force();
        return collectorOver(store, transport);
    }

    private TombstoneCollector collectorOver(final LocalStore store,
                                             final RecordingTransport transport) {
        return new TombstoneCollector(SELF, store, transport, loop,
                new CorrelationIdGenerator(SELF), Duration.ofSeconds(10));
    }

    private boolean sweep(final TombstoneCollector collector) {
        return collector.collect(KEY, TOMBSTONE, outcomes::add);
    }

    private void answer(final TombstoneCollector collector, final NodeId from,
                        final PurgeAnswer answer) {
        collector.onCheckResp(new PeerMessage.PurgeCheckResp(from, askedCorrelationId(), answer));
    }

    private long askedCorrelationId() {
        for (final PeerMessage message : transport.sent) {
            if (message instanceof PeerMessage.PurgeCheckReq) {
                return message.correlationId();
            }
        }
        throw new AssertionError("Nothing was asked: " + transport.sent);
    }

    private List<PeerMessage.PurgeReq> decisionsSent() {
        final List<PeerMessage.PurgeReq> decisions = new ArrayList<>();
        for (final PeerMessage message : transport.sent) {
            if (message instanceof PeerMessage.PurgeReq) {
                decisions.add((PeerMessage.PurgeReq) message);
            }
        }
        return decisions;
    }

    private void assertCollected() {
        assertEquals(1, outcomes.size(), "Expected exactly one decision, got " + outcomes);
        assertTrue(outcomes.get(0).collected(), "The sweep must have collected: " + outcomes.get(0));
        assertTrue(outcomes.get(0).blockers().isEmpty(),
                "Nothing blocked: " + outcomes.get(0).blockers());
        assertEquals(2, decisionsSent().size(), "Every member is told, and told once");
        assertEquals(KEY, decisionsSent().get(0).key());
        assertEquals(TOMBSTONE, decisionsSent().get(0).tombstoneBallot(),
                "The decision names the state it is about, so a key rewritten above it is left alone");
        assertNull(store.get(KEY), "The coordinator applies its own decision like any other member");
        assertEquals(1, wal.countEntries(Wal.Entry.Purge.class),
                "And writes the record that stops replay bringing the key back");
    }

    private void assertBlockedBy(final NodeId... expected) {
        assertEquals(1, outcomes.size(), "Expected exactly one decision, got " + outcomes);
        assertFalse(outcomes.get(0).collected(), "Nothing may be collected: " + outcomes.get(0));
        assertTrue(decisionsSent().isEmpty(), "No decision may go out: " + decisionsSent());
        assertNotNull(store.get(KEY), "The tombstone stays exactly where it was");
        assertEquals(0, wal.countEntries(Wal.Entry.Purge.class));
        final List<NodeId> named = new ArrayList<>();
        for (final TombstoneSweep.Blocker blocker : blockers()) {
            named.add(blocker.peer());
        }
        assertEquals(List.of(expected), named, "Who held the sweep up: " + blockers());
    }

    /** The blockers of the one decision this case took. */
    private List<TombstoneSweep.Blocker> blockers() {
        return outcomes.get(outcomes.size() - 1).blockers();
    }

    @Test
    @DisplayName("Every member holds the tombstone -- nothing can resurrect the value, so it goes")
    void everyMemberHeldCollects() {
        final TombstoneCollector collector = sweeping();
        assertTrue(sweep(collector));

        assertEquals(2, transport.sent.size(), "Every member is asked, not a quorum");
        assertTrue(outcomes.isEmpty(), "Nothing is decided while a member has not answered");

        answer(collector, PEER_A, PurgeAnswer.HELD);
        assertTrue(outcomes.isEmpty());

        answer(collector, PEER_B, PurgeAnswer.HELD);
        assertCollected();
    }

    @Test
    @DisplayName("A member that holds nothing at all cannot resurrect anything either")
    void oneAbsentStillCollects() {
        // The replaced member. Calling this a refusal would block every collection in the cluster
        // for as long as that member lives, which is why ABSENT is not RETAINED.
        final TombstoneCollector collector = sweeping();
        sweep(collector);
        answer(collector, PEER_A, PurgeAnswer.ABSENT);
        answer(collector, PEER_B, PurgeAnswer.HELD);

        assertCollected();
    }

    @Test
    @DisplayName("A member holding the value with no tombstone is the case that must block")
    void oneRetainedBlocks() {
        final TombstoneCollector collector = sweeping();
        sweep(collector);
        answer(collector, PEER_A, PurgeAnswer.RETAINED);
        answer(collector, PEER_B, PurgeAnswer.HELD);

        // Decided at the deadline rather than on the refusal: waiting costs a sweep that was not
        // going to collect anyway, and buys the whole list of members holding it up.
        assertTrue(outcomes.isEmpty(), "A blocked sweep waits for everyone before it says who");
        collector.expireNow();

        assertBlockedBy(PEER_A);
        assertEquals(PurgeAnswer.RETAINED, blockers().get(0).answer(),
                "A member that is merely behind needs nothing done to it -- anti-entropy ends that");
    }

    @Test
    @DisplayName("A member that never answers blocks exactly like one that refuses")
    void oneSilentBlocks() {
        final TombstoneCollector collector = sweeping();
        sweep(collector);
        answer(collector, PEER_A, PurgeAnswer.HELD);
        collector.expireNow();

        assertBlockedBy(PEER_B);
        assertNull(blockers().get(0).answer(),
                "Silence is a block with no answer behind it: that member may be the replica that "
                        + "still holds the value");
    }

    @Test
    @DisplayName("The coordinator answers as a member, and blocks itself when it cannot claim its disk")
    void theCoordinatorIsAMemberToo() {
        // This node holds the value and no tombstone -- the same state that blocks anywhere else. A
        // coordinator exempting itself would purge the key everywhere and keep the value here, with
        // no tombstone left in the cluster to out-vote it.
        store = recovered(wal);
        store.writeAccept(KEY, VALUE_BALLOT, TestBytes.hashed("v"), false);
        final TombstoneCollector collector = collectorOver(store, transport);

        sweep(collector);
        answer(collector, PEER_A, PurgeAnswer.HELD);
        answer(collector, PEER_B, PurgeAnswer.HELD);
        collector.expireNow();

        assertBlockedBy(SELF);
    }

    @Test
    @DisplayName("An answer from outside the membership settles nothing about the member it names")
    void answersAreCountedByIdentity() {
        // Identities are counted, not answers. Counting answers would let a duplicate, or a member
        // the sweep never asked, stand in for the one that has not replied -- and the member that
        // has not replied is the whole point of asking all N.
        final TombstoneCollector collector = sweeping();
        sweep(collector);
        answer(collector, PEER_A, PurgeAnswer.HELD);
        answer(collector, STRANGER, PurgeAnswer.HELD);
        answer(collector, PEER_A, PurgeAnswer.HELD);

        assertTrue(outcomes.isEmpty(), "Two of three members have answered");
        collector.expireNow();
        assertBlockedBy(PEER_B);
    }

    @Test
    @DisplayName("An answer to a sweep that is over belongs to nothing")
    void answersToAFinishedSweepAreDropped() {
        final TombstoneCollector collector = sweeping();
        sweep(collector);
        final long asked = askedCorrelationId();
        answer(collector, PEER_A, PurgeAnswer.HELD);
        answer(collector, PEER_B, PurgeAnswer.HELD);
        assertCollected();

        collector.onCheckResp(new PeerMessage.PurgeCheckResp(PEER_A, asked, PurgeAnswer.HELD));
        assertEquals(1, outcomes.size(), "A sweep is decided once");
        assertEquals(2, decisionsSent().size(), "And broadcast once");
    }

    @Test
    @DisplayName("One candidate at a time: a sweep in flight refuses to start another")
    void oneSweepAtATime() {
        final TombstoneCollector collector = sweeping();
        assertTrue(sweep(collector));
        assertFalse(collector.collect(TestBytes.hashed("other"), TOMBSTONE, outcomes::add),
                "A refused sweep never completes, so its caller is not left waiting on a callback");
        assertEquals(2, transport.sent.size(), "And nothing was asked for the second candidate");

        answer(collector, PEER_A, PurgeAnswer.HELD);
        answer(collector, PEER_B, PurgeAnswer.HELD);
        assertCollected();
        assertTrue(sweep(collector), "The next candidate may start as soon as this one is decided");
    }

    @Test
    @DisplayName("A node with nobody to ask decides on its own answer")
    void singleNodeClusterDecidesAlone() {
        final RecordingTransport alone = new RecordingTransport(1);
        store = recovered(wal);
        store.writeAccept(KEY, TOMBSTONE, null, true);
        wal.force();
        final TombstoneCollector collector = collectorOver(store, alone);

        assertTrue(collector.collect(KEY, TOMBSTONE, outcomes::add));

        assertEquals(1, outcomes.size());
        assertTrue(outcomes.get(0).collected());
        assertNull(store.get(KEY));
        assertTrue(alone.sent.isEmpty(), "There is nobody to ask and nobody to tell");
    }

    @Test
    @DisplayName("A node on its way down decides nothing")
    void closingEndsTheSweepWithoutDeciding() {
        final TombstoneCollector collector = sweeping();
        sweep(collector);
        answer(collector, PEER_A, PurgeAnswer.HELD);
        collector.close();

        answer(collector, PEER_B, PurgeAnswer.HELD);
        assertTrue(outcomes.isEmpty(), "No completion runs, so no sweeper picks a next candidate");
        assertTrue(decisionsSent().isEmpty(),
                "And a member leaving the cluster does not broadcast a decision on its way out");
        assertFalse(sweep(collector), "A closed collector starts nothing");
    }
}
