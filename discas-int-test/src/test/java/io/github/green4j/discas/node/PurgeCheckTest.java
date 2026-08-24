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
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The message half of tombstone collection: what a member answers when it is asked whether it can
 * still resurrect a value, and what it does with the decision.
 * <p>
 * Every answer here is about one member's own storage, and only two of them are safe: it holds the
 * tombstone durably, or it holds nothing for the key at all. Everything else blocks the collection,
 * because the replica that is merely behind is exactly the one anti-entropy would copy the old value
 * back off.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
@DisplayName("Purge check -- what a member answers before anyone collects")
class PurgeCheckTest {

    private static final NodeId SELF = NodeId.of("1");
    private static final NodeId COORDINATOR = NodeId.of("2");
    private static final List<NodeId> MEMBERS = List.of(SELF, COORDINATOR);
    private static final ClusterId CLUSTER = ClusterId.of("purge-check-cluster");
    private static final HashedBytes KEY = TestBytes.hashed("k");
    private static final Ballot VALUE_BALLOT = new Ballot(4L, COORDINATOR);
    private static final Ballot TOMBSTONE = new Ballot(7L, COORDINATOR);

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (int i = toClose.size() - 1; i >= 0; i--) {
            try {
                toClose.get(i).close();
            } catch (final Exception ignored) {
                // teardown of a node or loop that never fully started
            }
        }
    }

    private static LocalStore recovered(final Wal wal) {
        final LocalStore store = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        while (loader.loadBatch(1024)) {
            // drain
        }
        return store;
    }

    /**
     * A store holding the tombstone at {@link #TOMBSTONE}, over the value it suppresses -- with the
     * value durable and the tombstone not.
     * <p>
     * That is the ordinary state of a member seconds after a delete, and the only one where the
     * force behind a {@code HELD} answer changes the outcome: the crash that loses the tombstone
     * uncovers the value underneath rather than the whole key.
     */
    private static LocalStore withTombstone(final InMemoryWal wal) {
        final LocalStore store = recovered(wal);
        store.writeAccept(KEY, VALUE_BALLOT, TestBytes.hashed("v"), false);
        wal.force();
        store.writeAccept(KEY, TOMBSTONE, null, true);
        return store;
    }

    @Test
    @DisplayName("A key this member never heard of cannot resurrect anything")
    void absentKeyAnswersAbsent() {
        // The answer that keeps one replaced member from blocking every collection in the cluster
        // forever. It has to be distinguishable from "I hold a value and no tombstone", which is
        // the same silence-shaped ignorance from the outside and the opposite fact.
        final LocalStore store = recovered(new InMemoryWal());

        assertEquals(PurgeAnswer.ABSENT, store.purgeCheck(KEY, TOMBSTONE));
    }

    @Test
    @DisplayName("A member holding the value and no tombstone must block")
    void valueWithoutTombstoneBlocks() {
        final LocalStore store = recovered(new InMemoryWal());
        store.writeAccept(KEY, VALUE_BALLOT, TestBytes.hashed("v"), false);

        assertEquals(PurgeAnswer.RETAINED, store.purgeCheck(KEY, TOMBSTONE),
                "This is the replica anti-entropy would copy the old value back off");
    }

    @Test
    @DisplayName("A tombstone below the ballot asked about is a member that is behind")
    void tombstoneBelowTheBallotBlocks() {
        final LocalStore store = recovered(new InMemoryWal());
        store.writeAccept(KEY, new Ballot(3L, COORDINATOR), null, true);

        // Not the state the question is about: whatever happened at ballots 4 through 7, this
        // member has not seen it, so its agreement is about a different key state than the
        // coordinator's. A tombstone at or above the ballot suppresses the same value and does.
        assertEquals(PurgeAnswer.RETAINED, store.purgeCheck(KEY, TOMBSTONE));
        assertEquals(PurgeAnswer.HELD, withTombstone(new InMemoryWal())
                .purgeCheck(KEY, new Ballot(5L, COORDINATOR)));
    }

    @Test
    @DisplayName("HELD is answered only after a force, so it survives the crash it is a claim about")
    void heldForcesBeforeAnswering() {
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore store = withTombstone(wal);

        assertEquals(PurgeAnswer.HELD, store.purgeCheck(KEY, TOMBSTONE));

        // The power loss the answer has to survive. A tombstone is acknowledged before the log is
        // forced, so without the force in purgeCheck this tail is where it goes.
        wal.loseUnforcedTail();
        final LocalStore afterCrash = recovered(wal);
        assertNotNull(afterCrash.get(KEY), "The state behind a HELD answer must survive a restart");
        assertTrue(afterCrash.get(KEY).tombstone,
                "Coming back holding the value, with every other member purged, is the resurrection");
    }

    @Test
    @DisplayName("Verified by removal: without the force, the same crash brings the value back")
    void withoutTheForceTheValueComesBack() {
        // The same sequence with the answer taken from memory instead. This is what makes the test
        // above about the force rather than about a log that was durable anyway.
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore store = withTombstone(wal);
        assertTrue(store.get(KEY).tombstone, "In memory, the delete is done");

        wal.loseUnforcedTail();

        final LocalStore afterCrash = recovered(wal);
        assertEquals(VALUE_BALLOT, afterCrash.get(KEY).accepted,
                "The deleted value is what is underneath a lost tombstone");
        assertEquals(TestBytes.hashed("v"), afterCrash.get(KEY).value,
                "And with everyone else purged, nothing is left to out-vote it");
    }

    @Test
    @DisplayName("A member that cannot complete a force cannot claim its disk holds anything")
    void failedForceBlocks() {
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore store = withTombstone(wal);
        wal.failForce();

        assertEquals(PurgeAnswer.RETAINED, store.purgeCheck(KEY, TOMBSTONE),
                "Blocking costs a delayed sweep; answering costs the deleted value coming back");
    }

    @Test
    @DisplayName("A node answers checks over the wire and applies the decision it is sent")
    void aNodeAnswersAndApplies() throws Exception {
        final InMemoryWal wal = new InMemoryWal();
        // Seeded before the node opens the log, so it replays a member that already holds the
        // tombstone -- there is no coordinator yet to have put one there.
        wal.append(new Wal.Entry.Accept(KEY, VALUE_BALLOT, TestBytes.hashed("v"), false));
        wal.append(new Wal.Entry.Accept(KEY, TOMBSTONE, null, true));
        wal.force();

        final BlockingQueue<PeerMessage.PurgeCheckResp> answers = new LinkedBlockingQueue<>();
        final EventLoop coordinatorLoop = start(new EventLoop("purge-check-coordinator"));
        final InProcessPeerTransport coordinator = new InProcessPeerTransport(
                COORDINATOR, MEMBERS.size(), coordinatorLoop, InMemoryMembers.ofNodes(MEMBERS));
        // Anti-entropy is running on the member, so its digest requests arrive here too. Only the
        // answers are the subject.
        coordinator.register(message -> {
            if (message instanceof PeerMessage.PurgeCheckResp) {
                answers.add((PeerMessage.PurgeCheckResp) message);
            }
        });

        // Not started here, unlike the coordinator's: the node starts the loop it is given.
        final EventLoop memberLoop = new EventLoop("purge-check-member");
        toClose.add(memberLoop::shutdown);
        final DisCasNode member = new DisCasNode(
                NodeConfig.builder()
                        .nodeId(SELF)
                        .clusterId(CLUSTER)
                        .clusterSize(MEMBERS.size())
                        .build(),
                wal, memberLoop,
                new InProcessPeerTransport(SELF, MEMBERS.size(), memberLoop,
                        InMemoryMembers.ofNodes(MEMBERS)));
        toClose.add(member);
        member.start();
        TestAwait.until("the member serves", Duration.ofSeconds(10), () -> {
            if (member.healthSource().state() != NodeState.SERVING) {
                throw new IllegalStateException("Still " + member.healthSource().state());
            }
        });

        coordinator.send(SELF, new PeerMessage.PurgeCheckReq(COORDINATOR, 1L, KEY, TOMBSTONE));
        assertEquals(PurgeAnswer.HELD, nextAnswer(answers).answer());

        coordinator.send(SELF, new PeerMessage.PurgeCheckReq(
                COORDINATOR, 2L, TestBytes.hashed("never-existed"), TOMBSTONE));
        assertEquals(PurgeAnswer.ABSENT, nextAnswer(answers).answer());

        // The decision, which is a broadcast and gets no answer: what it did is visible in the log
        // it wrote, and only there until a coordinator exists to ask again.
        coordinator.send(SELF, new PeerMessage.PurgeReq(COORDINATOR, 3L, KEY, TOMBSTONE));
        TestAwait.until("the purge is applied", Duration.ofSeconds(10), () -> {
            if (wal.countEntries(Wal.Entry.Purge.class) != 1) {
                throw new IllegalStateException("No purge record yet");
            }
        });

        coordinator.send(SELF, new PeerMessage.PurgeCheckReq(COORDINATOR, 4L, KEY, TOMBSTONE));
        assertEquals(PurgeAnswer.ABSENT, nextAnswer(answers).answer(),
                "A purged key is absent, and answers as a key that never existed -- which is what "
                        + "lets a cluster mid-sweep finish rather than block on itself");
    }

    private EventLoop start(final EventLoop loop) {
        toClose.add(loop::shutdown);
        loop.start();
        return loop;
    }

    private static PeerMessage.PurgeCheckResp nextAnswer(
            final BlockingQueue<PeerMessage.PurgeCheckResp> answers) throws InterruptedException {
        final PeerMessage.PurgeCheckResp answer = answers.poll(10, TimeUnit.SECONDS);
        assertNotNull(answer, "The member must answer a purge check");
        return answer;
    }
}
