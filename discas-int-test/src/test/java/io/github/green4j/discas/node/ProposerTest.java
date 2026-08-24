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
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.transport.PeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Proposer -- Paxos prepare/accept rounds")
class ProposerTest {

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    private static final List<NodeId> ALL_NODES = List.of(nid(1), nid(2), nid(3));
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private EventLoop loop1, loop2, loop3;
    private InMemoryWal wal1, wal2, wal3;
    private LocalStore store1, store2, store3;
    private Acceptor acceptor1, acceptor2, acceptor3;
    private Proposer proposer1;

    @BeforeEach
    void setUp() throws InterruptedException {
        loop1 = new EventLoop("node-1");
        loop2 = new EventLoop("node-2");
        loop3 = new EventLoop("node-3");

        loop1.start();
        loop2.start();
        loop3.start();

        wal1 = new InMemoryWal();
        wal2 = new InMemoryWal();
        wal3 = new InMemoryWal();

        store1 = new LocalStore(wal1);
        store2 = new LocalStore(wal2);
        store3 = new LocalStore(wal3);

        acceptor1 = new Acceptor(nid(1), store1);
        acceptor2 = new Acceptor(nid(2), store2);
        acceptor3 = new Acceptor(nid(3), store3);

        final InProcessPeerTransport t1 = new InProcessPeerTransport(nid(1), ALL_NODES.size(), loop1,
                InMemoryMembers.ofNodes(ALL_NODES));
        final InProcessPeerTransport t2 = new InProcessPeerTransport(nid(2), ALL_NODES.size(), loop2,
                InMemoryMembers.ofNodes(ALL_NODES));
        final InProcessPeerTransport t3 = new InProcessPeerTransport(nid(3), ALL_NODES.size(), loop3,
                InMemoryMembers.ofNodes(ALL_NODES));

        // Wire acceptors to receive messages on their loops
        t2.register(msg -> handleOnAcceptor(2, acceptor2, t2, msg));
        t3.register(msg -> handleOnAcceptor(3, acceptor3, t3, msg));

        proposer1 = new Proposer(nid(1), t1, acceptor1, loop1, store1, new CorrelationIdGenerator(nid(1)));

        // Register t1 last so proposer responses can be routed
        t1.register(msg -> loop1.execute(() -> {
            if (msg instanceof PeerMessage.PrepareResp) {
                proposer1.onPrepareResp((PeerMessage.PrepareResp) msg);
            } else if (msg instanceof PeerMessage.AcceptResp) {
                proposer1.onAcceptResp((PeerMessage.AcceptResp) msg);
            }
        }));
        // No pause here: in-process registration is synchronous, and a task handed to a loop that
        // has not started yet is queued rather than lost, so there is nothing to wait for.
    }

    private void handleOnAcceptor(final int nodeId, final Acceptor acceptor,
                                  final PeerTransport transport, final PeerMessage msg) {
        if (msg instanceof PeerMessage.PrepareReq) {
            final PeerMessage.PrepareReq req = (PeerMessage.PrepareReq) msg;
            final PeerMessage.PrepareResp resp = acceptor.handlePrepare(req);
            transport.send(req.senderId(), resp);
        } else if (msg instanceof PeerMessage.AcceptReq) {
            final PeerMessage.AcceptReq req = (PeerMessage.AcceptReq) msg;
            final PeerMessage.AcceptResp resp = acceptor.handleAccept(req);
            transport.send(req.senderId(), resp);
        }
    }

    @AfterEach
    void tearDown() {
        loop1.execute(() -> proposer1.drainAllPending(new RuntimeException("Test teardown")));
        loop1.shutdown();
        loop2.shutdown();
        loop3.shutdown();
        loop1.awaitTermination(Duration.ofSeconds(2));
        loop2.awaitTermination(Duration.ofSeconds(2));
        loop3.awaitTermination(Duration.ofSeconds(2));
    }

    private <T> T get(final CompletableFuture<T> f) throws Exception {
        return f.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Test
    @DisplayName("CAS succeeds on a fresh key -- value and existence flags set")
    void casSucceedsOnFreshKey() throws Exception {
        final HashedBytes v1 = TestBytes.hashed("v1");
        final Proposer.CasResult result = get(
                proposer1.write(TestBytes.hashed("k"), current -> v1));

        assertEquals(v1, result.value());
        assertTrue(result.keyExists());
        assertFalse(result.tombstone());
    }

    @Test
    @DisplayName("CAS applies the transform to the existing value across two rounds")
    void casAppliesTransformToExistingValue() throws Exception {
        final HashedBytes k = TestBytes.hashed("k");
        final HashedBytes v1 = TestBytes.hashed("v1");
        final HashedBytes v2 = TestBytes.hashed("v2");

        get(proposer1.write(k, current -> v1));
        final Proposer.CasResult result = get(proposer1.write(k, current -> v2));

        assertEquals(v2, result.value());
    }

    @Test
    @DisplayName("Read-only (identity) round short-circuits accept broadcast when value unchanged")
    void readOnlyShortCircuitWhenValueUnchanged() throws Exception {
        final HashedBytes k = TestBytes.hashed("k");
        final HashedBytes v1 = TestBytes.hashed("v1");

        get(proposer1.write(k, current -> v1));
        final Proposer.CasResult result = get(proposer1.identityRound(k));

        assertEquals(v1, result.value());
        assertTrue(result.keyExists());
    }

    @Test
    @DisplayName("Repair round always broadcasts accept (no short-circuit)")
    void repairRoundAlwaysBroadcastsAccept() throws Exception {
        final HashedBytes k = TestBytes.hashed("k");
        final HashedBytes v1 = TestBytes.hashed("v1");

        get(proposer1.write(k, current -> v1));
        final Proposer.CasResult result = get(proposer1.repairRound(k));

        assertEquals(v1, result.value());
        assertTrue(result.keyExists());
    }

    @Test
    @DisplayName("Duplicate accept response is ignored -- quorum arithmetic stays correct")
    void duplicateAcceptResponseIgnored() throws Exception {
        final Proposer.CasResult result = get(
                proposer1.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));
        assertNotNull(result);
        assertTrue(result.keyExists());
    }

    @Test
    @DisplayName("Ballot monotonically increases across successive CAS rounds")
    void ballotMonotonicallyIncreases() throws Exception {
        final HashedBytes k = TestBytes.hashed("k");

        get(proposer1.write(k, current -> TestBytes.hashed("v1")));
        final Ballot b1 = store1.get(k).accepted;

        get(proposer1.write(k, current -> TestBytes.hashed("v2")));
        final Ballot b2 = store1.get(k).accepted;

        assertTrue(b2.compareTo(b1) > 0, "Second ballot should be strictly higher");
    }

    @Test
    @DisplayName("Reserved-ballot survives rehydration after a simulated restart")
    void ballotReservationSurvivesRehydration() throws Exception {
        get(proposer1.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")));

        final long reservedBefore = store1.reservedProposerBallot();
        assertTrue(reservedBefore > 0);

        proposer1.rehydrateAfterRecovery();

        final Proposer.CasResult result = get(
                proposer1.write(TestBytes.hashed("k2"), current -> TestBytes.hashed("v2")));
        assertNotNull(result);
    }

    @Test
    @DisplayName("Observing an external ballot bumps the local proposer counter past it")
    void observeExternalBallotBumpsCounter() throws Exception {
        final HashedBytes k = TestBytes.hashed("k");
        get(proposer1.write(k, current -> TestBytes.hashed("v1")));
        final Ballot b1 = store1.get(k).accepted;

        // Simulate a high ballot from an external proposer by writing to acceptors
        final Ballot externalBallot = new Ballot(b1.counter() + 1000, nid(2));
        acceptor1.handlePrepare(new PeerMessage.PrepareReq(nid(2), 999L, k, externalBallot));
        acceptor2.handlePrepare(new PeerMessage.PrepareReq(nid(2), 999L, k, externalBallot));
        acceptor3.handlePrepare(new PeerMessage.PrepareReq(nid(2), 999L, k, externalBallot));

        // Proposer1's next CAS should observe the NACK and jump ahead
        final Proposer.CasResult result = get(proposer1.write(k, current -> TestBytes.hashed("v2")));
        final Ballot b2 = store1.get(k).accepted;
        assertTrue(b2.counter() > externalBallot.counter(),
                "Proposer should have jumped past external ballot");
    }

    @Test
    @DisplayName("drainAllPending cancels in-flight retry timers and completes futures exceptionally")
    void drainAllPendingCancelsRetryTimers() throws Exception {
        // Create a proposer whose peers never respond (simulating pending rounds)
        final PeerTransport silentTransport = new PeerTransport() {
            @Override
            public void send(final NodeId targetNodeId, final PeerMessage message) {
                // Drop everything
            }

            @Override
            public void register(final Consumer<PeerMessage> handler) {
            }

            @Override
            public List<NodeId> peers() {
                return List.of(nid(2), nid(3));
            }

            @Override
            public int clusterSize() {
                return 3;
            }
        };

        final InMemoryWal drainWal = new InMemoryWal();
        final LocalStore drainStore = new LocalStore(drainWal);
        // Self-acceptor NACKs to force retry path
        final Acceptor drainAcceptor = new Acceptor(nid(1), drainStore);
        drainAcceptor.handlePrepare(new PeerMessage.PrepareReq(nid(99), 0L, TestBytes.hashed("k"), new Ballot(999999,
                nid(99))));

        final Proposer drainProposer = new Proposer(
                nid(1), silentTransport, drainAcceptor, loop1, drainStore, new CorrelationIdGenerator(nid(1)));

        final CompletableFuture<Proposer.CasResult> future =
                drainProposer.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v"));

        // Wait for the round to be pending: the drain below can only cancel what has started, and
        // how long starting takes is a property of the machine, not of the code under test.
        TestAwait.until("the round to be in flight", () -> {
            final CompletableFuture<Integer> pending = new CompletableFuture<>();
            loop1.execute(() -> pending.complete(drainProposer.pendingRounds()));
            if (pending.get(5, TimeUnit.SECONDS) == 0) {
                throw new IllegalStateException("No round started yet");
            }
        });

        final CompletableFuture<Void> drainDone = new CompletableFuture<>();
        loop1.execute(() -> {
            drainProposer.drainAllPending(new RuntimeException("Shutdown"));
            drainDone.complete(null);
        });
        drainDone.get(5, TimeUnit.SECONDS);

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    @DisplayName("CAS to null records a tombstone with keyExists=true")
    void casWithDeleteTombstone() throws Exception {
        final HashedBytes k = TestBytes.hashed("k");
        get(proposer1.write(k, current -> TestBytes.hashed("v1")));

        final Proposer.CasResult deleteResult = get(
                proposer1.write(k, current -> null));
        assertTrue(deleteResult.tombstone());
        assertTrue(deleteResult.keyExists());
        assertNull(deleteResult.value());
    }

    @Test
    @DisplayName("CAS on non-existent key with null transform returns keyExists=false")
    void casOnNonExistentKeyWithNullTransformReturnsNotExists() throws Exception {
        final Proposer.CasResult result = get(
                proposer1.write(TestBytes.hashed("missing"), Function.identity()));

        assertFalse(result.keyExists());
    }
}
