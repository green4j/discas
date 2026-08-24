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
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.transport.PeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@DisplayName("AntiEntropy -- periodic divergence repair")
class AntiEntropyTest {

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    private static final List<NodeId> ALL_NODES = List.of(nid(1), nid(2), nid(3));
    private EventLoop loop1, loop2, loop3;
    private LocalStore store1, store2, store3;
    private Proposer proposer1;
    private AntiEntropy antiEntropy1;
    private AtomicInteger cyclesStarted;
    private InProcessPeerTransport t1, t2, t3;

    @BeforeEach
    void setUp() throws InterruptedException {
        loop1 = new EventLoop("ae-node-1");
        loop2 = new EventLoop("ae-node-2");
        loop3 = new EventLoop("ae-node-3");

        loop1.start();
        loop2.start();
        loop3.start();

        final InMemoryWal wal1 = new InMemoryWal();
        final InMemoryWal wal2 = new InMemoryWal();
        final InMemoryWal wal3 = new InMemoryWal();

        store1 = new LocalStore(wal1);
        store2 = new LocalStore(wal2);
        store3 = new LocalStore(wal3);

        final Acceptor acc1 = new Acceptor(nid(1), store1);
        final Acceptor acc2 = new Acceptor(nid(2), store2);
        final Acceptor acc3 = new Acceptor(nid(3), store3);

        t1 = new InProcessPeerTransport(nid(1), ALL_NODES.size(), loop1, InMemoryMembers.ofNodes(ALL_NODES));
        t2 = new InProcessPeerTransport(nid(2), ALL_NODES.size(), loop2, InMemoryMembers.ofNodes(ALL_NODES));
        t3 = new InProcessPeerTransport(nid(3), ALL_NODES.size(), loop3, InMemoryMembers.ofNodes(ALL_NODES));

        final CorrelationIdGenerator corrGen1 = new CorrelationIdGenerator(nid(1));

        proposer1 = new Proposer(nid(1), t1, acc1, loop1, store1, corrGen1);
        // Count cycle starts so tests can assert on cycle liveness (and its absence after
        // shutdown) rather than just sleeping and hoping.
        cyclesStarted = new AtomicInteger();
        antiEntropy1 = new AntiEntropy(nid(1), store1, proposer1, t1, loop1, corrGen1,
                new NodeObserver() {
                    @Override
                    public void repairCycleStarted() {
                        cyclesStarted.incrementAndGet();
                    }
                });

        // Wire node 2 as a full acceptor + anti-entropy responder
        final AntiEntropy ae2 = new AntiEntropy(nid(2), store2, null, t2, loop2, new CorrelationIdGenerator(nid(2)));
        t2.register(msg -> loop2.execute(() -> dispatchAll(2, acc2, store2, ae2, t2, msg)));

        final AntiEntropy ae3 = new AntiEntropy(nid(3), store3, null, t3, loop3, new CorrelationIdGenerator(nid(3)));
        t3.register(msg -> loop3.execute(() -> dispatchAll(3, acc3, store3, ae3, t3, msg)));

        // Wire proposer responses back
        t1.register(msg -> loop1.execute(() -> {
            if (msg instanceof PeerMessage.PrepareResp) {
                proposer1.onPrepareResp((PeerMessage.PrepareResp) msg);
            } else if (msg instanceof PeerMessage.AcceptResp) {
                proposer1.onAcceptResp((PeerMessage.AcceptResp) msg);
            } else if (msg instanceof PeerMessage.DigestResp) {
                antiEntropy1.onDigestResp((PeerMessage.DigestResp) msg);
            } else if (msg instanceof PeerMessage.KeysResp) {
                antiEntropy1.onKeysResp((PeerMessage.KeysResp) msg);
            }
        }));
        // No pause here: in-process registration is synchronous, and a task handed to a loop that
        // has not started yet is queued rather than lost, so there is nothing to wait for.
    }

    private void dispatchAll(final int nodeId, final Acceptor acc, final LocalStore store,
                             final AntiEntropy ae, final PeerTransport transport, final PeerMessage msg) {
        if (msg instanceof PeerMessage.PrepareReq) {
            final PeerMessage.PrepareReq req = (PeerMessage.PrepareReq) msg;
            transport.send(req.senderId(), acc.handlePrepare(req));
        } else if (msg instanceof PeerMessage.AcceptReq) {
            final PeerMessage.AcceptReq req = (PeerMessage.AcceptReq) msg;
            transport.send(req.senderId(), acc.handleAccept(req));
        } else if (msg instanceof PeerMessage.DigestReq) {
            ae.handleDigestReq((PeerMessage.DigestReq) msg);
        } else if (msg instanceof PeerMessage.KeysReq) {
            ae.handleKeysReq((PeerMessage.KeysReq) msg);
        }
    }

    @AfterEach
    void tearDown() {
        antiEntropy1.close();
        loop1.execute(() -> proposer1.drainAllPending(new RuntimeException("Test teardown")));
        loop1.shutdown();
        loop2.shutdown();
        loop3.shutdown();
        loop1.awaitTermination(Duration.ofSeconds(2));
        loop2.awaitTermination(Duration.ofSeconds(2));
        loop3.awaitTermination(Duration.ofSeconds(2));
    }

    /** Generous: these bound a poll that returns the moment the state arrives, so it is never paid. */
    private static final Duration SETTLE_BUDGET = Duration.ofSeconds(20);

    @FunctionalInterface
    private interface Condition {
        boolean holds() throws Exception;
    }

    /**
     * Polls until {@code what} becomes true, rather than sleeping a fixed span and hoping.
     * <p>
     * A fixed sleep is wrong in both directions: too short and the test is flaky on a loaded
     * machine, too long and every run pays the worst case even when the state arrived at once.
     * This returns as soon as it does, and says what it was still waiting for if it never does.
     */
    private static void awaitTrue(final String what, final Condition condition) throws Exception {
        awaitTrue(() -> what, condition);
    }

    /** {@code what} is built only on failure, so it may inspect live state. */
    private static void awaitTrue(final Description what, final Condition condition) throws Exception {
        final long deadline = System.nanoTime() + SETTLE_BUDGET.toNanos();
        do {
            if (condition.holds()) {
                return;
            }
            Thread.sleep(20L);
        } while (System.nanoTime() - deadline < 0);
        fail("Timed out after " + SETTLE_BUDGET + " waiting for " + what.get());
    }

    @FunctionalInterface
    private interface Description {
        String get() throws Exception;
    }

    private static String describe(final List<Snapshot> all) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < all.size(); i++) {
            sb.append(i > 0 ? ", " : "").append("node").append(i + 1)
                    .append("=ballot ").append(all.get(i).ballotCounter)
                    .append('/').append(all.get(i).value);
        }
        return sb.append(']').toString();
    }

    /** Runs {@code body} on {@code loop} -- stores are loop-confined and must not be read directly. */
    private static <T> T onLoop(final EventLoop loop, final Supplier<T> body) throws Exception {
        final CompletableFuture<T> result = new CompletableFuture<>();
        loop.execute(() -> {
            try {
                result.complete(body.get());
            } catch (final RuntimeException e) {
                result.completeExceptionally(e);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }

    private static HashedBytes acceptedValue(final EventLoop loop, final LocalStore store, final HashedBytes key)
            throws Exception {
        return onLoop(loop, () -> {
            final KeyState state = store.get(key);
            return state == null ? null : state.value;
        });
    }

    /** How many of the three replicas hold {@code value} for {@code key} in their own state. */
    private int replicasHolding(final HashedBytes key, final HashedBytes value) throws Exception {
        int held = 0;
        if (value.equals(acceptedValue(loop1, store1, key))) {
            held++;
        }
        if (value.equals(acceptedValue(loop2, store2, key))) {
            held++;
        }
        if (value.equals(acceptedValue(loop3, store3, key))) {
            held++;
        }
        return held;
    }

    /** Starts a repair cycle, returning once it has actually begun. */
    private void startRepairCycle() throws Exception {
        final int before = cyclesStarted.get();
        awaitTrue("a repair cycle to start", () -> {
            loop1.execute(antiEntropy1::runRepairCycle);
            return cyclesStarted.get() > before;
        });
    }

    /** One replica's committed state for a key, copied off the loop that owns it. */
    private static final class Snapshot {
        final long ballotCounter;
        final HashedBytes value;

        Snapshot(final long ballotCounter, final HashedBytes value) {
            this.ballotCounter = ballotCounter;
            this.value = value;
        }
    }

    private static Snapshot snapshotOn(final EventLoop loop, final LocalStore store, final HashedBytes key)
            throws Exception {
        return onLoop(loop, () -> {
            final KeyState state = store.get(key);
            if (state == null || state.accepted == null) {
                return new Snapshot(-1L, null);
            }
            return new Snapshot(state.accepted.counter(), state.value);
        });
    }

    private List<Snapshot> snapshots(final HashedBytes key) throws Exception {
        return List.of(snapshotOn(loop1, store1, key),
                snapshotOn(loop2, store2, key),
                snapshotOn(loop3, store3, key));
    }

    /** Replicas whose accepted ballot is above {@code counter} -- i.e. that took a later round. */
    private int replicasAbove(final HashedBytes key, final long counter) throws Exception {
        int above = 0;
        final List<Snapshot> all = snapshots(key);
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i).ballotCounter > counter) {
                above++;
            }
        }
        return above;
    }

    /**
     * Waits until the in-flight repair cycle has finished.
     * <p>
     * There is no completion callback to observe, but there is an equivalent: {@code runRepairCycle}
     * returns early while a cycle is active, and {@code repairCycleStarted} fires only when one
     * actually begins -- so a cycle count that rises is proof the previous cycle drained. That makes
     * "is it done yet" answerable exactly, instead of being guessed at with a sleep long enough to
     * cover the response timeout.
     */
    private void awaitCycleDrained() throws Exception {
        final int before = cyclesStarted.get();
        awaitTrue("the in-flight repair cycle to drain so a fresh one can start", () -> {
            loop1.execute(antiEntropy1::runRepairCycle);
            return cyclesStarted.get() > before;
        });
    }

    @Test
    @DisplayName("Divergent key (one node ahead) converges once repair has seen the higher ballot")
    void divergentKeyRepairedByRepairCycle() throws Exception {
        // Write through proposer so all 3 acceptors get the value
        final CompletableFuture<Proposer.CasResult> f =
                proposer1.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v1"));
        f.get(5, TimeUnit.SECONDS);

        // Directly mutate store2 to simulate divergence (higher ballot, different value)
        final CompletableFuture<Void> diverge = new CompletableFuture<>();
        loop2.execute(() -> {
            store2.writeAccept(TestBytes.hashed("k"), new Ballot(9999, nid(2)), TestBytes.hashed("divergent"), false);
            diverge.complete(null);
        });
        diverge.get(5, TimeUnit.SECONDS);

        // Two cycles minimum, and that is the algorithm rather than a slow machine.
        //
        // Node 1's proposer has never seen ballot 9999 -- the divergence was written straight into
        // node 2's store, bypassing consensus, so nothing taught it. Its first repair round
        // therefore opens at a low ballot, and {node 1, node 3} is already a quorum for it: the
        // round commits below the divergence, and only node 2's NACKs carry 9999 back. Measured,
        // the state after exactly one cycle is node1=2/v1, node2=9999/divergent, node3=2/v1.
        //
        // The second round opens above 9999, node 2 promises, its value is the highest accepted in
        // the prepare quorum, and that is what commits everywhere. So drive cycles until the
        // property holds -- asserting after one cycle asserts something Paxos does not offer, and
        // this test only ever passed on a stray cycle queued by the poll loop below.
        awaitTrue(() -> "a repair round to commit on a quorum above the divergent ballot; replicas now "
                        + describe(snapshots(TestBytes.hashed("k")))
                        + ", cycles=" + cyclesStarted.get(),
                () -> {
                    if (replicasAbove(TestBytes.hashed("k"), 9999L) >= 2) {
                        return true;
                    }
                    startRepairCycle();
                    return replicasAbove(TestBytes.hashed("k"), 9999L) >= 2;
                });

        // Whichever value it settled on, the replicas that took it must agree -- convergence is the
        // property. *Which* value wins is Paxos's choice, not anti-entropy's: repairRound is an
        // ordinary round, so a prepare quorum of nodes 1 and 3 never sees node 2's higher ballot and
        // "v1" survives. Asserting a particular value here is asserting a coin flip, which is
        // exactly what made this test fail 2 runs in 5.
        final Set<HashedBytes> settled = new HashSet<>();
        final List<Snapshot> after = snapshots(TestBytes.hashed("k"));
        for (int i = 0; i < after.size(); i++) {
            if (after.get(i).ballotCounter > 9999L) {
                settled.add(after.get(i).value);
            }
        }
        assertEquals(1, settled.size(),
                "Replicas that took the repair round must agree on its value, saw " + settled);
    }

    @Test
    @DisplayName("Repair cycle is idempotent on already-converged state")
    void repairCycleIsIdempotentOnConvergedState() throws Exception {
        // Write through proposer so state is consistent
        proposer1.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")).get(5, TimeUnit.SECONDS);

        final CompletableFuture<Void> cycleStarted = new CompletableFuture<>();
        loop1.execute(() -> {
            antiEntropy1.runRepairCycle();
            cycleStarted.complete(null);
        });
        cycleStarted.get(5, TimeUnit.SECONDS);

        // Converged state produces no repair, so there is no effect to wait for -- the cycle
        // finishing is the event, and that is exactly what awaitCycleDrained detects.
        awaitCycleDrained();

        assertEquals(TestBytes.hashed("v"), acceptedValue(loop1, store1, TestBytes.hashed("k")),
                "An idempotent cycle must leave local state untouched");
        final Proposer.CasResult result = proposer1.identityRound(TestBytes.hashed("k")).get(5, TimeUnit.SECONDS);
        assertEquals(TestBytes.hashed("v"), result.value());
    }

    @Test
    @DisplayName("Repair cycle detects and propagates a local-only range")
    void repairCycleDetectsLocalOnlyRange() throws Exception {
        // Write directly to store1 only (bypassing Paxos) - divergent
        final CompletableFuture<Void> localWrite = new CompletableFuture<>();
        loop1.execute(() -> {
            store1.writeAccept(TestBytes.hashed("local-only"), new Ballot(1, nid(1)), TestBytes.hashed("v"), false);
            localWrite.complete(null);
        });
        localWrite.get(5, TimeUnit.SECONDS);

        startRepairCycle();

        // What repair owes is propagation: the key existed on node 1 alone and must end up on a
        // quorum. Only a quorum -- a Paxos round commits at a majority, so the third replica may
        // legitimately not have it. (identityRound returns a non-null CasResult unconditionally,
        // so asserting on it would assert nothing.)
        awaitTrue("the local-only key to reach a quorum of replicas", () ->
                replicasHolding(TestBytes.hashed("local-only"), TestBytes.hashed("v")) >= 2);

        final Proposer.CasResult result =
                proposer1.identityRound(TestBytes.hashed("local-only")).get(5, TimeUnit.SECONDS);
        assertEquals(TestBytes.hashed("v"), result.value());
    }

    @Test
    @DisplayName("Shutdown stops the periodic repair cycle cleanly")
    void shutdownStopsRepairCycle() throws Exception {
        final CompletableFuture<Void> done = new CompletableFuture<>();
        loop1.execute(() -> {
            antiEntropy1.startPeriodicRepair(Duration.ofMillis(100));
            antiEntropy1.close();
            done.complete(null);
        });
        done.get(5, TimeUnit.SECONDS);

        final int afterShutdown = cyclesStarted.get();
        // The one sleep worth keeping: this asserts an *absence*, so elapsed time is the substance
        // of the test rather than a stand-in for a condition. Six intervals at 100ms.
        Thread.sleep(600L);
        assertEquals(afterShutdown, cyclesStarted.get(),
                "No repair cycle may start after shutdown -- no cycle timer may survive it");
    }

    @Test
    @DisplayName("Cycles keep starting: completion always re-arms the next cycle")
    void periodicCyclesKeepRunning() throws Exception {
        // Guards the liveness half of the cycle-accounting invariant. Completion is derived from
        // activeRepairRanges draining, so a range dropped without being accounted for would leave
        // the cycle permanently incomplete: cycleActive stays true, runRepairCycle keeps
        // returning early, and anti-entropy stops for the life of the node. Only a lower bound is
        // asserted -- cycles are scheduled sequentially (the next is armed on completion), so the
        // achievable rate is machine-dependent, but a wedge shows up unambiguously as a flat count.
        proposer1.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")).get(5, TimeUnit.SECONDS);

        // Diverge a key so each cycle has real repair work to drain, not just a digest compare.
        final CompletableFuture<Void> diverge = new CompletableFuture<>();
        loop2.execute(() -> {
            store2.writeAccept(TestBytes.hashed("k"), new Ballot(9999, nid(2)), TestBytes.hashed("divergent"), false);
            diverge.complete(null);
        });
        diverge.get(5, TimeUnit.SECONDS);

        loop1.execute(() -> antiEntropy1.startPeriodicRepair(Duration.ofMillis(100)));

        awaitTrue("at least three repair cycles to start"
                + " -- a cycle that never completes wedges anti-entropy permanently",
                () -> cyclesStarted.get() >= 3);
    }

    @Test
    @DisplayName("Repair cycle tolerates an unresponsive peer (proceeds via quorum + timeout)")
    void repairCycleToleratesUnresponsivePeer() throws Exception {
        proposer1.write(TestBytes.hashed("k"), current -> TestBytes.hashed("v")).get(5, TimeUnit.SECONDS);

        // Replace node 3's handler with one that silently drops all messages
        t3.register(msg -> { /* drop everything */ });

        // Trigger repair - should complete via timeout, not hang
        final CompletableFuture<Void> cycleStarted = new CompletableFuture<>();
        loop1.execute(() -> {
            antiEntropy1.runRepairCycle();
            cycleStarted.complete(null);
        });
        cycleStarted.get(5, TimeUnit.SECONDS);

        // Node 3 never answers, so this cycle can only end via RESPONSE_TIMEOUT. Waiting for the
        // cycle to actually drain is the whole property -- a cycle that never completes leaves
        // cycleActive set, every later runRepairCycle returns early, and anti-entropy is dead for
        // the life of the node. A fixed sleep would assert none of that, since the read that
        // follows succeeds from the surviving quorum whether or not the cycle ever finished.
        awaitCycleDrained();

        final Proposer.CasResult result = proposer1.identityRound(TestBytes.hashed("k")).get(5, TimeUnit.SECONDS);
        assertEquals(TestBytes.hashed("v"), result.value());
    }

    /**
     * The key deleted through consensus, held as a tombstone by every replica.
     * <p>
     * A cluster is mid-purge whenever one member has applied a decision another has not seen yet,
     * and repair decides whether that window closes or becomes a loop: a tombstone handed back to
     * the member that just collected it would undo every collection as fast as it was decided.
     */
    private HashedBytes deletedEverywhere(final String name) throws Exception {
        final HashedBytes key = TestBytes.hashed(name);
        proposer1.write(key, current -> TestBytes.hashed("v")).get(5, TimeUnit.SECONDS);
        proposer1.write(key, current -> null).get(5, TimeUnit.SECONDS);
        awaitTrue("every replica to hold the tombstone", () -> replicasTombstoned(key) == 3);
        return key;
    }

    private Ballot tombstoneBallot(final HashedBytes key) throws Exception {
        return onLoop(loop1, () -> store1.get(key).accepted);
    }

    private void collectOn(final EventLoop loop, final LocalStore store,
                           final HashedBytes key, final Ballot ballot) throws Exception {
        assertTrue(onLoop(loop, () -> store.purge(key, ballot)), "Precondition: the purge applies");
    }

    private boolean holdsNothing(final EventLoop loop, final LocalStore store, final HashedBytes key)
            throws Exception {
        return onLoop(loop, () -> store.get(key) == null);
    }

    private boolean holdsTombstone(final EventLoop loop, final LocalStore store, final HashedBytes key)
            throws Exception {
        return onLoop(loop, () -> {
            final KeyState state = store.get(key);
            return state != null && state.tombstone;
        });
    }

    private int replicasTombstoned(final HashedBytes key) throws Exception {
        int held = 0;
        if (holdsTombstone(loop1, store1, key)) {
            held++;
        }
        if (holdsTombstone(loop2, store2, key)) {
            held++;
        }
        if (holdsTombstone(loop3, store3, key)) {
            held++;
        }
        return held;
    }

    /** Two full cycles: one to compare, and one to prove the first left nothing behind. */
    private void repairTwice() throws Exception {
        startRepairCycle();
        awaitCycleDrained();
        startRepairCycle();
        awaitCycleDrained();
    }

    @Test
    @DisplayName("A member that collected a tombstone is not handed it back by one that has not")
    void aCollectedTombstoneIsNotPulledBack() throws Exception {
        final HashedBytes key = deletedEverywhere("collected-here");
        collectOn(loop1, store1, key, tombstoneBallot(key));

        repairTwice();

        assertTrue(holdsNothing(loop1, store1, key),
                "The key this node collected must stay collected: a tombstone pulled back here is "
                        + "a collection undone, and the next sweep would decide it all over again");
        assertEquals(2, replicasTombstoned(key), "The members that have not collected it still hold it");
    }

    @Test
    @DisplayName("A tombstone is not pushed to the members that already collected it")
    void aTombstoneIsNotPushedToMembersThatCollectedIt() throws Exception {
        final HashedBytes key = deletedEverywhere("collected-there");
        final Ballot ballot = tombstoneBallot(key);
        collectOn(loop2, store2, key, ballot);
        collectOn(loop3, store3, key, ballot);

        // The direction that re-seeds: this node holds a key no peer has, which is exactly the
        // shape of a local-only key that repair is supposed to propagate.
        repairTwice();

        assertTrue(holdsNothing(loop2, store2, key), "Node 2 collected this key");
        assertTrue(holdsNothing(loop3, store3, key), "Node 3 collected this key");
        assertEquals(1, replicasTombstoned(key),
                "And the member still holding it collects it on a later sweep, where the two that "
                        + "already have answer ABSENT and permit");
    }

    @Test
    @DisplayName("A delete still reaches the replica that is merely behind -- the one holding the value")
    void aDeleteStillReachesAReplicaHoldingTheValue() throws Exception {
        // The control for the two above: 'a tombstone and an absent key are the same state' must not
        // become 'tombstones stop propagating'. The replica that has not seen a delete is the one
        // still holding the value, and that is what tombstones exist to out-vote.
        final HashedBytes key = TestBytes.hashed("delete-propagates");
        proposer1.write(key, current -> TestBytes.hashed("v")).get(5, TimeUnit.SECONDS);
        onLoop(loop1, () -> {
            store1.writeAccept(key, new Ballot(9999L, nid(1)), null, true);
            return null;
        });

        repairTwice();

        awaitTrue("the delete to reach a quorum", () -> replicasTombstoned(key) >= 2);
        assertEquals(0, replicasHolding(key, TestBytes.hashed("v")),
                "No replica may still be serving the deleted value");
    }
}
