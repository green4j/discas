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
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.transport.PeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Every way a member can be away while a tombstone is being collected, crossed with everything it
 * can come back with.
 *
 * <p>Sampling faults here would prove little, for the reason
 * {@code StorageRecoveryMatrixTest} exists: two of its rows were bugs that passed the tests of their
 * time. So the space is enumerated -- <b>what the member lost</b> against <b>when it went away</b> --
 * and every cell answers the same three questions:
 *
 * <ol>
 *   <li><b>no phantom</b> -- the deleted value never becomes readable again. Checked as a reader
 *       would experience it rather than per member: a read adopts the highest accepted state in the
 *       quorum that answers it, so the claim is that <em>no quorum</em> has a value as its highest
 *       accepted state. A member that is merely behind holds the old value and is not a phantom;
 *       one that holds it where nothing out-votes it is.</li>
 *   <li><b>it does go</b> -- with every member back and the key left alone, the tombstone is
 *       collected everywhere.</li>
 *   <li><b>no permanent block</b> -- a member that returns with less than it left with delays
 *       collection and nothing more. That is the same assertion arriving within a budget rather
 *       than never; the complement, a member that stays away stopping collection, is
 *       {@link #aMemberThatStaysAwayStopsCollectionAndNothingElse()}.</li>
 * </ol>
 *
 * <h2>A witness key, not a workload key</h2>
 * The key here is written once and deleted once, both by seeding each member's log directly, and no
 * traffic ever touches it again. That is deliberate and is the note {@code ClusterKillNineChaosTest}
 * records: every round re-replicates what it decides, so a cluster-level test cannot see lost data
 * through keys its own traffic keeps repairing. A purge is exactly the state ordinary traffic
 * repairs away.
 *
 * <h2>What is modelled here and what is modelled elsewhere</h2>
 * The storage shapes themselves -- which directories can prove their own ceiling -- are enumerated
 * on real directories in {@code StorageRecoveryMatrixTest}. What a memory-backed log injects here is
 * their <em>consequence</em>, so that the cluster half of the space can be enumerated without a
 * process per cell: a short log ({@link Lost#UNFORCED_TAIL}), a log missing records it cannot
 * account for ({@link Lost#HALF_REMOVED}), an empty directory with a new incarnation
 * ({@link Lost#STORAGE_REPLACED}).
 *
 * <p>The victim never sweeps while it is the subject of one: which node coordinates a sweep is a
 * cost choice (see item 3), and fixing it removes a race this matrix is not about. It comes back
 * sweeping like everyone else, because for the rows where it is the last holder of the tombstone,
 * its own sweep is what finishes the collection.
 */
@Tag("chaos")
@Timeout(value = 15, unit = TimeUnit.MINUTES)
@DisplayName("Tombstone collection -- every way a member can go away and come back")
class TombstoneCollectionMatrixTest {

    private static final ClusterId CLUSTER = ClusterId.of("tombstone-matrix");
    private static final NodeId ONE = NodeId.of("gc-1");
    private static final NodeId TWO = NodeId.of("gc-2");
    private static final NodeId VICTIM = NodeId.of("gc-3");
    private static final List<NodeId> MEMBERS = List.of(ONE, TWO, VICTIM);

    private static final HashedBytes WITNESS = TestBytes.hashed("witness");
    private static final HashedBytes VALUE = TestBytes.hashed("the-deleted-value");
    private static final Ballot VALUE_BALLOT = new Ballot(4L, ONE);
    private static final Ballot TOMBSTONE_BALLOT = new Ballot(7L, ONE);
    /**
     * A ceiling every member reserved before any of this, as a running cluster would have. Without
     * it a member that has to take its floor from the cluster would be handed zero, and the rule
     * that bounds what it may keep would have nothing to bound.
     */
    private static final long RESERVED_CEILING = 1000L;

    private static final Duration SWEEP_INTERVAL = Duration.ofMillis(200);
    /** Long enough that a member configured with it does nothing periodic during a cell. */
    private static final Duration PASSIVE = Duration.ofHours(1);
    /**
     * Generous, and one row needs it: a member that comes back empty and is handed a stale prepare
     * for the witness key answers it, and a promise with no accepted state behind it is a state --
     * so that member answers {@code RETAINED} and blocks the collection until the promise is
     * evicted, which cannot happen before {@code LocalStore.MIN_PROMISE_AGE_NANOS}.
     */
    private static final Duration BUDGET = Duration.ofSeconds(90);

    /** What the member's log holds when it opens again. */
    enum Lost {
        /** A clean restart: everything it acknowledged is still there. */
        NOTHING_LOST,
        /** {@code SIGKILL} then a power cut: the records past the last force are gone. */
        UNFORCED_TAIL,
        /** An empty directory and a new incarnation -- a replaced disk, or a wiped node. */
        STORAGE_REPLACED,
        /**
         * A log that came back missing the tombstone's accept, keeping the value's, and unable to
         * account for where it starts. The worst consequence of half a directory: unlike a lost
         * tail this can swallow a record that was forced, which is the fault no local check can see.
         */
        HALF_REMOVED
    }

    /** The moment the member went away. */
    enum When {
        /** Before anyone asked it anything. */
        BEFORE_THE_CHECK,
        /** After it answered -- its answer is monotonic and stands -- but before the decision. */
        AFTER_ANSWERING,
        /** After it applied the purge, with that record still in its buffer. */
        AFTER_THE_PURGE,
        /** While anti-entropy was repairing the key: it starts behind, holding the value. */
        DURING_REPAIR
    }

    private final List<Member> members = new ArrayList<>();
    private Member one;
    private Member two;
    private Member victim;

    @AfterEach
    void tearDown() {
        for (int i = members.size() - 1; i >= 0; i--) {
            members.get(i).stop();
        }
    }

    @ParameterizedTest(name = "away {1}, back with {0}")
    @CsvSource({
        "NOTHING_LOST,      BEFORE_THE_CHECK",
        "NOTHING_LOST,      AFTER_ANSWERING",
        "NOTHING_LOST,      AFTER_THE_PURGE",
        "NOTHING_LOST,      DURING_REPAIR",
        "UNFORCED_TAIL,     BEFORE_THE_CHECK",
        "UNFORCED_TAIL,     AFTER_ANSWERING",
        "UNFORCED_TAIL,     AFTER_THE_PURGE",
        "UNFORCED_TAIL,     DURING_REPAIR",
        "STORAGE_REPLACED,  BEFORE_THE_CHECK",
        "STORAGE_REPLACED,  AFTER_ANSWERING",
        "STORAGE_REPLACED,  AFTER_THE_PURGE",
        "STORAGE_REPLACED,  DURING_REPAIR",
        "HALF_REMOVED,      BEFORE_THE_CHECK",
        "HALF_REMOVED,      AFTER_ANSWERING",
        "HALF_REMOVED,      AFTER_THE_PURGE",
        "HALF_REMOVED,      DURING_REPAIR",
    })
    void everyWayAMemberCanGoAndComeBack(final Lost lost, final When when) throws Exception {
        startCluster(when);

        reachTheMoment(when);
        victim.stop();
        outage(when);
        damage(victim, lost);
        victim.bringBack();

        awaitCollectedEverywhere(lost + " away " + when);
    }

    @Test
    @DisplayName("A member that stays away stops collection, and stops nothing else")
    void aMemberThatStaysAwayStopsCollectionAndNothingElse() throws Exception {
        // The complement of the matrix: every row above ends in a collection because the member
        // came back. This is the one shape that does not resolve itself, and it must look like what
        // it is -- a cluster with a member down -- rather than like a broken collector.
        startCluster(When.BEFORE_THE_CHECK);
        victim.stop();

        // Several sweep intervals with a member down. A sleep rather than a wait, and the kind that
        // earns one: the claim is that nothing happens.
        Thread.sleep(SWEEP_INTERVAL.toMillis() * 10);

        assertTrue(holdsTombstone(one), "The tombstone stays: silence blocks, and that is the design");
        assertTrue(holdsTombstone(two), "On both surviving members");
        assertEquals(0, one.wal.countEntries(Wal.Entry.Purge.class),
                "And no decision may be taken while a member cannot answer for its own storage");
        assertEquals(0, two.wal.countEntries(Wal.Entry.Purge.class));

        // And it is only collection that is blocked: the member comes back and it finishes.
        victim.bringBack();
        awaitCollectedEverywhere("after the member came back");
    }


    private void startCluster(final When when) throws Exception {
        final boolean victimIsBehind = when == When.DURING_REPAIR;
        one = new Member(ONE, seeded(true), true);
        two = new Member(TWO, seeded(true), true);
        // Passive until it comes back: it neither sweeps nor runs repair cycles of its own, so the
        // coordinator is always another member and 'behind' stays behind until a peer repairs it.
        // Which node coordinates is a cost choice (item 3); fixing it removes a race this matrix is
        // not about. Behind means it never saw the delete -- it holds the value alone.
        victim = new Member(VICTIM, seeded(!victimIsBehind), false);
        members.add(one);
        members.add(two);
        members.add(victim);
        // Armed before anything starts. Anti-entropy runs its first cycle the moment a node serves,
        // so a rule installed after that races it -- and losing that race leaves the victim already
        // repaired, with the moment this cell is about gone by.
        arm(when);
        for (final Member member : members) {
            member.start();
        }
        awaitTrue("every member to serve", BUDGET, () -> {
            for (final Member member : members) {
                if (member.node.healthSource().state() != NodeState.SERVING) {
                    return false;
                }
            }
            return true;
        });
    }

    /**
     * A log holding the reserved ceiling, the value forced, and the tombstone <em>not</em> forced --
     * the ordinary state of a member seconds after a delete, and the one where losing a tail
     * uncovers the value the tombstone was suppressing.
     */
    private static InMemoryWal seeded(final boolean deleted) {
        final InMemoryWal wal = new InMemoryWal();
        wal.append(new Wal.Entry.PromiseCeiling(RESERVED_CEILING));
        wal.append(new Wal.Entry.Accept(WITNESS, VALUE_BALLOT, VALUE, false));
        wal.force();
        if (deleted) {
            wal.append(new Wal.Entry.Accept(WITNESS, TOMBSTONE_BALLOT, null, true));
        }
        return wal;
    }

    private static void damage(final Member member, final Lost lost) {
        switch (lost) {
            case NOTHING_LOST:
                return;
            case UNFORCED_TAIL:
                member.wal.loseUnforcedTail();
                return;
            case STORAGE_REPLACED:
                // A different log, because that is what a replaced disk is: an empty directory
                // whose incarnation id is new, which is how its peers can tell.
                member.wal = new InMemoryWal();
                return;
            case HALF_REMOVED:
                member.wal.swallow(entry -> entry instanceof Wal.Entry.Accept
                        && ((Wal.Entry.Accept) entry).tombstone());
                return;
            default:
                throw new IllegalArgumentException(lost.name());
        }
    }

    /**
     * Keep the member away long enough for the cluster to try without it, where that is what the row
     * is about -- and assert what it must have done: swept, been blocked, and decided nothing.
     * <p>
     * Without this the member is back within milliseconds and no sweep runs during its absence, so
     * the rows say nothing about the all-members condition: relaxing that condition to a quorum
     * leaves the whole matrix green, which is a matrix that would not notice the one change it
     * exists to notice.
     */
    private void outage(final When when) throws Exception {
        if (when != When.BEFORE_THE_CHECK) {
            // The other rows reached their moment through the protocol, which means a sweep has
            // already run -- and for AFTER_THE_PURGE the key is gone from the survivors, so there
            // is no candidate left to be blocked on.
            return;
        }
        awaitTrue(() -> "a sweep to run and be blocked by the member that is away: " + describe(),
                BUDGET, () -> one.blockedSweeps.get() > 0 || two.blockedSweeps.get() > 0);
        assertEquals(0, one.wal.countEntries(Wal.Entry.Purge.class),
                "Nothing may be decided while a member cannot answer for its own storage");
        assertEquals(0, two.wal.countEntries(Wal.Entry.Purge.class));
    }

    /** What must not reach the victim for the cell to be about the moment it names. */
    private void arm(final When when) {
        switch (when) {
            case BEFORE_THE_CHECK:
                // The check must not reach the victim, and dropping it is what says so. There is no
                // quiet window to take the member away in -- a sweep runs one interval after a node
                // serves -- so anything resting on being quicker than the sweeper would be a race
                // this matrix is not about.
                dropToVictim(message -> message instanceof PeerMessage.PurgeCheckReq);
                return;
            case AFTER_THE_PURGE:
                // Nothing: this one is reached by letting the protocol run, and taking the member
                // away at a point its own log records.
                return;
            case AFTER_ANSWERING:
                // The decision is allowed to happen; only its delivery to the victim is not.
                dropToVictim(message -> message instanceof PeerMessage.PurgeReq);
                return;
            case DURING_REPAIR:
                // The victim starts behind, so repair pushes the tombstone at it from the first
                // cycle. Dropping that accept is what leaves it behind when it goes.
                dropToVictim(message -> message instanceof PeerMessage.AcceptReq
                        && WITNESS.equals(((PeerMessage.AcceptReq) message).key()));
                // And it must not repair itself. Anti-entropy runs a cycle the moment a node
                // serves, whatever its interval, and a round the victim drives adopts the highest
                // state its own prepare quorum reports -- the tombstone -- and applies it locally,
                // through no message this test could drop. A member that pulls the delete in by
                // itself is not a member that is behind, which is what this row is about.
                victim.drop = (target, message) -> aRoundOverTheWitness(message);
                return;
            default:
                throw new IllegalArgumentException(when.name());
        }
    }

    /** Run the cluster until the victim is in the state {@code when} names, then leave it there. */
    private void reachTheMoment(final When when) throws Exception {
        switch (when) {
            case BEFORE_THE_CHECK:
                // Nothing to wait for: the check is dropped on the way in, so the victim has not
                // answered one whether or not a sweep has already run.
                assertEquals(0, victim.checksAnswered.get(),
                        "This row is about a member that was never asked");
                return;
            case AFTER_ANSWERING:
                awaitTrue(() -> "the victim to answer a purge check: " + describe(), BUDGET,
                        () -> victim.checksAnswered.get() > 0);
                return;
            case AFTER_THE_PURGE:
                awaitTrue(() -> "the victim to apply the purge: " + describe(), BUDGET,
                        () -> victim.wal.countEntries(Wal.Entry.Purge.class) > 0);
                return;
            case DURING_REPAIR:
                awaitTrue(() -> "a repair round to reach the victim: " + describe(), BUDGET,
                        () -> repairsAttemptedAtVictim() > 0);
                return;
            default:
                throw new IllegalArgumentException(when.name());
        }
    }

    private int repairsAttemptedAtVictim() {
        return one.droppedToVictim.get() + two.droppedToVictim.get();
    }

    private static boolean aRoundOverTheWitness(final PeerMessage message) {
        if (message instanceof PeerMessage.PrepareReq) {
            return WITNESS.equals(((PeerMessage.PrepareReq) message).key());
        }
        if (message instanceof PeerMessage.AcceptReq) {
            return WITNESS.equals(((PeerMessage.AcceptReq) message).key());
        }
        return false;
    }

    private void dropToVictim(final Predicate<PeerMessage> what) {
        for (final Member member : members) {
            member.drop = (target, message) -> VICTIM.equals(target) && what.test(message);
        }
    }

    private void allowEverything() {
        for (final Member member : members) {
            member.drop = (target, message) -> false;
        }
    }


    private void awaitCollectedEverywhere(final String cell) throws Exception {
        allowEverything();
        awaitTrue(() -> "every member to end with nothing for the witness key (" + cell + "): "
                        + describe(), BUDGET, () -> {
                assertNoPhantom(cell);
                return absentEverywhere();
            });
        assertNoPhantom(cell);
    }

    /**
     * A read adopts the highest accepted state of the quorum that answers it, so a value is
     * readable exactly when some quorum holds it as its highest. With three members every pair is a
     * quorum, and a member that is down is included: it is coming back, and a read after that can
     * use it.
     */
    private void assertNoPhantom(final String cell) throws Exception {
        final List<KeyState> states = new ArrayList<>();
        for (final Member member : members) {
            final KeyState state = stateOf(member);
            states.add(state == null ? ABSENT : state);
        }
        for (int a = 0; a < states.size(); a++) {
            for (int b = a + 1; b < states.size(); b++) {
                final KeyState highest =
                        states.get(a).accepted.compareTo(states.get(b).accepted) >= 0
                                ? states.get(a) : states.get(b);
                if (!highest.accepted.isZero() && !highest.tombstone) {
                    fail("The deleted value is readable again (" + cell + "): members " + (a + 1)
                            + " and " + (b + 1) + " would answer a read with it -- " + describe());
                }
            }
        }
    }

    /** Stands in for a member that holds nothing: the state a read gets from it. */
    private static final KeyState ABSENT = new KeyState();

    private boolean absentEverywhere() throws Exception {
        return stateOf(one) == null && stateOf(two) == null && stateOf(victim) == null;
    }

    private boolean holdsTombstone(final Member member) throws Exception {
        final KeyState state = stateOf(member);
        return state != null && state.tombstone;
    }

    /**
     * What a member holds for the witness key: live state while it is up, and what its log would
     * replay to while it is down -- which is what it is about to come back with.
     */
    private KeyState stateOf(final Member member) throws Exception {
        if (member.node != null) {
            return onLoop(member.loop, () -> member.node.store().get(WITNESS));
        }
        final LocalStore replayed = new LocalStore(member.wal);
        final LocalStore.ThrottledLoader loader = replayed.beginRecovery();
        while (loader.loadBatch(1024)) {
            // drain
        }
        return replayed.get(WITNESS);
    }

    private String describe() throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (final Member member : members) {
            final KeyState state = stateOf(member);
            sb.append(sb.length() == 0 ? "" : ", ").append(member.id.value()).append('=');
            if (state == null) {
                sb.append("absent");
            } else if (state.accepted.isZero()) {
                sb.append("promise-only@").append(state.promised.counter());
            } else {
                sb.append(state.tombstone ? "tombstone" : "VALUE").append('@')
                        .append(state.accepted.counter());
            }
            sb.append(" (repairs=").append(member.repairsIssued.get())
                    .append(" prepares=").append(member.preparesSent.get())
                    .append(" collected=").append(member.collections.get())
                    .append(" answered=").append(member.checksAnswered.get())
                    .append(" droppedToVictim=").append(member.droppedToVictim.get())
                    .append(member.roundFailures.isEmpty()
                            ? "" : " roundFailures=" + List.copyOf(member.roundFailures))
                    .append(')');
        }
        return sb.toString();
    }


    /** One member, and the log it keeps across every restart of it. */
    private final class Member {
        private final NodeId id;
        private InMemoryWal wal;
        private final AtomicInteger checksAnswered = new AtomicInteger();
        private final AtomicInteger droppedToVictim = new AtomicInteger();
        /** What this member's own protocol did, so a timeout says why rather than only that. */
        private final AtomicInteger repairsIssued = new AtomicInteger();
        private final AtomicInteger preparesSent = new AtomicInteger();
        private final AtomicInteger collections = new AtomicInteger();
        private final AtomicInteger blockedSweeps = new AtomicInteger();
        private final List<String> roundFailures = Collections.synchronizedList(new ArrayList<>());
        private final NodeObserver observer = new NodeObserver() {
            @Override
            public void keyRepaired(final HashedBytes key) {
                if (WITNESS.equals(key)) {
                    repairsIssued.incrementAndGet();
                }
            }

            @Override
            public void roundFailed(final HashedBytes key, final String reason) {
                if (WITNESS.equals(key)) {
                    roundFailures.add(reason);
                }
            }

            @Override
            public void tombstoneSwept(final TombstoneSweep sweep) {
                if (sweep.collected()) {
                    collections.incrementAndGet();
                } else if (sweep.blocked() && WITNESS.equals(sweep.key())) {
                    blockedSweeps.incrementAndGet();
                }
            }
        };

        private boolean active;
        private EventLoop loop;
        private DisCasNode node;
        private volatile BiPredicate<NodeId, PeerMessage> drop = (target, message) -> false;

        private Member(final NodeId id, final InMemoryWal wal, final boolean active) {
            this.id = id;
            this.wal = wal;
            this.active = active;
        }

        private void start() {
            loop = new EventLoop("matrix-" + id.value());
            final PeerTransport raw = new InProcessPeerTransport(
                    id, MEMBERS.size(), loop, InMemoryMembers.ofNodes(MEMBERS));
            node = new DisCasNode(
                    config(id, active), wal, loop, new Interceptor(raw, this), observer);
            node.start();
        }

        /** Bring the member back as a full member: sweeping and repairing like every other one. */
        private void bringBack() throws Exception {
            active = true;
            start();
            awaitTrue(id.value() + " to serve again", BUDGET,
                    () -> node.healthSource().state() == NodeState.SERVING);
        }

        private void stop() {
            if (node == null) {
                return;
            }
            try {
                node.close();
            } catch (final Exception ignored) {
                // a node closing after its own transport was replaced is still a closed node
            }
            node = null;
            loop = null;
        }
    }

    private static NodeConfig config(final NodeId id, final boolean active) {
        return NodeConfig.builder()
                .nodeId(id)
                .clusterId(CLUSTER)
                .clusterSize(MEMBERS.size())
                .tombstoneSweepInterval(active ? SWEEP_INTERVAL : PASSIVE)
                .peerResponseTimeout(Duration.ofMillis(500))
                .repairInterval(active ? Duration.ofMillis(500) : PASSIVE)
                // Long, so that a tombstone stays unforced until the check that has to force it --
                // the whole point of the rows that lose a tail.
                .walForceInterval(Duration.ofSeconds(30))
                .snapshotInterval(Duration.ofHours(1))
                .ceilingRecoveryRetryCap(Duration.ofMillis(200))
                // Promise-only states are what a stale prepare leaves behind, and they block a
                // collection while they last. Swept as often as the sweeper runs, so a row that
                // hits one waits out the age threshold and nothing more.
                .promiseEvictionInterval(Duration.ofMillis(200))
                .build();
    }

    /**
     * Lets the test drop chosen messages and see chosen ones go out -- the two things a "when" needs.
     * A member is taken away by closing it, which is the real thing; this is only for the moments
     * that are defined by a message rather than by a member.
     */
    private final class Interceptor implements PeerTransport {
        private final PeerTransport delegate;
        private final Member owner;

        private Interceptor(final PeerTransport delegate, final Member owner) {
            this.delegate = delegate;
            this.owner = owner;
        }

        @Override
        public void send(final NodeId target, final PeerMessage message) {
            if (owner.drop.test(target, message)) {
                if (VICTIM.equals(target)) {
                    owner.droppedToVictim.incrementAndGet();
                }
                return;
            }
            delegate.send(target, message);
            if (message instanceof PeerMessage.PurgeCheckResp) {
                owner.checksAnswered.incrementAndGet();
            }
            if (message instanceof PeerMessage.PrepareReq
                    && WITNESS.equals(((PeerMessage.PrepareReq) message).key())) {
                owner.preparesSent.incrementAndGet();
            }
        }

        @Override
        public void register(final Consumer<PeerMessage> handler) {
            delegate.register(handler);
        }

        @Override
        public List<NodeId> peers() {
            return delegate.peers();
        }

        @Override
        public int clusterSize() {
            return delegate.clusterSize();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }


    @FunctionalInterface
    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void awaitTrue(final String what, final Duration budget, final Condition condition)
            throws Exception {
        awaitTrue(() -> what, budget, condition);
    }

    /** {@code what} is built only on failure, so it may inspect live state. */
    private static void awaitTrue(final ThrowingSupplier what, final Duration budget,
                                  final Condition condition) throws Exception {
        final long deadline = System.nanoTime() + budget.toNanos();
        do {
            if (condition.holds()) {
                return;
            }
            Thread.sleep(20L);
        } while (System.nanoTime() - deadline < 0);
        fail("Timed out after " + budget + " waiting for " + what.get());
    }

    @FunctionalInterface
    private interface ThrowingSupplier {
        String get() throws Exception;
    }

    private static <T> T onLoop(final EventLoop loop, final Supplier<T> body) throws Exception {
        final CompletableFuture<T> result = new CompletableFuture<>();
        loop.execute(() -> {
            try {
                result.complete(body.get());
            } catch (final RuntimeException e) {
                result.completeExceptionally(e);
            }
        });
        return result.get(10, TimeUnit.SECONDS);
    }
}
