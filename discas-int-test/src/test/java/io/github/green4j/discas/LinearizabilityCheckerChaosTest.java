/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.chaos.TestProfile;
import io.github.green4j.discas.chaos.history.HistoryRecorder;
import io.github.green4j.discas.chaos.history.OpRecord;
import io.github.green4j.discas.chaos.history.RegisterLinearizabilityChecker;
import io.github.green4j.discas.node.HashedBytes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Workstream B -- history-based linearizability under fault injection. Several clients
 * drive a concurrent get/put/cas/delete workload over a small key set while a nemesis
 * repeatedly partitions a single node (always leaving a quorum) and heals it. Every
 * operation is recorded with its real-time interval; afterwards the merged history must
 * be linearizable per key. Because each client issues operations synchronously, at most
 * {@code #clients} operations overlap at any instant, keeping the Wing&amp;Gong search
 * cheap.
 */
@Tag("chaos")
@DisplayName("Linearizability under partitions (history checker)")
class LinearizabilityCheckerChaosTest {

    private static final int NODES = 3;
    private static final int CLIENTS = 3;

    /**
     * Operations each client issues. The workload is driven by <em>count</em>, not by a wall-clock
     * window: with a window, an operation slower than the window left the run with almost no
     * history, and the resulting "too few operations" failure fired before the linearizability
     * check ever ran -- reporting a slow machine as though the store had misbehaved. It failed
     * roughly one run in fourteen, which is worse than no test, because it teaches you to ignore
     * the signal.
     */
    private static final int OPS_PER_CLIENT = 40;

    /**
     * Per-operation cap. Must stay well below the time a whole run may take, and it is deliberately
     * far shorter than the node's proposer round timeout: an operation routed to the partitioned
     * coordinator cannot succeed anyway, so waiting the full round out buys nothing and only starves
     * the history. Such operations are recorded {@code UNKNOWN}, which the checker handles by
     * exploring both outcomes.
     */
    private static final long OP_TIMEOUT_MS = 500L;

    private static final HashedBytes[] KEYS = {
            new HashedBytes("k0".getBytes()), new HashedBytes("k1".getBytes()), new HashedBytes("k2".getBytes())
    };
    private static final HashedBytes[] VALUES = {
            new HashedBytes("a".getBytes()), new HashedBytes("b".getBytes()), new HashedBytes("c".getBytes())
    };

    private TestCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new TestCluster(NODES, CLIENTS);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void historyIsLinearizableUnderPartitions() throws Exception {
        final TestProfile profile = TestProfile.current();
        assumeTrue(profile.atLeast(TestProfile.STANDARD),
                "linearizability chaos check runs at STANDARD profile or higher");

        final int opsPerClient = profile.scale(OPS_PER_CLIENT);

        final List<HistoryRecorder> recorders = new ArrayList<>();
        for (int c = 0; c < CLIENTS; c++) {
            recorders.add(new HistoryRecorder(cluster.client(c), OP_TIMEOUT_MS));
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread nemesis = new Thread(() -> runNemesis(running), "nemesis");
        nemesis.start();

        final List<Thread> workers = new ArrayList<>();
        for (int c = 0; c < CLIENTS; c++) {
            final HistoryRecorder recorder = recorders.get(c);
            final Thread worker =
                    new Thread(() -> runWorkload(recorder, opsPerClient), "worker-" + c);
            workers.add(worker);
            worker.start();
        }

        for (final Thread worker : workers) {
            worker.join();
        }

        // Stop the nemesis and restore full connectivity so any last in-flight op settles.
        running.set(false);
        nemesis.join();
        healAll();

        final List<OpRecord> history = new ArrayList<>();
        for (final HistoryRecorder recorder : recorders) {
            history.addAll(recorder.snapshot());
        }
        // Exact, not a lower bound: HistoryRecorder counts a call on both the success and the
        // timeout path, so nothing about cluster health can change this. Any other number means a
        // worker died rather than a slow run.
        //
        // Counted in calls rather than records because a fenced CAS is a read and then a write: it
        // contributes one call and one or two records, so the record count is not the iteration
        // count. The records are what the checker consumes; the calls say every worker finished.
        int issued = 0;
        for (final HistoryRecorder recorder : recorders) {
            issued += recorder.callCount();
        }
        assertEquals(CLIENTS * opsPerClient, issued,
                "Every issued operation must be recorded, successful or not");

        // Guard the test's value, not just its stability. A history of nothing but UNKNOWN
        // operations is trivially linearizable, so a change that made every operation time out
        // would leave this test green while checking nothing. Observed here is 96-100% decided;
        // the floor is set far below that so it flags degeneration without policing throughput.
        int decided = 0;
        for (int i = 0; i < history.size(); i++) {
            if (history.get(i).status() == OpRecord.Status.OK) {
                decided++;
            }
        }
        assertTrue(decided * 2 >= history.size(),
                "History is mostly undecided (" + decided + "/" + history.size()
                        + " decided) -- the checker would pass on it vacuously");

        final RegisterLinearizabilityChecker checker = new RegisterLinearizabilityChecker();
        final RegisterLinearizabilityChecker.Result result = checker.check(history);
        assertTrue(result.linearizable(), result.message());
    }

    private void runWorkload(final HistoryRecorder recorder, final int operations) {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < operations; i++) {
            final HashedBytes key = KEYS[rnd.nextInt(KEYS.length)];
            final int roll = rnd.nextInt(100);
            if (roll < 40) {
                recorder.get(key);
            } else if (roll < 65) {
                recorder.put(key, VALUES[rnd.nextInt(VALUES.length)]);
            } else if (roll < 90) {
                final HashedBytes expected = rnd.nextBoolean() ? VALUES[rnd.nextInt(VALUES.length)] : null;
                final HashedBytes desired = VALUES[rnd.nextInt(VALUES.length)];
                recorder.cas(key, expected, desired);
            } else {
                recorder.delete(key);
            }
        }
    }

    private void runNemesis(final AtomicBoolean running) {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while (running.get()) {
            final int victim = 1 + rnd.nextInt(NODES); // node ids are 1..NODES
            partition(victim);
            sleep(250 + rnd.nextInt(250));
            heal(victim);
            sleep(150 + rnd.nextInt(150));
        }
    }

    /** Bidirectionally isolate {@code victim} from every other node (quorum still survives). */
    private void partition(final int victim) {
        for (final int other : cluster.nodeIds()) {
            if (other == victim) {
                continue;
            }
            cluster.transport(victim).isolate(other);
            cluster.transport(other).isolate(victim);
        }
    }

    private void heal(final int victim) {
        for (final int other : cluster.nodeIds()) {
            if (other == victim) {
                continue;
            }
            cluster.transport(victim).heal(other);
            cluster.transport(other).heal(victim);
        }
    }

    private void healAll() {
        for (final int a : cluster.nodeIds()) {
            for (final int b : cluster.nodeIds()) {
                if (a != b) {
                    cluster.transport(a).heal(b);
                }
            }
        }
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
