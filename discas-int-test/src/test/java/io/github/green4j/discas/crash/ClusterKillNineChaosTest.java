/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.crash;

import io.github.green4j.discas.TestPorts;
import io.github.green4j.discas.chaos.TestProfile;
import io.github.green4j.discas.chaos.history.HistoryRecorder;
import io.github.green4j.discas.chaos.history.OpRecord;
import io.github.green4j.discas.chaos.history.RegisterLinearizabilityChecker;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A three-node cluster whose members are killed with {@code kill -9} at random intervals under a
 * concurrent workload, checked for linearizability over the recorded history.
 *
 * <h2>What this adds over the tests it sits beside</h2>
 * {@code LinearizabilityCheckerChaosTest} runs the same shape of workload and the same checker, but
 * its nodes share the test's JVM and its fault is a partition. {@link KillNineCrashTest} kills a
 * real process, but only one holding a {@code FileWal} -- storage, not a cluster. Between them sat
 * the claim a store is actually judged on, which neither made: <b>a cluster whose members are
 * killed at random under load admits no history that is not linearizable, and every acknowledged
 * write survives.</b>
 * <p>
 * So every node here is its own operating-system process ({@link NodeProcess}, running the shipped
 * {@link io.github.green4j.discas.node.starter.DisCasNodeStarter}), reached over the TCP client
 * transport. A "restart" here is a signal the process cannot handle, followed by a fresh JVM
 * opening the directory the dead one left -- not {@code close()} then {@code start()}, which runs
 * every orderly path a crash skips.
 *
 * <h2>Why there are witness keys as well as a history</h2>
 * The obvious way to assert that acknowledged writes survive is to read the workload's keys at the
 * end and let the checker judge: a lost write leaves a final read with no place in any
 * linearization. That assertion is here, and it is nearly blind, which is worth stating rather than
 * discovering later.
 * <p>
 * Every operation in this store is a consensus round, and a round writes the value it decides to
 * whichever quorum answered it -- <em>reads included</em>. So continuing traffic refills a member
 * that came back missing something, within a round or two of the key being touched. Measured, not
 * assumed: with the WAL's replay of accepted values stubbed out, so that every restarted member came
 * back with an empty tail, this run stayed green over three keys and stayed green over eight.
 * <p>
 * The <b>witness keys</b> are the part that cannot be repaired that way. Each is written once, with
 * its acknowledgement waited for, and never touched again until the end of the run -- so no round
 * ever re-replicates it, and whether it comes back is a question about the disks of the members
 * that were killed under it. That is where the removal check bites: with the same replay stubbed
 * out, the witnesses are gone and this test fails, which is what makes its claim about durability
 * mean anything.
 *
 * <h2>Why one member at a time</h2>
 * Not for safety. Killing two of three would also be safe -- {@code SIGKILL} leaves the page cache,
 * so everything an acceptor wrote is still on the platter, and a cluster that cannot form a quorum
 * acknowledges nothing to lose. It would only make the cluster unavailable, and an unavailable
 * cluster produces a history of timed-out operations, which is trivially linearizable and proves
 * nothing. One at a time keeps the register answering, which is what makes the history worth
 * checking.
 * <p>
 * The nemesis waits for a restarted member to report {@code /ready} before choosing its next
 * victim, for that same reason rather than for correctness.
 *
 * <h2>What is not modelled here</h2>
 * A lost machine. {@code SIGKILL} keeps the page cache, so bytes that reached {@code write()}
 * survive it; the fault that takes them is a power cut, which {@code KillNineCrashTest} models by
 * truncating to the last forced offset. That distinction matters more for a cluster than for a
 * single log, because a node's unforced tail is covered by the quorum that also holds it -- which
 * is why {@code walForceInterval} can be a throughput dial at all.
 */
@Tag("chaos")
@Timeout(value = 15, unit = TimeUnit.MINUTES)
@DisplayName("Crash -- a cluster killed with kill -9 stays linearizable")
class ClusterKillNineChaosTest {

    private static final int NODES = 3;
    private static final int CLIENTS = 3;
    private static final String CLUSTER_ID = "kill-nine-cluster";

    /**
     * The one source of randomness in this run. Fixed, and combined with a per-thread constant the
     * way the rest of the chaos suite does it, so a failure can be run again rather than waited for:
     * which member the nemesis takes, when it takes it, and what each client does are all decided
     * from here.
     */
    private static final long SEED = 0x9E3779B97F4A7C15L;

    /**
     * Operations each client issues, driven by count rather than by a wall-clock window for the
     * reason recorded in {@code LinearizabilityCheckerChaosTest}: a window turns a slow machine
     * into a failure that reads as a misbehaving store.
     */
    private static final int OPS_PER_CLIENT = 60;

    /**
     * Pause between one client's operations. It exists to spread the workload across the nemesis
     * rather than to throttle anything: without it three clients finish in a couple of seconds and
     * most of the kills land on an idle cluster.
     */
    private static final long OP_PAUSE_MIN_MS = 80L;
    private static final long OP_PAUSE_MAX_MS = 180L;

    /**
     * How many members the nemesis kills, counted rather than timed. A nemesis that stops when the
     * workload does makes the amount of chaos a property of the runner's speed -- a fast machine
     * would get one kill and still report success, which is the failure mode this suite has been
     * bitten by before.
     */
    private static final int KILL_CYCLES = 5;

    /**
     * Per-operation cap. Longer than the in-process test's, because these operations cross a real
     * socket to a real process, and a failover has a TCP connect in it. Still far below the run's
     * budget: an operation that has not answered by now is recorded {@code UNKNOWN}, which the
     * checker explores both ways.
     */
    private static final long OP_TIMEOUT_MS = 3_000L;

    /** How long a killed member stays down before its replacement is started. */
    private static final long OUTAGE_MIN_MS = 250L;
    private static final long OUTAGE_MAX_MS = 700L;

    /** How long the nemesis waits after a member is ready again before taking the next one. */
    private static final long CALM_MIN_MS = 300L;
    private static final long CALM_MAX_MS = 800L;

    private static final Duration READY_BUDGET = Duration.ofSeconds(60);

    /**
     * Eight keys, where the in-process linearizability test uses three: spreading the same load
     * leaves fewer duels on one key, so more of the history is decided while members are being
     * killed under it.
     * <p>
     * It is <em>not</em> what makes lost data visible, and it was tried as though it were. A member
     * that comes back missing a key is refilled by the next round that touches it, and eight keys
     * only makes that take a little longer -- with the WAL's replay of accepted values stubbed out,
     * the run stayed green over three keys and stayed green over eight. The witness keys are what
     * sees that; these are the workload.
     */
    private static final HashedBytes[] KEYS = {
            new HashedBytes("k0".getBytes()),
            new HashedBytes("k1".getBytes()),
            new HashedBytes("k2".getBytes()),
            new HashedBytes("k3".getBytes()),
            new HashedBytes("k4".getBytes()),
            new HashedBytes("k5".getBytes()),
            new HashedBytes("k6".getBytes()),
            new HashedBytes("k7".getBytes())
    };
    private static final HashedBytes[] VALUES = {
            new HashedBytes("a".getBytes()),
            new HashedBytes("b".getBytes()),
            new HashedBytes("c".getBytes())
    };

    /** How long the witness writer waits between one acknowledged witness and the next. */
    private static final long WITNESS_PAUSE_MS = 250L;

    private final List<NodeProcess> nodes = new ArrayList<>();
    private final List<DisCasClient> clients = new ArrayList<>();

    /**
     * Witness keys whose write was acknowledged, and the value acknowledged for each. Nothing reads
     * or rewrites them until the run is over, so what comes back is what the members' storage kept.
     */
    private final Map<String, String> witnesses = new LinkedHashMap<>();

    @AfterEach
    void tearDown() {
        for (final DisCasClient client : clients) {
            try {
                client.close();
            } catch (final Exception ignored) {
                // A client that will not close cannot invalidate a history already collected.
            }
        }
        clients.clear();
        for (final NodeProcess node : nodes) {
            node.stop();
        }
        nodes.clear();
        witnesses.clear();
    }

    @Test
    @DisplayName("Members killed at random under load leave a linearizable history")
    void clusterSurvivesRandomKills(@TempDir final Path rootDir) throws Exception {
        final TestProfile profile = TestProfile.current();
        assumeTrue(profile.atLeast(TestProfile.STANDARD),
                "the out-of-process cluster costs four JVMs; it runs at STANDARD or higher");

        startCluster(rootDir);
        startClients();

        final List<HistoryRecorder> recorders = new ArrayList<>();
        for (int c = 0; c < CLIENTS; c++) {
            recorders.add(new HistoryRecorder(clients.get(c), OP_TIMEOUT_MS));
        }

        final int killCycles = profile.scale(KILL_CYCLES);
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        final Thread nemesis = new Thread(() -> runNemesis(killCycles, failures), "kill-nemesis");
        nemesis.setDaemon(true);
        nemesis.start();

        // Writes nobody will touch again, laid down throughout the run so some of them are
        // acknowledged by a quorum that is missing a member. Stopped once everything else is done.
        final AtomicBoolean writingWitnesses = new AtomicBoolean(true);
        final Thread witnessWriter = new Thread(
                () -> runWitnessWriter(writingWitnesses, failures), "kill-witness");
        witnessWriter.setDaemon(true);
        witnessWriter.start();

        final int opsPerClient = profile.scale(OPS_PER_CLIENT);
        final List<Thread> workers = new ArrayList<>();
        for (int c = 0; c < CLIENTS; c++) {
            final HistoryRecorder recorder = recorders.get(c);
            final long workerSeed = SEED + c * 9973L;
            final Thread worker = new Thread(
                    () -> runWorkload(recorder, opsPerClient, workerSeed, failures),
                    "kill-worker-" + c);
            workers.add(worker);
            worker.start();
        }
        for (final Thread worker : workers) {
            worker.join();
        }

        // The nemesis owes the run a fixed number of kills, so it is waited for rather than
        // stopped -- the workload and the pacing above are sized to overlap it, but neither
        // decides how much chaos this run contains.
        nemesis.join(READY_BUDGET.toMillis() + 60_000L);
        writingWitnesses.set(false);
        witnessWriter.join(READY_BUDGET.toMillis());

        // A worker or the nemesis that threw took its part of the run with it, and every assertion
        // below would be measuring a run that did not happen.
        assertTrue(failures.isEmpty(), () -> "the run did not complete: " + describe(failures));

        restoreCluster();

        final List<OpRecord> history = new ArrayList<>();
        for (final HistoryRecorder recorder : recorders) {
            history.addAll(recorder.snapshot());
        }
        history.addAll(settlingReads());

        // Guard the test's value, not just its stability: a history of nothing but UNKNOWN
        // operations is trivially linearizable, so a change that made every operation time out
        // would leave this green while checking nothing.
        int decidedCount = 0;
        for (final OpRecord record : history) {
            if (record.status() == OpRecord.Status.OK) {
                decidedCount++;
            }
        }
        final int decided = decidedCount;
        assertTrue(decided * 2 >= history.size(),
                "History is mostly undecided (" + decided + "/" + history.size()
                        + " decided) -- the checker would pass on it vacuously");

        assertEquals(killCycles, totalKills(),
                "The nemesis owes the run its kills; fewer means the cluster was left alone for "
                        + "part of it and the history says less than it appears to");
        assertTrue(witnesses.size() >= killCycles,
                "Too few witnesses were acknowledged (" + witnesses.size() + ") for the survival "
                        + "assertion to have crossed the kills it is about");

        // What a chaos run actually contained, printed because a reader deciding whether to
        // believe it cannot get any of these numbers from a green tick.
        System.out.println("[clusterSurvivesRandomKills] seed=" + Long.toHexString(SEED)
                + " kills=" + killsByNode() + " records=" + history.size()
                + " decided=" + decided + " witnesses=" + witnesses.size());

        assertWitnessesSurvived();

        final RegisterLinearizabilityChecker.Result result =
                new RegisterLinearizabilityChecker().check(history);
        assertTrue(result.linearizable(),
                () -> result.message() + "\nkills=" + killsByNode() + " records=" + history.size()
                        + " decided=" + decided + "\n" + logs());
    }


    /**
     * Start every member and wait for all of them to be ready. Peer addresses have to be chosen
     * before any member exists -- each validates the whole member list at construction -- and the
     * client and observability ports have to outlive a kill, since the client keeps dialing the
     * same address and the nemesis keeps polling the same endpoint.
     */
    private void startCluster(final Path rootDir) throws Exception {
        final List<Integer> peerPorts = TestPorts.allocate(NODES);
        final List<Integer> clientPorts = TestPorts.allocate(NODES);
        final List<Integer> observabilityPorts = TestPorts.allocate(NODES);

        final StringBuilder members = new StringBuilder();
        for (int i = 0; i < NODES; i++) {
            if (i > 0) {
                members.append(',');
            }
            members.append(i + 1).append("=127.0.0.1:").append(peerPorts.get(i));
        }

        for (int i = 0; i < NODES; i++) {
            nodes.add(new NodeProcess(i + 1, CLUSTER_ID, members.toString(),
                    clientPorts.get(i), observabilityPorts.get(i), rootDir));
        }
        for (final NodeProcess node : nodes) {
            node.start();
        }
        for (final NodeProcess node : nodes) {
            node.awaitReady(READY_BUDGET);
        }
    }

    private void startClients() {
        final Map<NodeId, InetSocketAddress> addresses = new HashMap<>();
        for (final NodeProcess node : nodes) {
            addresses.put(NodeId.of(Integer.toString(node.id())), node.clientAddress());
        }
        // The per-attempt timeout is the client's half of OP_TIMEOUT_MS: a coordinator that has
        // just been killed must be given up on with time left to reach a live one, or every kill
        // costs the run three undecided operations instead of one failover.
        final DisCasClientConfig config = DisCasClientConfig.builder()
                .perAttemptTimeout(Duration.ofMillis(OP_TIMEOUT_MS / 3))
                .requestDeadline(Duration.ofMillis(OP_TIMEOUT_MS))
                .build();
        // One more client than there are workload threads: the last one writes the witnesses and
        // reads them back, so its requests never share a connection with the recorded history.
        for (int i = 0; i <= CLIENTS; i++) {
            clients.add(DisCasClientFactory.create(
                    ClientId.of("kill-client-" + i),
                    new TcpClientBootstrap(addresses, ClientTransportConfig.defaults()),
                    config));
        }
    }

    /** Bring back whatever the nemesis left down, and wait for the whole cluster to be ready. */
    private void restoreCluster() throws Exception {
        for (final NodeProcess node : nodes) {
            if (!node.isAlive()) {
                node.start();
            }
        }
        for (final NodeProcess node : nodes) {
            node.awaitReady(READY_BUDGET);
        }
    }


    private void runWorkload(final HistoryRecorder recorder,
                             final int operations,
                             final long seed,
                             final List<Throwable> failures) {
        final Random rnd = new Random(seed);
        try {
            for (int i = 0; i < operations; i++) {
                final HashedBytes key = KEYS[rnd.nextInt(KEYS.length)];
                final int roll = rnd.nextInt(100);
                if (roll < 40) {
                    recorder.get(key);
                } else if (roll < 65) {
                    recorder.put(key, VALUES[rnd.nextInt(VALUES.length)]);
                } else if (roll < 90) {
                    final HashedBytes expected =
                            rnd.nextBoolean() ? VALUES[rnd.nextInt(VALUES.length)] : null;
                    recorder.cas(key, expected, VALUES[rnd.nextInt(VALUES.length)]);
                } else {
                    recorder.delete(key);
                }
                Thread.sleep(between(rnd, OP_PAUSE_MIN_MS, OP_PAUSE_MAX_MS));
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            failures.add(e);
        } catch (final Throwable e) {
            failures.add(e);
        }
    }

    /**
     * Kill a member, leave it down for a moment, start it again on the same directory and ports,
     * and wait until it is back in the quorum before choosing the next victim.
     * <p>
     * <b>Never the same member twice running.</b> A member that is never killed holds every
     * acknowledged write for the whole run, and a quorum containing it is right whatever the other
     * two lost -- so a nemesis that happened to leave one member alone would be checking the
     * cluster's availability and nothing about anyone's disk. Rotating means each acknowledged
     * write eventually has to come back off storage that was killed under it.
     */
    private void runNemesis(final int cycles, final List<Throwable> failures) {
        final Random rnd = new Random(SEED ^ 0xCAFEBABEL);
        int previous = -1;
        try {
            for (int cycle = 0; cycle < cycles; cycle++) {
                int index = rnd.nextInt(nodes.size());
                if (index == previous) {
                    index = (index + 1 + rnd.nextInt(nodes.size() - 1)) % nodes.size();
                }
                previous = index;
                final NodeProcess victim = nodes.get(index);
                victim.killNine();
                Thread.sleep(between(rnd, OUTAGE_MIN_MS, OUTAGE_MAX_MS));
                victim.start();
                victim.awaitReady(READY_BUDGET);
                Thread.sleep(between(rnd, CALM_MIN_MS, CALM_MAX_MS));
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            failures.add(e);
        } catch (final Throwable e) {
            failures.add(e);
        }
    }

    /**
     * Write a key, wait for its acknowledgement, and never touch it again. One at a time, so every
     * witness recorded here is one the cluster answered for -- an operation that failed or timed out
     * is skipped rather than recorded, because it may or may not have applied and a witness that
     * might legitimately be absent asserts nothing.
     * <p>
     * Each witness gets a key of its own for the same reason: a retry on a shared key could not be
     * told apart from the write it was retrying.
     */
    private void runWitnessWriter(final AtomicBoolean writing, final List<Throwable> failures) {
        final DisCasClient client = clients.get(clients.size() - 1);
        try {
            for (int i = 0; writing.get(); i++) {
                final String key = "witness-" + i;
                final String value = "w" + i;
                try {
                    client.put(buffer(key), buffer(value)).get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    witnesses.put(key, value);
                } catch (final ExecutionException | TimeoutException indeterminate) {
                    // Not an error: the cluster is being killed under this write. It is simply not
                    // a witness, because nothing was promised about it.
                }
                Thread.sleep(WITNESS_PAUSE_MS);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            failures.add(e);
        } catch (final Throwable e) {
            failures.add(e);
        }
    }

    /**
     * Every acknowledged witness must still be there, with the value it was acknowledged for.
     * <p>
     * This is the durability half of the claim, and it is the half the history cannot make: a key
     * the workload keeps touching is repaired by the next round that touches it, so it says more
     * about the cluster still working than about anything that was written to a disk.
     */
    private void assertWitnessesSurvived() throws Exception {
        final DisCasClient client = clients.get(clients.size() - 1);
        final List<String> lost = new ArrayList<>();
        for (final Map.Entry<String, String> witness : witnesses.entrySet()) {
            final String observed = readWithRetry(client, witness.getKey());
            if (!witness.getValue().equals(observed)) {
                lost.add(witness.getKey() + ": acknowledged " + witness.getValue()
                        + ", read back " + observed);
            }
        }
        assertTrue(lost.isEmpty(),
                () -> "acknowledged writes did not survive the kills:\n"
                        + String.join("\n", lost) + "\n" + logs());
    }

    /**
     * A read of a whole, idle cluster, retried once. The retry is not about the store: it is about
     * refusing to report a witness as lost on the strength of one request that did not come back,
     * which is a different claim entirely.
     */
    private static String readWithRetry(final DisCasClient client, final String key)
            throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                final ByteBuffer read = client.get(buffer(key))
                        .get(OP_TIMEOUT_MS * 3, TimeUnit.MILLISECONDS).value();
                return read == null ? null : text(read);
            } catch (final ExecutionException | TimeoutException e) {
                last = e;
            }
        }
        throw new IllegalStateException("Reading witness " + key + " failed twice on a cluster "
                + "that is whole and idle", last);
    }

    private static ByteBuffer buffer(final String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(final ByteBuffer buffer) {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** A value in {@code [min, max]} from a seeded {@link Random}, which has no ranged nextLong. */
    private static long between(final Random rnd, final long min, final long max) {
        return min + (long) (rnd.nextDouble() * (max - min));
    }

    /**
     * One more read of every workload key, recorded, after the cluster is whole again. It closes the
     * history: without it the last operations on each key are unconstrained by anything, and a
     * checker asked about a register nobody read again has less to disagree with.
     * <p>
     * It is not where lost data is caught -- see the class javadoc on why a key the workload keeps
     * touching heals itself, and what the witnesses are for.
     * <p>
     * Retried until each read is decided, because an {@code UNKNOWN} read constrains nothing: it
     * would withdraw the little these reads do add, silently.
     */
    private List<OpRecord> settlingReads() {
        final HistoryRecorder recorder = new HistoryRecorder(clients.get(0), OP_TIMEOUT_MS * 3);
        for (final HashedBytes key : KEYS) {
            boolean decided = false;
            for (int attempt = 0; attempt < 5 && !decided; attempt++) {
                recorder.get(key);
                final List<OpRecord> soFar = recorder.snapshot();
                decided = soFar.get(soFar.size() - 1).status() == OpRecord.Status.OK;
            }
            assertTrue(decided, "The cluster is whole and idle, yet a read of " + key
                    + " would not settle -- there is nothing to check a history against\n" + logs());
        }
        return recorder.snapshot();
    }


    private int totalKills() {
        int total = 0;
        for (final NodeProcess node : nodes) {
            total += node.kills();
        }
        return total;
    }

    private String killsByNode() {
        final StringBuilder sb = new StringBuilder();
        for (final NodeProcess node : nodes) {
            sb.append(sb.length() == 0 ? "" : " ").append(node.id()).append(':').append(node.kills());
        }
        return sb.toString();
    }

    /** Every member's log, for a failure whose cause is on the other side of a process boundary. */
    private String logs() {
        final StringBuilder sb = new StringBuilder();
        for (final NodeProcess node : nodes) {
            sb.append(node.logTail()).append('\n');
        }
        return sb.toString();
    }

    private static String describe(final List<Throwable> failures) {
        final StringBuilder sb = new StringBuilder();
        for (final Throwable failure : failures) {
            final StringWriter writer = new StringWriter();
            failure.printStackTrace(new PrintWriter(writer));
            sb.append(writer).append('\n');
        }
        return sb.toString();
    }
}
