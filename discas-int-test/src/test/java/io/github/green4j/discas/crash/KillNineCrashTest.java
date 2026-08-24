/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.crash;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.Acceptor;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.LocalStore;
import io.github.green4j.discas.node.PeerMessage;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import io.github.green4j.discas.node.wal.StorageFormat;
import io.github.green4j.discas.node.wal.WalFileName;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a real {@code SIGKILL} leaves behind, and what the node makes of it.
 *
 * <p>Every other durability test in this suite simulates the crash: entries are dropped in memory,
 * or a node is closed and started again in the same JVM, running every orderly path a crash skips.
 * These kill a separate process with {@code kill -9} and then open the directory it left. The exit
 * code is asserted to be {@code 137} -- {@code 128 + SIGKILL} -- because a test that quietly let the
 * process exit cleanly would be making a much weaker claim than its name.
 *
 <h2>What SIGKILL is and is not</h2>
 * It runs no shutdown hook, forces nothing, seals no segment and removes no temp file -- but the
 * kernel keeps the page cache, so bytes that reached {@code write()} still reach the platter. That
 * makes it a crashed <em>process</em>, not a lost machine.
 * <p>
 * The lost machine is the harsher fault, and the one promise durability is about: the promise
 * test below adds it by truncating the log to the last offset the subject
 * reported as forced, which is exactly what a power cut takes. It is not a second version of the
 * same test -- truncation only removes a suffix of what SIGKILL left, so the stronger fault implies
 * the weaker one and one test covers both.
 *
 * @see CrashSubject the process being killed
 */
@Tag("chaos")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
@DisplayName("Crash -- what survives kill -9")
class KillNineCrashTest {

    private static final Duration STARTUP_BUDGET = Duration.ofSeconds(60);
    private static final NodeId PROPOSER = NodeId.of("p");

    @Test
    @DisplayName("A promise acknowledged before the crash is still refused after it")
    void promisesSurviveTheCrash(@TempDir final Path dir) throws Exception {
        final Subject subject = start(dir, CrashSubject.Mode.PROMISES);
        final long highestPromised;
        final long forcedBytes;
        try {
            highestPromised = subject.awaitPromisedAtLeast(20);
            forcedBytes = subject.lastForcedBytes();
        } finally {
            subject.killNine();
        }

        // Both faults in one, because the harsher one implies the other: truncation removes a suffix
        // of what SIGKILL left, so a floor that covers every acknowledged promise here covers it
        // without the truncation too. Only unforced bytes are dropped -- dropping forced ones would
        // be a claim about the disk lying, which no protocol survives and none of this promises.
        final long dropped = truncateActiveSegmentTo(dir, forcedBytes);
        assertTrue(dropped > 0,
                "This test is only about promises whose record did not reach the platter, and the"
                        + " subject was killed with none in flight");

        final Recovered recovered = recover(dir);
        assertTrue(recovered.floor >= highestPromised,
                "A promise acknowledged before the crash must still be covered: floor="
                        + recovered.floor + " highest acknowledged=" + highestPromised
                        + " dropped=" + dropped + " bytes");
        assertTrue(recovered.ceilingProven,
                "A log that lost its tail is short, not broken: replay must be able to account"
                        + " for it");

        // The load-bearing consequence, and the reason the floor exists at all.
        final Acceptor acceptor = new Acceptor(NodeId.of("crash"), recovered.store);
        assertFalse(acceptor.handlePrepare(prepare(highestPromised)).ok(),
                "The promise whose record was just lost is exactly what the ceiling is for");
        assertFalse(acceptor.handlePrepare(prepare(recovered.floor)).ok(),
                "The whole band up to the floor is refused, since any of it may have been promised");
        assertTrue(acceptor.handlePrepare(prepare(recovered.floor + 1)).ok(),
                "And the first ballot above the floor is available, or the node is stuck");
    }

    @Test
    @DisplayName("Killed mid-snapshot: the committed values are still there and the node starts")
    void snapshotInterruptedByTheKill(@TempDir final Path dir) throws Exception {
        final Subject subject = start(dir, CrashSubject.Mode.COMMITTED_THEN_SNAPSHOT);
        try {
            subject.awaitLine("READY");
        } finally {
            subject.killNine();
        }

        // A partial snapshot is left on disk with no committed footer, and the node has to ignore it
        // and replay the tail instead. Both halves matter: starting at all, and starting with the
        // values that were acknowledged before the kill.
        final Recovered recovered = recover(dir);
        assertEquals(200, recovered.store.keyCount() > 0 ? countCommitted(recovered.store) : 0,
                "Every committed value must survive: a snapshot that never finished is not a"
                        + " substitute for the log, it is an optimisation over it");
        assertTrue(recovered.ceilingProven, "The log is intact, so the ceiling is provable");
    }

    @Test
    @DisplayName("Killed during its first start: a marker and nothing else is a node without state")
    void killedBeforeWritingAnything(@TempDir final Path dir) throws Exception {
        final Subject subject = start(dir, CrashSubject.Mode.JUST_INITIALIZED);
        try {
            subject.awaitLine("READY");
        } finally {
            subject.killNine();
        }

        // The directory now carries an incarnation marker and an empty log. That is not a failure to
        // start: it is a node that cannot prove a ceiling, which is answered by asking the cluster.
        final Recovered recovered = recover(dir);
        assertFalse(recovered.ceilingProven,
                "Nothing was ever recorded, so there is no ceiling this node can prove");
        assertEquals(0L, recovered.floor);
    }


    private Subject start(final Path dir, final CrashSubject.Mode mode) throws Exception {
        final Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        final ProcessBuilder builder = new ProcessBuilder(
                java.toString(),
                "-cp", System.getProperty("java.class.path"),
                CrashSubject.class.getName(),
                dir.toString(),
                mode.name());
        builder.redirectErrorStream(false);
        return new Subject(builder.start());
    }

    /** A child process, its stdout, and the one way this test is allowed to stop it. */
    private static final class Subject {
        private static final int SIGKILL_EXIT = 137;   // 128 + 9

        private final Process process;
        private final BufferedReader out;
        private final List<String> seen = new ArrayList<>();

        private Subject(final Process process) {
            this.process = process;
            this.out = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        }

        private String awaitLine(final String prefix) throws Exception {
            final long deadline = System.nanoTime() + STARTUP_BUDGET.toNanos();
            while (System.nanoTime() < deadline) {
                final String line = out.readLine();
                if (line == null) {
                    throw new IllegalStateException(
                            "The subject exited before printing " + prefix + ", saw " + seen);
                }
                seen.add(line);
                if (line.startsWith(prefix)) {
                    return line;
                }
            }
            throw new IllegalStateException(
                    "Timed out waiting for " + prefix + " from the subject, saw " + seen);
        }

        /** Read until the subject has acknowledged at least this ballot, and report the highest. */
        private long awaitPromisedAtLeast(final long counter) throws Exception {
            long highest = 0;
            while (highest < counter) {
                highest = Long.parseLong(awaitLine("PROMISED ").substring("PROMISED ".length()));
            }
            return highest;
        }

        /** WAL bytes as of the last fsync the subject reported; everything after it is unforced. */
        private long lastForcedBytes() {
            long bytes = -1;
            for (final String line : seen) {
                if (line.startsWith("FORCED ")) {
                    bytes = Long.parseLong(line.split(" ")[1]);
                }
            }
            if (bytes < 0) {
                throw new IllegalStateException("The subject never reported a forced offset: " + seen);
            }
            return bytes;
        }

        /**
         * {@code kill -9}, by name rather than by {@code destroyForcibly()}, so what this test does
         * is not a detail of the JDK's process API. The exit code is checked because a subject that
         * managed to exit any other way would have run the orderly paths this test exists to skip.
         */
        private void killNine() throws Exception {
            if (!process.isAlive()) {
                throw new IllegalStateException(
                        "The subject died on its own (exit " + process.exitValue() + "): " + seen);
            }
            new ProcessBuilder("kill", "-9", Long.toString(process.pid())).start().waitFor();
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "The subject must be gone");
            assertEquals(SIGKILL_EXIT, process.exitValue(),
                    "The subject must have died of SIGKILL, or this test proves less than it says");
        }
    }


    /** Recover the directory the way a node does, and report what the replay established. */
    private static Recovered recover(final Path dir) throws Exception {
        final FileWal wal = new FileWal(StorageConfig.builder().baseDirectory(dir).build());
        wal.initialize();
        final LocalStore store = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        while (loader.loadBatch(256)) {
            // drain
        }
        return new Recovered(store, store.recoveryPromiseFloor(), wal.ceilingIsProven());
    }

    private static final class Recovered {
        private final LocalStore store;
        private final long floor;
        private final boolean ceilingProven;

        private Recovered(final LocalStore store, final long floor, final boolean ceilingProven) {
            this.store = store;
            this.floor = floor;
            this.ceilingProven = ceilingProven;
        }
    }

    /**
     * Drop everything the active segment holds past {@code forcedBytes} -- the page cache going with
     * the machine.
     * <p>
     * Only the active segment: a segment is forced before it is sealed, so every sealed one is
     * already on the platter, and cutting into those would be modelling a disk that lost
     * acknowledged data rather than a machine that stopped.
     * <p>
     * This is the only place the test touches storage at all, and it reads nothing: the two facts it
     * needs come from the code that owns them -- which file is active from {@link WalFileName}, and
     * where the sync boundary lies from {@link FileWal#forcedBytes()}. Parsing records here would
     * mean asserting against the test's own copy of the format instead of the format.
     *
     * @return how many bytes were dropped
     */
    private static long truncateActiveSegmentTo(final Path dir, final long forcedBytes)
            throws Exception {
        final List<Path> open = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir.resolve("wal"))) {
            files.filter(Files::isRegularFile).forEach(path -> {
                final WalFileName parsed = WalFileName.parse(
                        StorageFormat.LAYOUT_VERSION, path.getFileName().toString());
                if (parsed != null && parsed.type() == WalFileName.Type.OPEN) {
                    open.add(path);
                }
            });
        }
        assertEquals(1, open.size(), "Exactly one segment should be open when the subject died");

        final Path active = open.get(0);
        final long size = Files.size(active);
        if (size <= forcedBytes) {
            return 0;
        }
        try (FileChannel channel = FileChannel.open(active, StandardOpenOption.WRITE)) {
            channel.truncate(forcedBytes);
        }
        return size - forcedBytes;
    }

    private static int countCommitted(final LocalStore store) {
        int committed = 0;
        for (int i = 0; i < 200; i++) {
            final HashedBytes key = HashedBytes.copyOf(ByteBuffer.wrap(
                    ("k" + (i % 32)).getBytes(StandardCharsets.UTF_8)));
            if (store.get(key) != null && !store.get(key).accepted.isZero()) {
                committed++;
            }
        }
        return committed;
    }

    private static PeerMessage.PrepareReq prepare(final long counter) {
        return new PeerMessage.PrepareReq(PROPOSER, counter,
                HashedBytes.copyOf(ByteBuffer.wrap("probe".getBytes(StandardCharsets.UTF_8))),
                new Ballot(counter, PROPOSER));
    }
}
