/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.WatchResult;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Following a key without polling it yourself: a watcher on one client and a writer on another.
 * <p>
 * <b>Watch is coalescing, and that is the whole thing worth understanding about it.</b> A key here
 * is a CASPaxos register with no history to replay, so a watch reports the <em>current</em> value
 * whenever the version has moved past the one you asked from. It is not an event stream: under
 * churn a watcher sees the latest value, not every value, and the writes it never observed left no
 * trace it could have read. Scenario 2 below makes that visible by counting -- a burst of commits
 * against the number of times the watcher woke.
 * <p>
 * Three things a caller has to know, one scenario each:
 * <ul>
 *   <li><b>{@link Version#INITIAL} fires on the first committed value.</b> An existing key returns
 *       at once; an absent one blocks until it appears, which is how you wait for a key rather
 *       than poll for it.</li>
 *   <li><b>The returned version is where you watch from next.</b> Feed {@link WatchResult#version()}
 *       back in
 *       to keep following. That is the whole loop; there is no subscription to hold open and
 *       nothing on the server remembering you.</li>
 *   <li><b>"Nothing changed" is an answer, not a failure.</b> When {@code maxWait} elapses the
 *       future completes normally with {@link WatchResult#changed()} false and the version you
 *       passed in. Re-arm and carry on.</li>
 * </ul>
 * <p>
 * Deliberately not wired into {@code runAssertingExamples}: scenario 2 is timing-shaped by
 * construction, and a build task whose result depends on how fast a burst runs is a flaky build.
 * The claims that do not depend on timing are checked here anyway, so running it by hand still
 * fails loudly.
 */
public final class WatchExample {

    private static final long CALL_MS = 15_000L;
    private static final Duration MAX_WAIT = Duration.ofSeconds(5);
    /** Short on purpose: scenario 3 has to actually wait this out. */
    private static final Duration QUIET_WAIT = Duration.ofSeconds(1);
    private static final int BURST = 20;

    private WatchExample() {
    }

    public static void main(final String[] args) throws Exception {
        try (ExampleCluster.RunningCluster cluster = ExampleCluster.startInProcessCluster(2)) {
            final DisCasClient watcher = cluster.client(0);
            final DisCasClient writer = cluster.client(1);
            final ByteBuffer key = ExampleBytes.encode("config/feature-flags");

            System.out.println("=== Watch: 3 in-process nodes, a watcher and a writer ===");

            final Version afterCreate = waitForAKeyThatDoesNotExistYet(watcher, writer, key);
            final Version afterBurst = burstOfWritesIsCoalesced(watcher, writer, key, afterCreate);
            nothingChangedIsAnAnswer(watcher, key, afterBurst);

            System.out.println("=== done ===");
        }
    }

    /**
     * Scenario 1. Watching from {@link Version#INITIAL} blocks on an absent key until somebody
     * creates it -- a wait, not a poll loop the caller has to write.
     */
    private static Version waitForAKeyThatDoesNotExistYet(final DisCasClient watcher,
                                                          final DisCasClient writer,
                                                          final ByteBuffer key) throws Exception {
        System.out.println("1. the key does not exist; watching from Version.INITIAL");

        // Armed before the write, so what returns is the watch firing rather than a read that
        // happened to be late.
        final CompletableFuture<WatchResult> watching =
                watcher.watch(key.duplicate(), Version.INITIAL, MAX_WAIT);

        Thread.sleep(300L);
        ExampleOutcome.of(writer.put(key.duplicate(), ExampleBytes.encode("enabled=false")), CALL_MS)
                .require("writer creates the key");

        final WatchResult fired = ExampleOutcome.of(watching, CALL_MS).require("watch from INITIAL");
        require(fired.changed(), "a key appearing must wake a watch armed at INITIAL");
        require(fired.exists(), "the key exists once it has been written");
        System.out.println("   woke with " + ExampleBytes.decode(fired.value())
                + " @ " + fired.version().token());
        return fired.version();
    }

    /**
     * Scenario 2. The register keeps no history, so a watcher under churn converges on the latest
     * value rather than replaying the ones it missed.
     */
    private static Version burstOfWritesIsCoalesced(final DisCasClient watcher,
                                                    final DisCasClient writer,
                                                    final ByteBuffer key,
                                                    final Version from) throws Exception {
        System.out.println("2. " + BURST + " commits as fast as the writer can make them");

        final String last = "enabled=true;rev=" + BURST;
        final Thread writing = new Thread(() -> {
            for (int i = 1; i <= BURST; i++) {
                ExampleOutcome.of(writer.put(key.duplicate(),
                        ExampleBytes.encode("enabled=true;rev=" + i)), CALL_MS).require("burst write");
            }
        }, "burst-writer");
        writing.start();

        // Follow the key by feeding each result's version back in. The loop ends when the watcher
        // sees the writer's final value, which it is guaranteed to reach: every wake-up reports
        // the current value, and the last write is current once the burst is over.
        int wakeUps = 0;
        Version at = from;
        String seen;
        do {
            final WatchResult result = ExampleOutcome
                    .of(watcher.watch(key.duplicate(), at, MAX_WAIT), CALL_MS)
                    .require("watch during the burst");
            require(result.changed(), "the burst must move the version past " + at.token());
            at = result.version();
            seen = ExampleBytes.decode(result.value());
            wakeUps++;
        } while (!last.equals(seen));
        writing.join();

        System.out.println("   " + BURST + " commits, " + wakeUps + " wake-up(s); the watcher ended "
                + "at " + seen + " @ " + at.token());
        System.out.println("   the " + (BURST - wakeUps) + " it did not observe left nothing behind "
                + "to read -- that is what coalescing means, and why a watch is not an event log.");
        return at;
    }

    /** Scenario 3. A watch that times out reports no news, and reports it normally. */
    private static void nothingChangedIsAnAnswer(final DisCasClient watcher,
                                                 final ByteBuffer key,
                                                 final Version from) throws Exception {
        System.out.println("3. nobody writes for " + QUIET_WAIT.getSeconds() + "s");

        final WatchResult quiet = ExampleOutcome
                .of(watcher.watch(key.duplicate(), from, QUIET_WAIT), CALL_MS)
                .require("watch with no writer");

        require(!quiet.changed(), "an elapsed maxWait must not claim a change");
        require(from.token().equals(quiet.version().token()),
                "an unchanged key must hand back the version it was given");
        System.out.println("   changed=" + quiet.changed() + ", still @ " + quiet.version().token()
                + " -- re-arm from this version and carry on");
    }

    private static void require(final boolean condition, final String what) {
        if (!condition) {
            throw new IllegalStateException("Claim does not hold: " + what);
        }
    }
}
