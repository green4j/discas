/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

final class ChaosNemesis {
    static final class Result {
        final List<Throwable> errors = new CopyOnWriteArrayList<>();
        int restartEvents = 0;
    }

    private final ChaosClusterHarness harness;
    private final long seed;

    ChaosNemesis(final ChaosClusterHarness harness, final long seed) {
        this.harness = harness;
        this.seed = seed;
    }

    Result run(final Duration duration,
               final Duration restartIntervalMin,
               final Duration restartIntervalMax,
               final Duration outageMin,
               final Duration outageMax,
               final AtomicBoolean stopSignal) throws InterruptedException {
        final Result result = new Result();
        final Random random = new Random(seed);
        final long deadlineNanos = System.nanoTime() + duration.toNanos();
        final Thread thread = new Thread(() -> {
            try {
                while (!stopSignal.get() && System.nanoTime() - deadlineNanos < 0) {
                    sleep(randomBetween(random, restartIntervalMin, restartIntervalMax));
                    if (stopSignal.get() || System.nanoTime() - deadlineNanos >= 0) {
                        return;
                    }
                    final int nodeId = 1 + random.nextInt(3);
                    final Duration outage = randomBetween(random, outageMin, outageMax);
                    harness.restartNode(nodeId, outage);
                    result.restartEvents++;
                }
            } catch (final Throwable e) {
                result.errors.add(e);
            }
        }, "chaos-nemesis");
        thread.setDaemon(true);
        thread.start();
        thread.join(duration.toMillis() + 120_000L);
        return result;
    }

    private static Duration randomBetween(final Random random, final Duration min, final Duration max) {
        final long minMs = min.toMillis();
        final long maxMs = max.toMillis();
        if (maxMs <= minMs) {
            return Duration.ofMillis(minMs);
        }
        return Duration.ofMillis(minMs + random.nextInt((int) (maxMs - minMs + 1)));
    }

    private static void sleep(final Duration duration) {
        try {
            Thread.sleep(Math.max(0L, duration.toMillis()));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Nemesis interrupted", e);
        }
    }
}
