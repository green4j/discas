/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.client.GetResult;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class ChaosWorkload {
    static final class Result {
        final Map<String, AtomicLong> successfulCounterSwaps = new ConcurrentHashMap<>();
        final List<Throwable> errors = new CopyOnWriteArrayList<>();
        final AtomicLong operations = new AtomicLong();
    }

    private final ChaosClusterHarness harness;
    private final List<String> counterKeys;
    private final List<String> regularKeys;
    private final int threadCount;
    private final long seed;
    private final Duration opAwaitTimeout;
    private final Duration scanAwaitTimeout;
    private final int casAttempts;

    ChaosWorkload(final ChaosClusterHarness harness,
                  final List<String> counterKeys,
                  final List<String> regularKeys,
                  final int threadCount,
                  final long seed,
                  final Duration opAwaitTimeout,
                  final Duration scanAwaitTimeout,
                  final int casAttempts) {
        this.harness = harness;
        this.counterKeys = counterKeys;
        this.regularKeys = regularKeys;
        this.threadCount = threadCount;
        this.seed = seed;
        this.opAwaitTimeout = opAwaitTimeout;
        this.scanAwaitTimeout = scanAwaitTimeout;
        this.casAttempts = casAttempts;
    }

    Result run(final Duration duration, final AtomicBoolean stopSignal) throws InterruptedException {
        final Result result = new Result();
        for (final String key : counterKeys) {
            result.successfulCounterSwaps.put(key, new AtomicLong());
        }

        final long deadlineNanos = System.nanoTime() + duration.toNanos();
        final CountDownLatch done = new CountDownLatch(threadCount);
        final List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int idx = t;
            final Thread thread = new Thread(() -> {
                final Random random = new Random(seed + idx * 9973L);
                final DisCasClient client = harness.client(idx % harness.clients().size());
                try {
                    while (!stopSignal.get() && System.nanoTime() - deadlineNanos < 0) {
                        final int op = random.nextInt(100);
                        if (op < 60) {
                            doCounterCas(client, random, result);
                        } else if (op < 75) {
                            doPut(client, random, result);
                        } else if (op < 85) {
                            doDelete(client, random, result);
                        } else if (op < 95) {
                            doGet(client, random, result);
                        } else {
                            doScan(client, result);
                        }
                        result.operations.incrementAndGet();
                    }
                } catch (final Throwable e) {
                    result.errors.add(e);
                } finally {
                    done.countDown();
                }
            }, "chaos-workload-" + t);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }

        done.await(duration.toMillis() + 60_000L, TimeUnit.MILLISECONDS);
        return result;
    }

    private void doCounterCas(final DisCasClient client, final Random random, final Result result) {
        final String key = counterKeys.get(random.nextInt(counterKeys.size()));
        final AtomicLong successes = result.successfulCounterSwaps.get(key);
        for (int attempt = 0; attempt < casAttempts; attempt++) {
            try {
                // getVersioned, not get: the fenced CAS below needs the version, and taking it
                // from the read that has to happen anyway keeps this at two round trips. Deriving a
                // value precondition instead and re-reading inside a helper made it three, and
                // under a nemesis every extra round trip is another chance to time out.
                final GetResult current = client
                        .get(buf(key), ReadConsistency.LINEARIZABLE)
                        .get(opAwaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
                final Integer currentValue = parseInt(current.value());
                final int next = currentValue == null ? 1 : currentValue + 1;
                final CasResult casResult = client
                        .cas(buf(key), current.version(), buf(Integer.toString(next)))
                        .get(opAwaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
                if (casResult.swapped()) {
                    successes.incrementAndGet();
                    return;
                }
            } catch (final Exception e) {
                if (attempt == casAttempts - 1) {
                    result.errors.add(e);
                }
            }
        }
    }

    private void doPut(final DisCasClient client, final Random random, final Result result) {
        final String key = regularKeys.get(random.nextInt(regularKeys.size()));
        final String value = "v-" + random.nextInt(1_000_000);
        try {
            client.put(buf(key), buf(value)).get(opAwaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            result.errors.add(e);
        }
    }

    private void doDelete(final DisCasClient client, final Random random, final Result result) {
        final String key = regularKeys.get(random.nextInt(regularKeys.size()));
        try {
            client.delete(buf(key)).get(opAwaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            result.errors.add(e);
        }
    }

    private void doGet(final DisCasClient client, final Random random, final Result result) {
        final String key = regularKeys.get(random.nextInt(regularKeys.size()));
        try {
            client.get(buf(key)).get(opAwaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            result.errors.add(e);
        }
    }

    private void doScan(final DisCasClient client, final Result result) {
        try {
            client.scan().get(scanAwaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            result.errors.add(e);
        }
    }

    static ByteBuffer buf(final String value) {
        return ByteBuffer.wrap(value.getBytes());
    }

    static String str(final ByteBuffer value) {
        if (value == null) {
            return null;
        }
        final byte[] bytes = new byte[value.remaining()];
        value.duplicate().get(bytes);
        return new String(bytes);
    }

    static Integer parseInt(final ByteBuffer value) {
        final String s = str(value);
        if (s == null) {
            return null;
        }
        return Integer.parseInt(s);
    }
}
