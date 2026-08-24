/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.DisCasClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Waiting for a cluster to come up.
 * <p>
 * Eight tests carried their own copy of this loop, under four names, with two incompatible
 * budgets (5s and 60s), four spellings of the probe key and four wordings of the failure. A test
 * that flakes because it waited five seconds where its neighbour waits sixty is not telling you
 * anything about the code.
 */
public final class TestAwait {

    /**
     * How long a cluster gets to answer its first request. Generous on purpose: this covers
     * process start, the peer mesh forming and the first Paxos round, on a machine that may be
     * running several test JVMs at once.
     */
    public static final Duration READY_BUDGET = Duration.ofSeconds(60);

    /** Gap between attempts. Short: readiness is usually one retry away. */
    public static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    /** Per-attempt timeout on the probe request itself. */
    public static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);

    /**
     * The key a readiness probe reads by default. Never written, so the read is a pure liveness
     * check on the read path -- and one spelling means one key, rather than a different unused
     * key per test file.
     * <p>
     * A cluster with client authorization enabled needs a key inside a prefix the probing client
     * is granted, or the probe is denied rather than merely unanswered; those tests pass their
     * own through {@link #awaitReady(DisCasClient, String)}.
     */
    public static final String PROBE_KEY = "__ready_probe__";

    private TestAwait() {
    }

    /** A probe that either completes or throws; anything thrown means "not ready yet". */
    @FunctionalInterface
    public interface Probe {
        void run() throws Exception;
    }

    /**
     * A client is ready once a read completes against the cluster it is pointed at.
     *
     * @throws IllegalStateException if the cluster does not answer within {@link #READY_BUDGET}
     */
    public static void awaitReady(final DisCasClient client) throws Exception {
        awaitReady(client, PROBE_KEY);
    }

    /**
     * As {@link #awaitReady(DisCasClient)}, reading {@code probeKey} instead -- for a cluster
     * whose client ACL grants only a particular prefix.
     */
    public static void awaitReady(final DisCasClient client, final String probeKey) throws Exception {
        final ByteBuffer key = ByteBuffer.wrap(probeKey.getBytes(StandardCharsets.UTF_8));
        until("the cluster to answer a read of " + probeKey, () ->
                client.get(key.duplicate())
                        .get(PROBE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    }

    /**
     * Wait until {@code sample} has returned the same value for {@code quiet}, so a count taken
     * afterwards is not a race with work still in flight.
     * <p>
     * The alternative -- sleeping a constant and hoping -- is a guess about how fast the machine is,
     * and it fails in the direction that looks like a product bug: a broadcast that had not landed
     * yet reads as one that never will. This waits for the system to go quiet instead, so a slow
     * machine takes longer rather than failing.
     *
     * @throws IllegalStateException if it never settles within {@code budget}
     */
    public static <T> T untilStable(final String what, final Supplier<T> sample,
                                    final Duration quiet, final Duration budget) throws Exception {
        final long deadline = System.nanoTime() + budget.toNanos();
        T last = sample.get();
        long stableSince = System.nanoTime();
        while (System.nanoTime() < deadline) {
            Thread.sleep(POLL_INTERVAL.toMillis());
            final T now = sample.get();
            if (!Objects.equals(now, last)) {
                last = now;
                stableSince = System.nanoTime();
            } else if (System.nanoTime() - stableSince >= quiet.toNanos()) {
                return now;
            }
        }
        throw new IllegalStateException(
                "Timed out after " + budget + " waiting for " + what + " to settle (last " + last + ")");
    }

    /** How long a value must hold still to count as settled. */
    public static final Duration QUIET = Duration.ofMillis(200);

    /** Poll {@code probe} on {@link #READY_BUDGET} until it completes without throwing. */
    public static void until(final String what, final Probe probe) throws Exception {
        until(what, READY_BUDGET, probe);
    }

    /**
     * Poll {@code probe} until it completes without throwing.
     *
     * @throws IllegalStateException if it never does, carrying the last failure as the cause
     */
    public static void until(final String what, final Duration budget, final Probe probe)
            throws Exception {
        final long deadline = System.nanoTime() + budget.toNanos();
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                probe.run();
                return;
            } catch (final Exception notYet) {
                last = notYet;
                Thread.sleep(POLL_INTERVAL.toMillis());
            }
        }
        throw new IllegalStateException(
                "Timed out after " + budget + " waiting for " + what, last);
    }
}
