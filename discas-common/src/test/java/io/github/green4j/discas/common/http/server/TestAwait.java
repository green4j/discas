/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Waiting for a condition in the HTTP server tests: one budget, one clock, one failure type, so it
 * cannot depend on which file a test is in.
 * <p>
 * Mirrors {@code io.github.green4j.discas.TestAwait} in discas-int-test, which the cluster tests
 * use; the two test trees cannot see each other, so change both together.
 */
final class TestAwait {

    /** How long a condition gets. Generous enough for a loaded CI box, short enough to fail fast. */
    static final Duration BUDGET = Duration.ofSeconds(5);

    /** Gap between checks. */
    static final Duration POLL_INTERVAL = Duration.ofMillis(10);

    private TestAwait() {
    }

    /** Poll until {@code events} contains {@code value}; the routers' event log is the condition. */
    static void untilPresent(final java.util.List<String> events, final String value)
            throws InterruptedException {
        until(value + "; events=" + events, () -> events.contains(value));
    }

    /** Poll {@code condition} on {@link #BUDGET} until it holds. */
    static void until(final String what, final BooleanSupplier condition) throws InterruptedException {
        until(what, BUDGET, condition);
    }

    /**
     * Poll {@code condition} until it holds.
     *
     * @throws AssertionError if it never does
     */
    static void until(final String what, final Duration budget, final BooleanSupplier condition)
            throws InterruptedException {
        final long deadline = System.nanoTime() + budget.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        throw new AssertionError("Timed out after " + budget + " waiting for " + what);
    }
}
