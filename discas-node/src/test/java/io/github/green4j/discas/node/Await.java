/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import java.time.Duration;

/**
 * Waiting for a condition, with a deadline -- the node module's counterpart to {@code TestAwait} in
 * the integration tests, without its cluster and client machinery.
 *
 * <p><b>Why a test must not sleep a constant instead.</b> A fixed pause is a guess about how fast
 * the machine is. Guess low and the test fails in CI for a reason it is not testing; guess high and
 * every run pays for it. Waiting for the condition is both faster in the ordinary case and correct
 * in the slow one, and the budget is only ever reached when something is genuinely wrong.
 *
 * <p>The exception, and the only one: a test whose subject <em>is</em> the passage of time -- one
 * asserting that nothing happened in a window, or holding a fault open for a given duration. There
 * the elapsed time is the substance rather than a stand-in for a condition, and a sleep says so.
 */
public final class Await {

    /** Generous on purpose: only reached when the condition never holds. */
    public static final Duration BUDGET = Duration.ofSeconds(30);
    private static final Duration POLL = Duration.ofMillis(10);

    private Await() {
    }

    public interface Probe {
        void run() throws Exception;
    }

    public static void until(final String what, final Probe probe) throws Exception {
        until(what, BUDGET, probe);
    }

    /**
     * Poll {@code probe} until it completes without throwing.
     * <p>
     * Note that an assertion failure is an {@link Error}, not an {@link Exception}, so it escapes
     * rather than being retried: a probe must signal "not yet" by throwing a plain exception.
     *
     * @throws IllegalStateException if it never does, carrying the last failure as the cause
     */
    public static void until(final String what, final Duration budget, final Probe probe) throws Exception {
        final long deadline = System.nanoTime() + budget.toNanos();
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                probe.run();
                return;
            } catch (final Exception notYet) {
                last = notYet;
                Thread.sleep(POLL.toMillis());
            }
        }
        throw new IllegalStateException("Timed out after " + budget + " waiting for " + what, last);
    }
}
