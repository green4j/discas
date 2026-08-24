/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * A monotonically increasing count, handed to a decorator once at registration and incremented
 * directly from then on.
 * <p>
 * Holding the counter as a field is the point: an observer callback that has to look its metric up
 * by name on every event pays a hash and a map probe on the node's event loop, which is the one
 * thread the whole protocol is serialised on. Registration resolves the name once; the callback
 * does an {@link LongAdder#increment()} and nothing else.
 * <p>
 * {@link LongAdder} rather than {@code AtomicLong} because a single registry is written by several
 * threads -- the node's event loop, a client's loop, and the file-watch daemon -- and {@code
 * LongAdder} spreads that across cells instead of contending on one CAS. The trade is that reads
 * ({@link #sum()}) are slightly more expensive and only eventually consistent with in-flight
 * increments, which is exactly the right trade for something read once per scrape.
 */
public final class Counter {

    private final String name;
    private final String help;
    private final String[] labelPairs;
    private final LongAdder value = new LongAdder();

    Counter(final String name, final String help, final String[] labelPairs) {
        this.name = name;
        this.help = help;
        this.labelPairs = labelPairs;
    }

    /** Adds one. The whole hot path. */
    public void increment() {
        value.increment();
    }

    /** Adds {@code delta}; for counters that advance by more than one per event. */
    public void add(final long delta) {
        value.add(delta);
    }

    /** The current total. Read at scrape time, not on the hot path. */
    public long sum() {
        return value.sum();
    }

    public String name() {
        return name;
    }

    public String help() {
        return help;
    }

    /** Alternating label name/value pairs; empty for an unlabelled counter. */
    public String[] labelPairs() {
        return labelPairs;
    }
}
