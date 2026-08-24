/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.metrics;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;

/**
 * The set of metrics a process exposes: counters that decorators increment, gauges read at scrape
 * time, and dynamic sources that emit a whole family per scrape.
 * <p>
 * The three kinds exist because discas has three shapes of measurement:
 * <ul>
 *   <li><b>Counters</b> -- events, incremented from an observer callback. Registered once, held as a
 *       field, so the callback never does a lookup.</li>
 *   <li><b>Gauges</b> -- state that already lives somewhere else (cluster size, ready flag). Reading
 *       it at scrape time via a {@link LongSupplier} means nothing has to be pushed into the
 *       registry when it changes, so state changes cost nothing.</li>
 *   <li><b>Sources</b> -- families whose <em>sample set</em> varies, notably the per-peer series.
 *       Registering one gauge per peer would mean writing to the registry from the event loop when a
 *       peer appears; instead one source is registered up front and enumerates the current peers
 *       when the scrape asks. Registration stays a startup-only activity.</li>
 * </ul>
 * <p>
 * Registration is expected at startup, but the collections are copy-on-write so a late registration
 * cannot corrupt a concurrent scrape.
 */
public final class MetricRegistry {

    private final List<Counter> counters = new CopyOnWriteArrayList<>();
    private final List<Gauge> gauges = new CopyOnWriteArrayList<>();
    private final List<MetricSource> sources = new CopyOnWriteArrayList<>();

    /**
     * Registers a counter and returns it for the caller to hold.
     *
     * @param name       the metric name; by Prometheus convention a counter ends in {@code _total}
     * @param help       one-line description emitted as {@code # HELP}
     * @param labelPairs alternating label name/value, e.g. {@code "peer", "n2"}; must be even-length
     */
    public Counter counter(final String name, final String help, final String... labelPairs) {
        requireEvenPairs(labelPairs);
        final Counter counter = new Counter(name, help, labelPairs);
        counters.add(counter);
        return counter;
    }

    /** Registers a gauge whose value is read from {@code value} at scrape time. */
    public void gauge(final String name,
                      final String help,
                      final LongSupplier value,
                      final String... labelPairs) {
        requireEvenPairs(labelPairs);
        gauges.add(new Gauge(name, help, value, labelPairs));
    }

    /** Registers a source that emits a variable set of samples on each scrape. */
    public void register(final MetricSource source) {
        sources.add(source);
    }

    /**
     * Emits every metric into {@code sink}. Called once per scrape; the ordering is registration
     * order, which keeps the rendered output stable between scrapes and therefore diffable.
     */
    public void collectInto(final MetricSink sink) {
        for (int i = 0; i < counters.size(); i++) {
            final Counter counter = counters.get(i);
            sink.counter(counter.name(), counter.help(), counter.sum(), counter.labelPairs());
        }
        for (int i = 0; i < gauges.size(); i++) {
            final Gauge gauge = gauges.get(i);
            sink.gauge(gauge.name, gauge.help, gauge.value.getAsLong(), gauge.labelPairs);
        }
        for (int i = 0; i < sources.size(); i++) {
            sources.get(i).collect(sink);
        }
    }

    private static void requireEvenPairs(final String[] labelPairs) {
        if (labelPairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "labelPairs must be alternating name/value, got " + labelPairs.length + " entries");
        }
    }

    /** Receives samples during a scrape. Implemented by the exposition-format encoder. */
    public interface MetricSink {

        void counter(String name, String help, long value, String... labelPairs);

        void gauge(String name, String help, long value, String... labelPairs);
    }

    /** A family whose samples are enumerated at scrape time rather than registered individually. */
    @FunctionalInterface
    public interface MetricSource {

        void collect(MetricSink sink);
    }

    private static final class Gauge {
        private final String name;
        private final String help;
        private final LongSupplier value;
        private final String[] labelPairs;

        private Gauge(final String name,
                      final String help,
                      final LongSupplier value,
                      final String[] labelPairs) {
            this.name = name;
            this.help = help;
            this.value = value;
            this.labelPairs = labelPairs;
        }
    }
}
