/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Renders a {@link MetricRegistry} as Prometheus/OpenMetrics text exposition.
 * <p>
 * This is the pull-based OpenTelemetry path: the OpenTelemetry Collector ingests this format
 * natively through its {@code prometheus} receiver, so a discas node is scrapeable by an OTel
 * pipeline, by Prometheus itself, or by Grafana Alloy without discas taking on a single runtime
 * dependency -- which matters, because it has none. OTLP proper is a <em>push</em> protocol and has
 * no meaning on a GET endpoint.
 * <p>
 * The class is a pure function of the registry's contents. That is deliberate and is what makes it
 * testable: the observer seams are free-form handlers whose firing is not worth asserting, but the
 * bytes this produces are a contract with a scraper and are worth pinning exactly.
 * <p>
 * Format, per the exposition spec: each family emits {@code # HELP} and {@code # TYPE} once,
 * followed by its samples. Samples of one family are grouped even when registered apart, because a
 * scraper rejects a family whose {@code # HELP} appears twice.
 */
public final class PrometheusTextFormat {

    private PrometheusTextFormat() {
    }

    /** Collects every metric in {@code registry} and renders the exposition body. */
    public static String render(final MetricRegistry registry) {
        final Collector collector = new Collector();
        registry.collectInto(collector);
        return collector.toText();
    }

    /**
     * Groups samples by family name as they arrive, then renders. Grouping is why collection and
     * rendering are two phases: a registry may emit {@code discas_node_peer_handshaked{peer="n2"}}
     * and {@code ...{peer="n3"}} from different sources, and they must land under one {@code # HELP}.
     */
    private static final class Collector implements MetricRegistry.MetricSink {

        private final Map<String, Family> families = new LinkedHashMap<>();

        @Override
        public void counter(final String name, final String help, final long value,
                            final String... labelPairs) {
            family(name, help, "counter").samples.add(new Sample(labelPairs, value));
        }

        @Override
        public void gauge(final String name, final String help, final long value,
                          final String... labelPairs) {
            family(name, help, "gauge").samples.add(new Sample(labelPairs, value));
        }

        private Family family(final String name, final String help, final String type) {
            final Family existing = families.get(name);
            if (existing != null) {
                return existing;
            }
            final Family created = new Family(name, help, type);
            families.put(name, created);
            return created;
        }

        private String toText() {
            final StringBuilder out = new StringBuilder(1024);
            for (final Family family : families.values()) {
                out.append("# HELP ").append(family.name).append(' ');
                appendEscapedHelp(out, family.help);
                out.append('\n');
                out.append("# TYPE ").append(family.name).append(' ').append(family.type).append('\n');
                for (int i = 0; i < family.samples.size(); i++) {
                    final Sample sample = family.samples.get(i);
                    out.append(family.name);
                    appendLabels(out, sample.labelPairs);
                    out.append(' ').append(sample.value).append('\n');
                }
            }
            return out.toString();
        }
    }

    private static void appendLabels(final StringBuilder out, final String[] labelPairs) {
        if (labelPairs.length == 0) {
            return;
        }
        out.append('{');
        for (int i = 0; i < labelPairs.length; i += 2) {
            if (i > 0) {
                out.append(',');
            }
            out.append(labelPairs[i]).append("=\"");
            appendEscapedLabelValue(out, labelPairs[i + 1]);
            out.append('"');
        }
        out.append('}');
    }

    /**
     * Escapes a label value per the exposition spec: backslash, double quote and newline. A node id
     * or a close reason reaches here unfiltered, so an unescaped quote would produce a body the
     * scraper rejects outright -- taking every other metric down with it.
     */
    private static void appendEscapedLabelValue(final StringBuilder out, final String value) {
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            switch (c) {
                case '\\':
                    out.append("\\\\");
                    break;
                case '"':
                    out.append("\\\"");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                default:
                    out.append(c);
                    break;
            }
        }
    }

    /** {@code # HELP} escapes only backslash and newline -- a quote is legal there unescaped. */
    private static void appendEscapedHelp(final StringBuilder out, final String help) {
        for (int i = 0; i < help.length(); i++) {
            final char c = help.charAt(i);
            switch (c) {
                case '\\':
                    out.append("\\\\");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                default:
                    out.append(c);
                    break;
            }
        }
    }

    private static final class Family {
        private final String name;
        private final String help;
        private final String type;
        private final List<Sample> samples = new ArrayList<>();

        private Family(final String name, final String help, final String type) {
            this.name = name;
            this.help = help;
            this.type = type;
        }
    }

    private static final class Sample {
        private final String[] labelPairs;
        private final long value;

        private Sample(final String[] labelPairs, final long value) {
            this.labelPairs = labelPairs;
            this.value = value;
        }
    }
}
