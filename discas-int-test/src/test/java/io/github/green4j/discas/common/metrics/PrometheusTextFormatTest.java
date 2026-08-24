/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.metrics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The exposition body is a contract with a scraper, so it is pinned exactly.
 * <p>
 * These assert the rendered bytes rather than that any observer fired: the observer seams are
 * free-form handlers whose invocation is not worth testing, but a malformed {@code # HELP} line or
 * an unescaped label value makes a scraper reject the <em>whole</em> body -- taking every other
 * metric in the process down with it.
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES)
@DisplayName("PrometheusTextFormat -- exposition rendering")
class PrometheusTextFormatTest {

    @Test
    @DisplayName("A counter renders HELP, TYPE and its value")
    void countersRender() {
        final MetricRegistry registry = new MetricRegistry();
        final Counter counter = registry.counter("discas_test_events_total", "Events seen.");
        counter.increment();
        counter.increment();

        assertEquals(
                "# HELP discas_test_events_total Events seen.\n"
                        + "# TYPE discas_test_events_total counter\n"
                        + "discas_test_events_total 2\n",
                PrometheusTextFormat.render(registry));
    }

    @Test
    @DisplayName("A gauge is read at render time, not at registration")
    void gaugesReadLive() {
        final MetricRegistry registry = new MetricRegistry();
        final long[] live = {7L};
        registry.gauge("discas_test_state", "Some state.", () -> live[0]);

        assertTrue(PrometheusTextFormat.render(registry).contains("discas_test_state 7\n"));
        live[0] = 9L;
        assertTrue(PrometheusTextFormat.render(registry).contains("discas_test_state 9\n"),
                "The gauge must reflect the value at scrape time, not the one at registration");
    }

    @Test
    @DisplayName("Samples of one family are grouped under a single HELP and TYPE")
    void samplesOfAFamilyAreGrouped() {
        final MetricRegistry registry = new MetricRegistry();
        // Registered apart, exactly as the per-peer series arrive from a dynamic source.
        registry.gauge("discas_test_peer_up", "Peer up.", () -> 1L, "peer", "n2");
        registry.gauge("discas_test_other", "Unrelated.", () -> 0L);
        registry.gauge("discas_test_peer_up", "Peer up.", () -> 0L, "peer", "n3");

        final String text = PrometheusTextFormat.render(registry);

        assertEquals(1, countOccurrences(text, "# HELP discas_test_peer_up "),
                "A family must declare HELP exactly once or the scraper rejects it");
        assertEquals(1, countOccurrences(text, "# TYPE discas_test_peer_up "));
        assertTrue(text.contains("discas_test_peer_up{peer=\"n2\"} 1\n"));
        assertTrue(text.contains("discas_test_peer_up{peer=\"n3\"} 0\n"));
    }

    @Test
    @DisplayName("Multiple labels render comma-separated in registration order")
    void multipleLabels() {
        final MetricRegistry registry = new MetricRegistry();
        registry.counter("discas_test_transitions_total", "Transitions.", "peer", "n2", "direction", "up")
                .increment();

        assertTrue(PrometheusTextFormat.render(registry)
                .contains("discas_test_transitions_total{peer=\"n2\",direction=\"up\"} 1\n"));
    }

    @Test
    @DisplayName("Label values escape quote, backslash and newline")
    void labelValuesAreEscaped() {
        final MetricRegistry registry = new MetricRegistry();
        registry.counter("discas_test_escaped_total", "Escaping.", "reason", "a\"b\\c\nd").increment();

        assertTrue(PrometheusTextFormat.render(registry)
                        .contains("discas_test_escaped_total{reason=\"a\\\"b\\\\c\\nd\"} 1\n"),
                "An unescaped quote or newline in a label value invalidates the entire body");
    }

    @Test
    @DisplayName("HELP escapes backslash and newline, and keeps the body single-line")
    void helpIsEscaped() {
        final MetricRegistry registry = new MetricRegistry();
        registry.counter("discas_test_help_total", "line one\nline two \\ done").increment();

        final String text = PrometheusTextFormat.render(registry);
        assertTrue(text.contains("# HELP discas_test_help_total line one\\nline two \\\\ done\n"));
        // A raw newline in HELP would split the comment into a line the scraper cannot parse.
        assertEquals(3, text.split("\n").length, "The family must render as exactly three lines");
    }

    @Test
    @DisplayName("An empty registry renders an empty body rather than failing")
    void emptyRegistry() {
        assertEquals("", PrometheusTextFormat.render(new MetricRegistry()));
    }

    @Test
    @DisplayName("An odd number of label parts is rejected at registration")
    void oddLabelPairsRejected() {
        final MetricRegistry registry = new MetricRegistry();
        try {
            registry.counter("discas_test_bad_total", "Bad.", "peer");
            throw new AssertionError("Expected IllegalArgumentException for an unpaired label");
        } catch (final IllegalArgumentException expected) {
            // The type is the signal; what it says about it is not.
        }
    }

    private static int countOccurrences(final String haystack, final String needle) {
        int count = 0;
        int from = 0;
        while (true) {
            final int at = haystack.indexOf(needle, from);
            if (at < 0) {
                return count;
            }
            count++;
            from = at + needle.length();
        }
    }
}
