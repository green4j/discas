/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.metrics.PrometheusTextFormat;
import io.github.green4j.discas.common.operator.OperatorAttention;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What an operator is told about collection, and -- more to the point -- what they are not.
 *
 * <p>Collection is the first subsystem in this store whose normal state is <em>not finishing</em>,
 * so the pressure is towards reporting every unfinished sweep, which would page a healthy cluster
 * nightly and teach an operator to ignore the one signal that matters. The rule these cases hold
 * to: <b>a series earns its place only if seeing it change tells somebody to do something</b>.
 *
 * <p>So the assertions are as much about staleness and about clearing as about presence: a gauge
 * that stops moving when a cluster stops collecting is worse than no gauge, and a blocked-by family
 * that accumulates would name members that have long since come back.
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES)
@DisplayName("Tombstone collection -- the operator surface")
class TombstoneCollectionReportingTest {

    private static final HashedBytes KEY = TestBytes.hashed("k");
    private static final NodeId PEER_A = NodeId.of("n2");
    private static final NodeId PEER_B = NodeId.of("n3");

    private final MetricRegistry registry = new MetricRegistry();

    /**
     * The chain an operator actually runs: counters and gauges, then a log, then nothing. The log's
     * streams go nowhere -- what a line says is not asserted here or anywhere, because the wording
     * of an operator line is meant to be rewritten whenever it reads badly.
     */
    private final LoggingNodeObserver logging = new LoggingNodeObserver(
            new Log("n1", sink(), sink()),
            new OperatorAttention(new Log("n1", sink(), sink())),
            NodeObserver.NONE,
            0L);
    private final NodeObserver observer = new MetricsNodeObserver(registry, logging);

    private String scrape() {
        return PrometheusTextFormat.render(registry);
    }

    private static PrintStream sink() {
        return new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
    }

    private static TombstoneSweep nothingToCollect(final int tombstones) {
        return new TombstoneSweep(null, tombstones, false, List.of());
    }

    private static TombstoneSweep collected(final int tombstones) {
        return new TombstoneSweep(KEY, tombstones, true, List.of());
    }

    private static TombstoneSweep blockedBy(final int tombstones,
                                            final TombstoneSweep.Blocker... blockers) {
        return new TombstoneSweep(KEY, tombstones, false, List.of(blockers));
    }

    @Test
    @DisplayName("A sweep with nothing to do still reports: the readings are not events")
    void anIdleSweepStillMovesTheGauges() {
        observer.tombstoneSwept(nothingToCollect(1240));

        final String text = scrape();
        assertTrue(text.contains("discas_node_tombstones 1240"),
                "Key space that cannot shrink is a reading, not an event: " + text);
        assertTrue(text.contains("discas_node_tombstone_collection_blocked_seconds 0"),
                "Nothing is waiting, so nothing has been waiting: " + text);
        assertFalse(text.contains("discas_node_tombstone_collection_blocked_by"),
                "And nobody is holding anything up: " + text);
    }

    @Test
    @DisplayName("Who blocked the last sweep, and with what -- one sample each, replaced not accumulated")
    void blockedByNamesTheMembersOfTheLastSweep() {
        observer.tombstoneSwept(blockedBy(9,
                new TombstoneSweep.Blocker(PEER_A, null),
                new TombstoneSweep.Blocker(PEER_B, PurgeAnswer.RETAINED)));

        final String blocked = scrape();
        assertTrue(blocked.contains(
                        "discas_node_tombstone_collection_blocked_by{peer=\"n2\",answer=\"silent\"} 1"),
                "Silence is the member-down row of FAILURE_MODES, and reads as itself: " + blocked);
        assertTrue(blocked.contains(
                        "discas_node_tombstone_collection_blocked_by{peer=\"n3\",answer=\"retained\"} 1"),
                "A member that is merely behind is a different problem, and says so: " + blocked);

        // The next sweep gets past n3 and is held up by n2 alone. A family that accumulated would go
        // on naming a member that has since caught up, which is worse than naming nobody.
        observer.tombstoneSwept(blockedBy(9, new TombstoneSweep.Blocker(PEER_A, null)));

        final String again = scrape();
        assertTrue(again.contains("peer=\"n2\""), again);
        assertFalse(again.contains("peer=\"n3\""),
                "The last sweep is the one an operator acts on: " + again);

        // And a sweep that collects clears the family entirely.
        observer.tombstoneSwept(collected(8));
        assertFalse(scrape().contains("discas_node_tombstone_collection_blocked_by"),
                "Nothing is blocking a cluster that just collected");
    }

    @Test
    @DisplayName("Blocked seconds measure the run, not the sweep, and reset when one gets through")
    void blockedSecondsMeasureTheRun() throws Exception {
        observer.tombstoneSwept(blockedBy(9, new TombstoneSweep.Blocker(PEER_A, null)));
        // The one wait in here, and it buys the only assertion that distinguishes this series from a
        // boolean: an operator alerts on a day of this, so it has to accumulate across sweeps rather
        // than restart with each one.
        Thread.sleep(1100L);
        observer.tombstoneSwept(blockedBy(9, new TombstoneSweep.Blocker(PEER_A, null)));

        assertTrue(secondsBlocked() >= 1,
                "A second blocked sweep continues the run rather than restarting it: " + scrape());

        observer.tombstoneSwept(collected(8));
        assertTrue(scrape().contains("discas_node_tombstone_collection_blocked_seconds 0"),
                "And a collection ends the run");
        assertTrue(scrape().contains("discas_node_tombstones_collected_total 1"),
                "Which is the counter that says a repair worked");
    }

    private long secondsBlocked() {
        for (final String line : scrape().split("\n")) {
            if (line.startsWith("discas_node_tombstone_collection_blocked_seconds ")) {
                return Long.parseLong(line.substring(line.lastIndexOf(' ') + 1).trim());
            }
        }
        throw new AssertionError("The series is missing: " + scrape());
    }

}
