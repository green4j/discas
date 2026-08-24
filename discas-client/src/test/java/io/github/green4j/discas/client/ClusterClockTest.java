/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the client's estimate of the cluster's clock does with the readings it is given.
 *
 * <p>Every case here is one a real deployment produces and no other test can: a client an hour off,
 * an operator setting the clock mid-run, an NTP slew that must <em>not</em> look like a step. The
 * clocks are driven by hand because the alternative is asking a test machine to change its time.
 */
@DisplayName("ClusterClock -- correcting this client's clock against the cluster's")
class ClusterClockTest {

    private static final NodeId NODE = NodeId.of("1");

    /** A wall clock and a monotonic clock this test moves independently, as reality can. */
    private static final class FakeClocks implements TimeSource {
        private final AtomicLong wallMillis = new AtomicLong(1_700_000_000_000L);
        private final AtomicLong monotonicNanos = new AtomicLong(5_000_000_000L);

        @Override
        public long wallMillis() {
            return wallMillis.get();
        }

        @Override
        public long monotonicNanos() {
            return monotonicNanos.get();
        }

        /** Time passing: both clocks advance together, which is the normal case. */
        void advance(final long millis) {
            wallMillis.addAndGet(millis);
            monotonicNanos.addAndGet(millis * 1_000_000L);
        }

        /** The wall clock alone jumping: an operator, a leap second, a restored VM snapshot. */
        void stepWallClock(final long millis) {
            wallMillis.addAndGet(millis);
        }
    }

    @Test
    @DisplayName("Before any handshake it answers with this client's own clock")
    void unadjustedUntilMeasured() {
        final FakeClocks clocks = new FakeClocks();
        final ClusterClock clock = new ClusterClock(clocks, ClientObserver.NONE);

        assertFalse(clock.offsetKnown(), "Nothing has been measured yet");
        assertEquals(clocks.wallMillis(), clock.nowMillis(),
                "A client that refused to tell the time until it had spoken to a coordinator could"
                        + " not take a lock at all before its first handshake");
    }

    @Test
    @DisplayName("A coordinator an hour ahead moves this client's answers an hour ahead")
    void offsetIsAppliedAfterMeasurement() {
        final FakeClocks clocks = new FakeClocks();
        final ClusterClock clock = new ClusterClock(clocks, ClientObserver.NONE);

        final long hourMs = 3_600_000L;
        final long sentAt = clocks.monotonicNanos();
        clocks.advance(40L);                                    // the round trip
        clock.observeCoordinatorTime(NODE, clocks.wallMillis() + hourMs, sentAt);

        // Half the round trip is added to the coordinator's reading, so the offset is the hour plus
        // 20ms of assumed one-way delay.
        assertEquals(hourMs + 20L, clock.offsetMillis());
        assertEquals(clocks.wallMillis() + hourMs + 20L, clock.nowMillis());
    }

    @Test
    @DisplayName("Ordinary drift is not a step: a slow slew keeps the offset")
    void slewBelowThresholdKeepsTheOffset() {
        final FakeClocks clocks = new FakeClocks();
        final ClusterClock clock = new ClusterClock(clocks, ClientObserver.NONE);
        clock.observeCoordinatorTime(NODE, clocks.wallMillis() + 1_000L, clocks.monotonicNanos());

        clocks.advance(10_000L);
        clocks.stepWallClock(ClusterClock.STEP_THRESHOLD_MS - 1);

        assertTrue(clock.offsetKnown(),
                "A correction that threw itself away on every NTP slew would be no correction");
        assertEquals(1_000L, clock.offsetMillis());
    }

    @ParameterizedTest(name = "step of {0}ms")
    @ValueSource(longs = {60_000L, -30_000L})
    @DisplayName("This client's clock stepping drops the offset it invalidated, and says so")
    void ownClockStepDropsTheOffset(final long stepMillis) {
        final FakeClocks clocks = new FakeClocks();
        final long[] reportedStep = {0L};
        final ClusterClock clock = new ClusterClock(clocks, new ClientObserver() {
            @Override
            public void clientClockStepped(final long stepMillis) {
                reportedStep[0] = stepMillis;
            }
        });
        clock.observeCoordinatorTime(NODE, clocks.wallMillis() + 1_000L, clocks.monotonicNanos());

        clocks.advance(5_000L);
        // Forwards or backwards: a step detector missing Math.abs only fails on the backward row.
        clocks.stepWallClock(stepMillis);

        // The offset described this client's clock as it was a minute ago; applying it now would
        // add the correction on top of a clock that has already moved.
        assertEquals(clocks.wallMillis(), clock.nowMillis(),
                "An invalidated offset must not be applied");
        assertFalse(clock.offsetKnown());
        assertEquals(stepMillis, reportedStep[0],
                "The step is worth reporting: until the next handshake, leases are written and"
                        + " judged on an uncorrected clock");
    }

    @Test
    @DisplayName("The next handshake re-measures, and the clock is corrected again")
    void reMeasurementRestoresCorrection() {
        final FakeClocks clocks = new FakeClocks();
        final ClusterClock clock = new ClusterClock(clocks, ClientObserver.NONE);
        clock.observeCoordinatorTime(NODE, clocks.wallMillis() + 1_000L, clocks.monotonicNanos());

        clocks.advance(1_000L);
        clocks.stepWallClock(120_000L);
        clock.nowMillis();                              // notices the step, drops the offset
        assertFalse(clock.offsetKnown());

        // The coordinator did not move; this client did. Measured against the stepped clock, the
        // offset is now the old one minus the step.
        clock.observeCoordinatorTime(NODE, clocks.wallMillis() - 119_000L, clocks.monotonicNanos());
        assertTrue(clock.offsetKnown());
        assertEquals(-119_000L, clock.offsetMillis());
    }
}
