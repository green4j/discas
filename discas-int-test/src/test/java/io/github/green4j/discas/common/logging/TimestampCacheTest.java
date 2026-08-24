/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.logging;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The cache exists to avoid re-deriving calendar fields per record, so what matters is that the
 * shortcut never produces a wrong timestamp -- especially when the clock does something other than
 * tick forward.
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES)
@DisplayName("TimestampCache -- fast formatting stays correct")
class TimestampCacheTest {

    private static final ZoneId UTC = ZoneOffset.UTC;
    private static final DateTimeFormatter REFERENCE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @Test
    @DisplayName("Output matches a DateTimeFormatter over a range of instants")
    void matchesReferenceFormatter() {
        final TimestampCache cache = new TimestampCache(UTC);
        final long base = Instant.parse("2026-08-11T10:33:01.123Z").toEpochMilli();
        for (long delta = 0; delta < 5_000; delta += 137) {
            assertEquals(reference(base + delta), format(cache, base + delta));
        }
    }

    @Test
    @DisplayName("Within one second only the millisecond digits change")
    void reusesTheRenderedSecond() {
        final TimestampCache cache = new TimestampCache(UTC);
        final long second = Instant.parse("2026-08-11T10:33:01.000Z").toEpochMilli();

        assertEquals("2026-08-11 10:33:01.000", format(cache, second));
        assertEquals("2026-08-11 10:33:01.001", format(cache, second + 1));
        assertEquals("2026-08-11 10:33:01.999", format(cache, second + 999));
    }

    @Test
    @DisplayName("Crossing a second boundary re-renders the prefix")
    void reRendersOnANewSecond() {
        final TimestampCache cache = new TimestampCache(UTC);
        final long second = Instant.parse("2026-08-11T10:33:01.500Z").toEpochMilli();

        assertEquals("2026-08-11 10:33:01.500", format(cache, second));
        assertEquals("2026-08-11 10:33:02.000", format(cache, second + 500));
    }

    @Test
    @DisplayName("A clock stepped BACKWARDS re-renders instead of serving the future prefix")
    void handlesBackwardDrift() {
        final TimestampCache cache = new TimestampCache(UTC);
        final long later = Instant.parse("2026-08-11T10:33:10.000Z").toEpochMilli();
        final long earlier = Instant.parse("2026-08-11T10:33:01.000Z").toEpochMilli();

        assertEquals("2026-08-11 10:33:10.000", format(cache, later));
        // The cached second is now ahead of the one being asked for. A `>` test would keep the
        // stale 10:33:10 prefix and stamp this record nine seconds in the future.
        assertEquals("2026-08-11 10:33:01.000", format(cache, earlier));
        // ...and the cache must now be keyed on the earlier second, not flip back.
        assertEquals("2026-08-11 10:33:01.250", format(cache, earlier + 250));
    }

    @Test
    @DisplayName("A large forward jump re-renders every field")
    void handlesForwardDrift() {
        final TimestampCache cache = new TimestampCache(UTC);
        final long before = Instant.parse("2026-08-11T10:33:01.000Z").toEpochMilli();
        final long after = Instant.parse("2027-01-02T03:04:05.006Z").toEpochMilli();

        assertEquals("2026-08-11 10:33:01.000", format(cache, before));
        assertEquals("2027-01-02 03:04:05.006", format(cache, after));
    }

    @Test
    @DisplayName("Drifting back and forth repeatedly stays correct")
    void handlesRepeatedDrift() {
        final TimestampCache cache = new TimestampCache(UTC);
        final long a = Instant.parse("2026-08-11T10:33:01.000Z").toEpochMilli();
        final long b = Instant.parse("2026-08-11T10:33:59.999Z").toEpochMilli();
        for (int i = 0; i < 4; i++) {
            assertEquals(reference(a), format(cache, a));
            assertEquals(reference(b), format(cache, b));
        }
    }

    @Test
    @DisplayName("A pre-epoch instant yields a millisecond field in [0, 999]")
    void handlesPreEpochInstants() {
        final TimestampCache cache = new TimestampCache(UTC);
        // Signed remainder would give a negative millisecond field and corrupt the fixed columns.
        final long beforeEpoch = Instant.parse("1969-12-31T23:59:59.250Z").toEpochMilli();
        assertEquals("1969-12-31 23:59:59.250", format(cache, beforeEpoch));
    }

    @Test
    @DisplayName("Output is always the fixed width the format promises")
    void fixedWidth() {
        final TimestampCache cache = new TimestampCache(UTC);
        final long base = Instant.parse("2026-01-02T03:04:05.006Z").toEpochMilli();
        assertEquals(TimestampCache.LENGTH, format(cache, base).length());
    }

    private static String format(final TimestampCache cache, final long epochMillis) {
        final StringBuilder out = new StringBuilder();
        cache.append(out, epochMillis);
        return out.toString();
    }

    private static String reference(final long epochMillis) {
        return REFERENCE.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), UTC));
    }
}
