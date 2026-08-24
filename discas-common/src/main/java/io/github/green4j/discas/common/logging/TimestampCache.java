/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.logging;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Renders {@code yyyy-MM-dd HH:mm:ss.SSS} into a reusable buffer, re-deriving the calendar fields
 * at most once per second: records sharing a second share their first nineteen characters, so all
 * that changes is three millisecond digits -- three array stores and no allocation, where a
 * {@code DateTimeFormatter} would build a fresh {@code String} per record.
 * <p>
 * <b>Not thread-safe.</b> The buffer is mutable state, so an instance belongs to one thread;
 * {@link Log} holds one per thread. Two threads in different seconds would also invalidate each
 * other's render on every record.
 * <p>
 * A clock stepped backwards re-renders just as a forward step does: the cache is keyed on the
 * second <em>differing</em> from the one rendered, not on being later, so a rendered timestamp
 * always reflects the {@code epochMillis} it was given.
 */
public final class TimestampCache {

    /** {@code yyyy-MM-dd HH:mm:ss.SSS} -- fixed width, so every index below is a constant. */
    public static final int LENGTH = 23;

    private static final int OFF_YEAR = 0;
    private static final int OFF_MONTH = 5;
    private static final int OFF_DAY = 8;
    private static final int OFF_HOUR = 11;
    private static final int OFF_MINUTE = 14;
    private static final int OFF_SECOND = 17;
    private static final int OFF_MILLIS = 20;

    private final char[] buffer = new char[LENGTH];
    private final ZoneId zone;

    /** The epoch-second {@link #buffer} currently holds; {@code MIN_VALUE} until the first render. */
    private long renderedSecond = Long.MIN_VALUE;

    /** Uses the JVM's default zone -- what an operator reading the process's output expects. */
    public TimestampCache() {
        this(ZoneId.systemDefault());
    }

    public TimestampCache(final ZoneId zone) {
        this.zone = zone;
        buffer[4] = '-';
        buffer[7] = '-';
        buffer[10] = ' ';
        buffer[13] = ':';
        buffer[16] = ':';
        buffer[19] = '.';
    }

    /**
     * Appends the formatted timestamp for {@code epochMillis} to {@code out}.
     * <p>
     * {@code floorDiv}/{@code floorMod} rather than {@code /} and {@code %}, so a pre-1970 instant
     * still yields a millisecond field in {@code [0, 999]} rather than a negative one.
     */
    public void append(final StringBuilder out, final long epochMillis) {
        final long second = Math.floorDiv(epochMillis, 1000L);
        final int millis = (int) Math.floorMod(epochMillis, 1000L);
        // Inequality, NOT `second > renderedSecond`. The wall clock can move in either direction --
        // an NTP step, a VM resuming from a snapshot, or an operator setting the date -- and a
        // greater-than test would keep serving the cached (now future-dated) prefix until real time
        // caught up, silently stamping every record in between with the wrong second. Comparing for
        // difference re-renders on any change, forward or backward, for the same one comparison.
        if (second != renderedSecond) {
            renderSecond(second);
            renderedSecond = second;
        }
        buffer[OFF_MILLIS] = (char) ('0' + millis / 100);
        buffer[OFF_MILLIS + 1] = (char) ('0' + (millis / 10) % 10);
        buffer[OFF_MILLIS + 2] = (char) ('0' + millis % 10);
        out.append(buffer, 0, LENGTH);
    }

    /**
     * The zone offset is resolved per rendered second rather than cached at construction, where a
     * fixed offset would render every timestamp an hour wrong for half the year in a DST zone.
     */
    private void renderSecond(final long epochSecond) {
        final ZoneOffset offset = zone.getRules().getOffset(Instant.ofEpochSecond(epochSecond));
        final LocalDateTime time = LocalDateTime.ofEpochSecond(epochSecond, 0, offset);
        writeFourDigits(OFF_YEAR, time.getYear());
        writeTwoDigits(OFF_MONTH, time.getMonthValue());
        writeTwoDigits(OFF_DAY, time.getDayOfMonth());
        writeTwoDigits(OFF_HOUR, time.getHour());
        writeTwoDigits(OFF_MINUTE, time.getMinute());
        writeTwoDigits(OFF_SECOND, time.getSecond());
    }

    private void writeTwoDigits(final int offset, final int value) {
        buffer[offset] = (char) ('0' + value / 10);
        buffer[offset + 1] = (char) ('0' + value % 10);
    }

    private void writeFourDigits(final int offset, final int value) {
        // A year outside [0, 9999] cannot fit the fixed-width field. Clamping keeps the record
        // well-formed rather than corrupting the columns that follow it; no real clock reaches here.
        final int year = value < 0 ? 0 : Math.min(value, 9999);
        buffer[offset] = (char) ('0' + year / 1000);
        buffer[offset + 1] = (char) ('0' + (year / 100) % 10);
        buffer[offset + 2] = (char) ('0' + (year / 10) % 10);
        buffer[offset + 3] = (char) ('0' + year % 10);
    }
}
