/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.logging;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Keeps one log site from drowning the record, without ever going silent about it. A diagnostic
 * that is fine at human rates becomes the incident when the thing it reports starts happening at
 * machine rates -- a flapping peer, a file rewritten in a loop -- and the lines, especially the
 * ones carrying a stack trace, then cost more than the fault.
 * <ul>
 *   <li><b>Up to {@link #DEFAULT_BURST} messages a second pass unchanged</b>, so ordinary operation
 *       is never rate-limited.</li>
 *   <li><b>Past that, at most one a second</b>, and each line that comes out says how many were
 *       dropped since the last: a throttle that hid its own effect would turn a storm into silence.
 *       </li>
 *   <li><b>Quiet for {@link #DEFAULT_RESET_NANOS} and the burst allowance comes back in full.</b>
 *       Measured from the last message <em>offered</em>, not the last emitted, so a site being
 *       hammered with everything suppressed stays throttled.</li>
 * </ul>
 * <p>
 * One instance per call site, held as a field; it writes the line itself, so no caller has to branch
 * on the throttle. What a caller still pays during a storm is building the message; what it stops
 * paying is the write and the stack trace.
 * <p>
 * <b>Not synchronised.</b> A throttle shared across threads counts wrong (it corrupts nothing).
 * Give each single-threaded site its own.
 */
public final class LogThrottle {

    /**
     * Property prefix for the three numbers below. System properties rather than CLI flags: they
     * are not part of a node's configuration surface and an operator should never need one, since
     * the defaults leave ordinary operation unthrottled.
     */
    public static final String PROPERTY_PREFIX = "discas.log.throttle.";

    /** Messages a second that pass untouched before the site is treated as running hot. */
    public static final int DEFAULT_BURST = intProperty("burst", 100);

    /** How long a throttled site waits between lines. */
    public static final long DEFAULT_THROTTLED_INTERVAL_NANOS =
            TimeUnit.MILLISECONDS.toNanos(longProperty("interval.ms", 1_000L));

    /** Quiet for this long and the burst allowance is restored in full. */
    public static final long DEFAULT_RESET_NANOS =
            TimeUnit.MILLISECONDS.toNanos(longProperty("reset.ms", 10_000L));

    private static final long BURST_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Never throws: a malformed number would otherwise stop the process during class initialisation
     * of its own logger, with no working log to say why. Out of range is treated as unparseable.
     */
    private static long longProperty(final String name, final long fallback) {
        try {
            final String value = System.getProperty(PROPERTY_PREFIX + name);
            if (value == null) {
                return fallback;
            }
            final long parsed = Long.parseLong(value.trim());
            return parsed > 0 ? parsed : fallback;
        } catch (final RuntimeException ignored) {
            return fallback;
        }
    }

    private static int intProperty(final String name, final int fallback) {
        final long value = longProperty(name, fallback);
        return value > Integer.MAX_VALUE ? fallback : (int) value;
    }

    /** Shared, because the un-throttled case is the common one and it allocates nothing. */
    private static final String NO_SUFFIX = "";

    private final int burst;
    private final long throttledIntervalNanos;
    private final long resetNanos;
    private final LongSupplier nanoClock;

    /** Start of the current burst window, and how many messages it has seen. */
    private long windowStartNanos;
    private int inWindow;

    private boolean throttled;
    private long lastAdmittedNanos;
    private long lastOfferedNanos;
    private long suppressed;

    /** Whether anything has been offered yet -- the first message must not look like a 10s gap. */
    private boolean started;

    public LogThrottle() {
        this(DEFAULT_BURST, DEFAULT_THROTTLED_INTERVAL_NANOS, DEFAULT_RESET_NANOS,
                System::nanoTime);
    }

    /** The clock is injectable so a caller can drive the windows without waiting them out. */
    public LogThrottle(final int burst, final long throttledIntervalNanos, final long resetNanos,
                       final LongSupplier nanoClock) {
        this.burst = burst;
        this.throttledIntervalNanos = throttledIntervalNanos;
        this.resetNanos = resetNanos;
        this.nanoClock = nanoClock;
    }

    /** Writes {@code message} at INFO unless this site is running hot. */
    public void info(final Log log, final String message) {
        final String suffix = admit();
        if (suffix != null) {
            log.info(message + suffix);
        }
    }

    /** Writes {@code message} at ERROR unless this site is running hot. */
    public void error(final Log log, final String message) {
        final String suffix = admit();
        if (suffix != null) {
            log.error(message + suffix);
        }
    }

    /** As {@link #error(Log, String)}, with the throwable whose stack trace follows it. */
    public void error(final Log log, final String message, final Throwable error) {
        final String suffix = admit();
        if (suffix != null) {
            log.error(message + suffix, error);
        }
    }

    /**
     * Offers one message to the throttle.
     *
     * @return {@code null} if this message should not be written, otherwise what to append to it:
     *         the empty string in the ordinary case, or {@code " (+N suppressed)"} when this line is
     *         standing in for messages that were dropped. One call both decides and reports, so the
     *         count cannot be read without being consumed.
     */
    private String admit() {
        final long now = nanoClock.getAsLong();

        // Quiet long enough that whatever was hot has stopped. Everything goes back, including the
        // burst allowance -- the alternative is a site that misbehaved once staying clipped forever.
        if (started && now - lastOfferedNanos >= resetNanos) {
            throttled = false;
            suppressed = 0;
            inWindow = 0;
            windowStartNanos = now;
        }
        if (!started) {
            started = true;
            windowStartNanos = now;
        }
        lastOfferedNanos = now;

        if (!throttled) {
            if (now - windowStartNanos >= BURST_WINDOW_NANOS) {
                windowStartNanos = now;
                inWindow = 0;
            }
            if (++inWindow <= burst) {
                lastAdmittedNanos = now;
                return NO_SUFFIX;
            }
            // The burst is spent: from here the site is running hot and pays the interval. This
            // message is the first casualty rather than the announcement -- the next line out says
            // so, and says how many, which is the same statement made once instead of twice.
            throttled = true;
            suppressed = 1;
            return null;
        }

        if (now - lastAdmittedNanos < throttledIntervalNanos) {
            suppressed++;
            return null;
        }
        lastAdmittedNanos = now;
        if (suppressed == 0) {
            return NO_SUFFIX;
        }
        final String suffix = " (+" + suppressed + " suppressed)";
        suppressed = 0;
        return suffix;
    }
}
