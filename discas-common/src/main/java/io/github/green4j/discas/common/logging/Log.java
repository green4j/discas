/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.logging;

import java.io.PrintStream;

/**
 * The whole logger: two levels, two streams, one line per record, no hierarchy, no configuration
 * file and no third-party dependency. Records look like
 * {@code 2026-08-11 10:33:01.123 INFO  [n1] recovery complete, keys=42}.
 * <p>
 * {@link LogLevel#INFO} goes to {@code stdout} and {@link LogLevel#ERROR} to {@code stderr}, the
 * split container runtimes and service supervisors capture separately, so an operator can alert on
 * the error stream without parsing every record. A record is assembled fully before a single
 * {@code println}, so concurrent writers interleave between lines rather than inside one.
 * <p>
 * Thread-safe and lock-free: the per-record scratch is held per thread, not per instance.
 */
public final class Log {

    /**
     * Held per thread rather than per {@code Log}, so several logs in one process share one buffer
     * per thread instead of one each.
     */
    private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

    private final String source;
    private final PrintStream infoStream;
    private final PrintStream errorStream;

    /**
     * @param source short identity shown in brackets on every record -- a node id, client id, or
     *               component name. Rendered once per record, so keep it short.
     */
    public Log(final String source) {
        this(source, System.out, System.err);
    }

    /** Streams are injectable so a test can capture records without touching the JVM's globals. */
    public Log(final String source, final PrintStream infoStream, final PrintStream errorStream) {
        this.source = source == null ? "" : source;
        this.infoStream = infoStream;
        this.errorStream = errorStream;
    }

    /** Writes one INFO record to {@code stdout}. */
    public void info(final String message) {
        write(LogLevel.INFO, infoStream, message, null);
    }

    /** Writes one ERROR record to {@code stderr}. */
    public void error(final String message) {
        write(LogLevel.ERROR, errorStream, message, null);
    }

    /**
     * Logs {@code message} followed by {@code error}'s type and message, then its stack trace on
     * the following lines -- outside the record, so the one-line-per-record shape survives.
     */
    public void error(final String message, final Throwable error) {
        write(LogLevel.ERROR, errorStream, message, error);
    }

    private void write(final LogLevel level,
                       final PrintStream stream,
                       final String message,
                       final Throwable error) {
        final Scratch scratch = SCRATCH.get();
        final StringBuilder line = scratch.line;
        line.setLength(0);
        scratch.timestamps.append(line, System.currentTimeMillis());
        line.append(' ').append(level.padded()).append(" [").append(source).append("] ").append(message);
        if (error != null) {
            line.append(": ").append(error);
        }
        stream.println(line);
        if (error != null) {
            error.printStackTrace(stream);
        }
    }

    private static final class Scratch {
        // Sized for a typical record so the common case never grows the array; it is reused for the
        // lifetime of the thread, so the one-off allocation is amortised to nothing.
        private final StringBuilder line = new StringBuilder(160);
        private final TimestampCache timestamps = new TimestampCache();
    }
}
