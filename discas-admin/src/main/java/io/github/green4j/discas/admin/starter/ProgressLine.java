/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One line of progress on a terminal, rewritten in place; one line every so often when the output
 * is not a terminal.
 *
 * <p>Both tools work for minutes or hours over a key space nobody can size in advance, and a
 * process that prints nothing until it finishes is one an operator kills. What each tool can
 * honestly say differs -- a dump knows only how much it has done, a restore knows the entry count
 * from the dump's trailer and can say how much is left -- so the <em>text</em> is the caller's and
 * only the drawing is here.
 *
 * <p>Throttled, because the callers report per key: at tens of thousands of keys a second, drawing
 * every one would cost more than the work being reported. {@link #finish} always draws.
 */
final class ProgressLine {

    private static final long MIN_REDRAW_NANOS = TimeUnit.MILLISECONDS.toNanos(250);

    private final PrintStream out;
    private final boolean terminal;
    private final AtomicLong lastDrawNanos = new AtomicLong(Long.MIN_VALUE);
    private volatile int lastLength;

    ProgressLine(final PrintStream out) {
        this.out = out;
        // No terminal means a log file or a pipe, where carriage returns pile up as one unreadable
        // line: print whole lines instead, and rarely.
        this.terminal = System.console() != null;
    }

    /** Draws {@code text} if enough time has passed since the last draw. Cheap to call per key. */
    void update(final String text) {
        final long now = System.nanoTime();
        final long last = lastDrawNanos.get();
        if (now - last < redrawInterval()) {
            return;
        }
        if (!lastDrawNanos.compareAndSet(last, now)) {
            return; // another thread is drawing this instant; one line, one writer
        }
        draw(text);
    }

    /** Draws {@code text} and ends the line. */
    void finish(final String text) {
        lastDrawNanos.set(System.nanoTime());
        draw(text);
        out.println();
        out.flush();
    }

    /** Ends the line without drawing again, so a failure's message starts on a clean one. */
    void abandon() {
        if (terminal && lastLength > 0) {
            out.println();
            out.flush();
        }
    }

    private long redrawInterval() {
        return terminal ? MIN_REDRAW_NANOS : TimeUnit.SECONDS.toNanos(10);
    }

    private void draw(final String text) {
        if (!terminal) {
            out.println(text);
            out.flush();
            return;
        }
        final StringBuilder line = new StringBuilder(text.length() + lastLength + 2);
        line.append('\r').append(text);
        // A shorter line than the last one would leave the tail of the old one on screen.
        for (int i = text.length(); i < lastLength; i++) {
            line.append(' ');
        }
        lastLength = text.length();
        out.print(line);
        out.flush();
    }

    /** Bytes as an operator reads them: three significant digits and a unit. */
    static String humanBytes(final long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        final String[] units = {"KiB", "MiB", "GiB", "TiB", "PiB"};
        double value = bytes / 1024.0;
        int unit = 0;
        while (value >= 1024.0 && unit < units.length - 1) {
            value /= 1024.0;
            unit++;
        }
        return String.format(value < 10.0 ? "%.2f %s" : "%.1f %s", value, units[unit]);
    }
}
