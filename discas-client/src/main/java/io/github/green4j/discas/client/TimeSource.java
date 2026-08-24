/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/**
 * The two clocks a client reads, behind one seam.
 *
 * <p>They are not interchangeable, and {@link ClusterClock} needs both for different jobs:
 * <ul>
 *   <li>{@link #wallMillis()} has a common origin, so it is the only one two processes can compare
 *       -- a lease deadline written by one client and read by another has to be expressed on it.
 *       It also steps, which is the whole problem this package is dealing with.</li>
 *   <li>{@link #monotonicNanos()} never steps, so it is the only one that can measure how much time
 *       has passed. It has no origin to share, so it can never express a deadline for someone
 *       else.</li>
 * </ul>
 *
 * <p>A seam rather than direct calls to {@link System}: a test that has to demonstrate what happens
 * to a lease when two clients disagree about the time cannot do it by moving the machine's clock,
 * and an application embedding this client may already have a time source of its own.
 */
public interface TimeSource {

    /** Epoch milliseconds, as {@link System#currentTimeMillis()} reports them. */
    long wallMillis();

    /** A monotonic reading in nanoseconds, as {@link System#nanoTime()} reports it. */
    long monotonicNanos();

    /** The real clocks. */
    TimeSource SYSTEM = new TimeSource() {
        @Override
        public long wallMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public long monotonicNanos() {
            return System.nanoTime();
        }

        @Override
        public String toString() {
            return "TimeSource.SYSTEM";
        }
    };
}
