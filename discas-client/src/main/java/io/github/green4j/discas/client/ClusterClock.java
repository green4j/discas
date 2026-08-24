/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.identity.NodeId;

/**
 * This client's estimate of the cluster's wall clock.
 *
 * <h2>What it is for</h2>
 * A lock lease is a deadline written by one client and judged by another; the node never sees it,
 * because locks are a client-side convention over CAS. So the divergence that matters is between
 * two <em>clients</em>, and left to their own clocks two of them an hour apart disagree by an hour
 * about whether a lease has run out.
 * <p>
 * The answer is correction, not policing. Every coordinator reports its own clock in the hello
 * response; the client derives an offset from it and expresses leases in <em>corrected</em> time,
 * so every client measures against the same reference and nothing has to be refused. Disconnecting
 * clients whose clocks diverge would instead turn a narrow correctness caveat into a broad
 * availability failure, since one NTP nudge would drop every client at once.
 * <p>
 * This makes the peers the shared reference, so <b>run NTP on them</b> -- which a cluster needs
 * anyway, since {@code PEER_HELLO} refuses a peer whose timestamp is more than five minutes from
 * ours. Clients need no clock discipline once the offset is applied.
 *
 * <h2>How the offset stays honest</h2>
 * An offset is measured at an instant, and applying it later needs to know how long ago that was --
 * which has to come from the monotonic clock, or a wall-clock step corrupts the very correction
 * meant to absorb it. The same pair detects the step: this holds both readings from when the offset
 * was taken, and a divergence between the wall delta and the monotonic delta beyond
 * {@link #STEP_THRESHOLD_MS} means <em>this</em> client's clock jumped. The offset is then stale --
 * it described a relationship that no longer holds -- so it is dropped rather than applied, and the
 * next handshake re-measures. A client can therefore tell its own clock moved under it without
 * asking anyone, and only has to ask when it did.
 *
 * <h2>What this does not fix, and nothing does</h2>
 * A holder paused past its lease by GC or a VM freeze has a perfectly correct clock. That is the
 * Redlock objection proper, and the answer is the fencing token
 * ({@link io.github.green4j.discas.client.lock.Lock#fencingToken()}) -- guarding the protected
 * resource with it stays mandatory whatever the clocks do.
 *
 * <p>Thread-safe: reads happen on the client's event loop and on its callback pool, writes on
 * whichever thread completed a handshake.
 */
public final class ClusterClock {

    /**
     * How far the wall-clock delta may drift from the monotonic delta before this client concludes
     * its own clock stepped rather than merely ran.
     * <p>
     * Well above ordinary crystal drift -- tens of ppm is seconds a day, milliseconds over the life
     * of one connection -- and well below the smallest step worth reacting to. NTP disciplines by
     * slewing under this; what lands above it is an operator setting the clock, a leap-second jump,
     * or a VM restored from a snapshot.
     */
    public static final long STEP_THRESHOLD_MS = 500L;

    private final TimeSource time;
    private final ClientObserver observer;

    /** Correction applied to this client's wall clock, valid only while {@link #offsetKnown}. */
    private long offsetMillis;
    private boolean offsetKnown;

    /** The pair of readings the offset was measured against; see the class javadoc. */
    private long anchorWallMillis;
    private long anchorMonotonicNanos;

    public ClusterClock(final TimeSource time, final ClientObserver observer) {
        this.time = time == null ? TimeSource.SYSTEM : time;
        this.observer = observer == null ? ClientObserver.NONE : observer;
    }

    /**
     * Fold in a coordinator's clock reading from a completed handshake.
     *
     * @param node               who answered, for the observer
     * @param coordinatorEpochMs the wall clock the coordinator reported
     * @param sentMonotonicNanos this client's monotonic reading when it sent the hello, which is
     *                           what makes the round trip measurable
     */
    public synchronized void observeCoordinatorTime(final NodeId node,
                                                    final long coordinatorEpochMs,
                                                    final long sentMonotonicNanos) {
        final long monotonicNow = time.monotonicNanos();
        final long wallNow = time.wallMillis();
        final long roundTripMillis = Math.max(0L, (monotonicNow - sentMonotonicNanos) / 1_000_000L);
        // The reading was taken somewhere inside the round trip; halving it is the same assumption
        // every clock-sync protocol makes -- that the two legs are roughly equal. Being wrong about
        // that costs at most half a round trip, which on any link where a lease is measured in
        // seconds is noise.
        final long correctedCoordinatorNow = coordinatorEpochMs + roundTripMillis / 2L;
        offsetMillis = correctedCoordinatorNow - wallNow;
        offsetKnown = true;
        anchorWallMillis = wallNow;
        anchorMonotonicNanos = monotonicNow;
        observer.clockOffsetMeasured(node, offsetMillis, roundTripMillis);
    }

    /**
     * The cluster's wall clock as this client best understands it: its own clock plus the measured
     * offset, or its own clock alone when no offset is known or the one held has been invalidated
     * by a step in this client's clock.
     * <p>
     * Falling back to the uncorrected clock is deliberate. A client that refused to answer "what
     * time is it" until it had spoken to a coordinator could not take a lock at all before its
     * first handshake, which trades a bounded inaccuracy for an outage.
     */
    public synchronized long nowMillis() {
        final long wallNow = time.wallMillis();
        if (!offsetKnown) {
            return wallNow;
        }
        final long monotonicElapsedMs = (time.monotonicNanos() - anchorMonotonicNanos) / 1_000_000L;
        final long wallElapsedMs = wallNow - anchorWallMillis;
        final long stepMs = wallElapsedMs - monotonicElapsedMs;
        if (Math.abs(stepMs) > STEP_THRESHOLD_MS) {
            offsetKnown = false;
            observer.clientClockStepped(stepMs);
            return wallNow;
        }
        return wallNow + offsetMillis;
    }

    /** Whether a usable offset is currently held. Diagnostic; {@link #nowMillis()} answers regardless. */
    public synchronized boolean offsetKnown() {
        return offsetKnown;
    }

    /**
     * The correction currently applied, in milliseconds; {@code 0} when none is held. Exposed for
     * metrics and for the tests that have to show a correction actually being applied.
     */
    public synchronized long offsetMillis() {
        return offsetKnown ? offsetMillis : 0L;
    }

    /**
     * A monotonic reading from the same source, for the one job a shared clock cannot do: measuring
     * how much of a lease its holder has left. See {@link TimeSource}.
     */
    public long monotonicNanos() {
        return time.monotonicNanos();
    }
}
