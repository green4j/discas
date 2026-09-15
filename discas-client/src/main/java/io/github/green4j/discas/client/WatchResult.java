/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import java.nio.ByteBuffer;

/**
 * The outcome of a {@link DisCasClient#watch} blocking query: the latest observed value and its
 * {@link Version}, plus whether the value changed within the wait budget.
 *
 * <p>Semantics are <b>coalescing</b> (latest-value), which is the only thing a CASPaxos register
 * store can express -- it keeps only the current value per key, with no history to replay. If the
 * key changed several times during the wait, {@link #changed()} is {@code true} and
 * {@link #value()}/{@link #version()} reflect the <em>latest</em> committed state; intermediate
 * values may be skipped. On {@code changed() == false} the wait elapsed with no advance past the
 * caller's version, and the value/version reflect the current (unchanged) state.
 *
 * <p>An unchanged answer also says whether it was {@link #confirmed()} -- whether a poll actually
 * succeeded at the end of the wait, or whether the polls were failing by then and this is the newest
 * thing any of them saw before the failures began. Both are the right answer for a blocking query,
 * and the difference matters only to a caller that measures how long it has been since it last
 * learnt anything.
 */
public final class WatchResult {

    private final ByteBuffer value;
    private final Version version;
    private final boolean changed;
    private final boolean confirmed;

    private WatchResult(final ByteBuffer value, final Version version, final boolean changed,
                        final boolean confirmed) {
        this.value = value;
        this.version = version == null ? Version.INITIAL : version;
        this.changed = changed;
        this.confirmed = confirmed;
    }

    static WatchResult changed(final GetResult observed) {
        // Only ever built straight after a poll that succeeded.
        return new WatchResult(observed.value(), observed.version(), true, true);
    }

    static WatchResult unchanged(final GetResult observed) {
        return new WatchResult(observed.value(), observed.version(), false, true);
    }

    static WatchResult unconfirmed(final GetResult best) {
        return new WatchResult(best.value(), best.version(), false, false);
    }

    /** The latest observed value bytes, or {@code null} if the key is absent or tombstoned. */
    public ByteBuffer value() {
        return value;
    }

    /** The version of the latest observed value; pass it back to the next {@code watch}. */
    public Version version() {
        return version;
    }

    /** True if the version advanced past the caller's version before the wait elapsed. */
    public boolean changed() {
        return changed;
    }

    /**
     * True if a poll succeeded at the end of the wait, so this is the state as of the deadline.
     * <p>
     * False when the polls were failing by the time the budget ran out and no poll succeeded after the
     * failures started: the value and version are then the newest any poll saw, which can be a whole
     * watch window old. A caller for which "nothing has changed" is evidence -- one counting how long
     * it has been since it could read the key at all -- should treat that as no evidence and read
     * again. A caller that only wants to know when to look again can ignore it.
     */
    public boolean confirmed() {
        return confirmed;
    }

    /** True if the key currently holds a value (not absent, not tombstoned). */
    public boolean exists() {
        return value != null;
    }

    @Override
    public String toString() {
        return "WatchResult[changed=" + changed + ", confirmed=" + confirmed
                + ", exists=" + exists() + ", version=" + version + "]";
    }
}
