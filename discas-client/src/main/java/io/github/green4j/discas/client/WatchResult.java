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
 */
public final class WatchResult {

    private final ByteBuffer value;
    private final Version version;
    private final boolean changed;

    private WatchResult(final ByteBuffer value, final Version version, final boolean changed) {
        this.value = value;
        this.version = version == null ? Version.INITIAL : version;
        this.changed = changed;
    }

    static WatchResult changed(final GetResult observed) {
        return new WatchResult(observed.value(), observed.version(), true);
    }

    static WatchResult unchanged(final GetResult observed) {
        return new WatchResult(observed.value(), observed.version(), false);
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

    /** True if the key currently holds a value (not absent, not tombstoned). */
    public boolean exists() {
        return value != null;
    }

    @Override
    public String toString() {
        return "WatchResult[changed=" + changed + ", exists=" + exists() + ", version=" + version + "]";
    }
}
