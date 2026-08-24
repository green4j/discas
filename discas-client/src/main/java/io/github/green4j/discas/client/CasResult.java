/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import java.nio.ByteBuffer;

/**
 * The answer to a version-fenced {@link DisCasClient#cas(ByteBuffer, Version, ByteBuffer)}.
 * <p>
 * Carries the version as well as the value, so a caller that lost the compare has everything it
 * needs to recompute without a second round trip, and never has to guess whether it lost on
 * value or on ballot.
 */
public final class CasResult {
    private final boolean swapped;
    private final ByteBuffer value;
    private final Version version;

    public CasResult(final boolean swapped, final ByteBuffer value, final Version version) {
        this.swapped = swapped;
        this.value = value;
        this.version = version == null ? Version.INITIAL : version;
    }

    /** True when the key was at the expected version and the desired value was committed. */
    public boolean swapped() {
        return swapped;
    }

    /**
     * The key's value as of the round: the desired value when {@link #swapped()}, otherwise
     * the value that was there instead. {@code null} when the key is absent or tombstoned.
     */
    public ByteBuffer value() {
        return value;
    }

    /**
     * The key's version as of the round -- after the swap when {@link #swapped()}, and the
     * version that won otherwise. Feed it straight back into the next attempt.
     */
    public Version version() {
        return version;
    }
}
