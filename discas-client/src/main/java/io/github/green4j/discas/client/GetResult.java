/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import java.nio.ByteBuffer;

/**
 * The answer to a {@link DisCasClient#get}: a key's value paired with its {@link Version}.
 * {@link #value()} is {@code null} when the key is absent or tombstoned; {@link #version()} still
 * advances on a delete, so a caller can distinguish "never existed" ({@link Version#INITIAL},
 * which {@code equals} compares by value) from "deleted" (a later version with a {@code null}
 * value).
 */
public final class GetResult {

    private final ByteBuffer value;
    private final Version version;

    GetResult(final ByteBuffer value, final Version version) {
        this.value = value;
        this.version = version == null ? Version.INITIAL : version;
    }

    /** The value bytes, or {@code null} if the key is absent or tombstoned. */
    public ByteBuffer value() {
        return value;
    }

    /** The per-key version of this value. */
    public Version version() {
        return version;
    }

    /** True if the key currently holds a value (not absent, not tombstoned). */
    public boolean exists() {
        return value != null;
    }

    @Override
    public String toString() {
        return "GetResult[exists=" + exists() + ", version=" + version + "]";
    }
}
