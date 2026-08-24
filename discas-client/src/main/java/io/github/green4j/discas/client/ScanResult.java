/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import java.nio.ByteBuffer;

/**
 * One entry of a {@link DisCasClient#scan()} result: a key and its version.
 * <p>
 * There is <strong>no value</strong>. Cross-key value consistency is the part a
 * logless, per-key-register store cannot honestly merge; key existence is the part quorum
 * intersection genuinely covers. Use {@link #version()} as a watch position and {@code get(key)} when
 * a committed value is needed.
 */
public final class ScanResult {
    private final ByteBuffer key;
    private final Version version;

    public ScanResult(final ByteBuffer key, final Version version) {
        this.key = key;
        this.version = version == null ? Version.INITIAL : version;
    }

    /** The key, exactly as stored. */
    public ByteBuffer key() {
        return key;
    }

    /** The key's version at scan time -- usable as a {@code watch} starting point. */
    public Version version() {
        return version;
    }
}
