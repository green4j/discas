/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.seed;

/**
 * Told how far the seeding of one member's directory has got, once per pair.
 *
 * <p>Relative: the dump's trailer carries the entry count and the reader has proven the file whole
 * before the first byte is written, so the total is known in advance. (A dump of a live cluster
 * cannot say the same, which is why its progress is absolute -- report what the operation can know.)
 */
@FunctionalInterface
public interface SeedProgress {

    /** Ignores it. */
    SeedProgress NONE = (written, total) -> { };

    /**
     * @param pairsWritten pairs written into this member's WAL so far
     * @param pairsTotal   pairs in the dump
     */
    void advanced(long pairsWritten, long pairsTotal);
}
