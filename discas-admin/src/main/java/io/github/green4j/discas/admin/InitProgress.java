/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin;

/**
 * Told how far {@code init} has got: which member is being written, and how far into the dump that
 * member is.
 *
 * <p>Relative on both counts, because both totals are known before anything is written -- the
 * membership was given on the command line and the pair count is in the dump's trailer.
 */
@FunctionalInterface
public interface InitProgress {

    /** Ignores it. */
    InitProgress NONE = (member, members, written, total) -> { };

    /**
     * @param member       1-based index of the member being written
     * @param members      how many members are being written in all
     * @param pairsWritten pairs written into this member so far
     * @param pairsTotal   pairs in the dump
     */
    void advanced(int member, int members, long pairsWritten, long pairsTotal);
}
