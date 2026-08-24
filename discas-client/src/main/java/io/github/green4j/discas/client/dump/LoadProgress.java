/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.dump;

/**
 * Told how far a load has got, once per pair written.
 *
 * <p><b>Relative</b>, unlike {@link DumpProgress}, and the asymmetry is not an oversight: a load
 * knows its total before it writes anything, because the dump's trailer carries the entry count and
 * the reader has already proven the file whole. A dump has nothing of the kind -- a scan pages
 * through a key space with no count in front of it -- so it can only report what it has done. Show
 * what is knowable, rather than always showing a percentage.
 */
@FunctionalInterface
public interface LoadProgress {

    /** Ignores it. The default when a caller asks for a load without asking to watch it. */
    LoadProgress NONE = (written, total) -> { };

    /**
     * @param pairsWritten pairs written so far
     * @param pairsTotal   pairs in the dump, from its trailer
     */
    void advanced(long pairsWritten, long pairsTotal);

    /**
     * The cleanup pass, and <b>absolute again</b>: it walks the cluster's key space, which has no
     * count in front of it any more than a dump's scan did. The same rule, not a second one --
     * report what the operation can know, which changes when the source of truth changes.
     *
     * @param keysExamined keys the cleanup has looked at
     * @param keysDeleted  keys it has deleted, being the ones the dump did not carry
     */
    default void cleaning(long keysExamined, long keysDeleted) {
    }
}
