/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

/**
 * The on-disk format invariants of the WAL and snapshot files: fixed header sizes and the two
 * versions a node validates before reading anything.
 * <p>
 * These are compile-time properties of the binary, not tuning knobs, and they lived on
 * {@code StorageConfig} alongside the knobs -- which also gave that config a
 * {@code layoutVersion()} "getter" that returned a constant. They belong next to their framing,
 * the way {@code KvLimits} holds the protocol maxima.
 */
public final class StorageFormat {

    /** Fixed size of a WAL segment's header, in bytes. */
    public static final int WAL_HEADER_BYTES = 64;

    /** Fixed size of a snapshot file's header, in bytes. */
    public static final int SNAPSHOT_HEADER_BYTES = 64;

    /**
     * Fixed size of a snapshot file's metadata block, which follows the header: four longs --
     * totalEntries, the proposer's ballot reservation, the acceptor's promise ceiling, snapshotLsn.
     */
    public static final int SNAPSHOT_META_BYTES = 32;

    /**
     * Byte-level framing version of WAL records and snapshot payloads, recorded in every file
     * header and checked on read.
     * <p>
     * It distinguishes one <em>released</em> framing from another, so it is bumped on the first
     * framing change after a release and never before one: a value above the number of shipped
     * formats claims a compatibility history that has no migration code behind it.
     */
    public static final int FORMAT_VERSION = 1;

    /**
     * On-disk storage-scheme version, encoded as the {@code %03d-} prefix of every WAL/snapshot
     * file name: how the store lays its persisted state out across segments and snapshots. A
     * node refuses to read files written under a different layout version rather than silently
     * ignoring them.
     * <p>
     * Distinct from {@link #FORMAT_VERSION}, which versions the record/header framing inside a
     * file. The two evolve independently.
     */
    public static final int LAYOUT_VERSION = 1;

    private StorageFormat() {
    }
}
