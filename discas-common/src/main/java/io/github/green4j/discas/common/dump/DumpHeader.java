/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.dump;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What a dump says about itself: when it was taken and which prefixes were asked for.
 * <p>
 * The prefixes are recorded rather than enforced: they say what this file was asked to contain, so
 * an operator holding one can tell a partial backup from a whole one without reading it to the end.
 * An empty list means the whole key space was asked for.
 * <p>
 * <b>No identity of any kind</b> -- no cluster id, no node id, no ballots. A backup here is a
 * portable set of pairs, and every id it could carry is a name from the old cluster that means
 * nothing in the new one.
 * <p>
 * That a dump seeds a new cluster and never restores a member into an existing one is enforced
 * elsewhere: restore refuses a non-empty data directory and mints the incarnation itself, and a
 * dump carries neither promises nor ballots, so there is nothing in one to merge.
 */
public final class DumpHeader {

    private final long createdAtEpochMs;
    private final List<ByteBuffer> prefixes;

    public DumpHeader(final long createdAtEpochMs,
                      final List<ByteBuffer> prefixes) {
        if (prefixes != null && prefixes.size() > DumpCodec.MAX_PREFIXES) {
            throw new IllegalArgumentException("Too many prefixes: " + prefixes.size()
                    + " exceeds the maximum " + DumpCodec.MAX_PREFIXES);
        }
        this.createdAtEpochMs = createdAtEpochMs;

        // Copied on construction, like LockValueCodec.LockRecord: a header outlives the buffers it
        // was built from -- the writer's caller keeps writing, and the reader's block buffer is
        // reused by the next pass over the file.
        final List<ByteBuffer> copies = new ArrayList<>(prefixes == null ? 0 : prefixes.size());
        if (prefixes != null) {
            for (final ByteBuffer prefix : prefixes) {
                final ByteBuffer source = prefix.duplicate();
                final ByteBuffer copy = ByteBuffer.allocate(source.remaining());
                copy.put(source);
                copy.flip();
                copies.add(copy.asReadOnlyBuffer());
            }
        }
        this.prefixes = Collections.unmodifiableList(copies);
    }

    /** When the dump was started, in epoch millis on the dumping process's clock. */
    public long createdAtEpochMs() {
        return createdAtEpochMs;
    }

    /** The prefixes asked for, empty for the whole key space. Read-only views. */
    public List<ByteBuffer> prefixes() {
        final List<ByteBuffer> views = new ArrayList<>(prefixes.size());
        for (final ByteBuffer prefix : prefixes) {
            views.add(prefix.duplicate());
        }
        return Collections.unmodifiableList(views);
    }
}
