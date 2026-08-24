/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.dump;

import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * The on-disk framing of a cluster dump: the file {@link DumpWriter} produces and
 * {@link DumpReader} refuses unless it is whole.
 *
 * <pre>
 *   magic "DCDUMP"      6 bytes
 *   formatVersion       int
 *   headerLength        int      bytes of the header block that follows
 *   header block:       createdAtEpochMs (long)
 *                       prefixCount (int), then prefixCount * (length-prefixed bytes)
 *   entries:            repeated { int keyLength, key bytes, int valueLength, value bytes }
 *   end of entries:     int -1
 *   trailer:            entryCount (long), CRC32 (int) of every byte above it
 * </pre>
 *
 * <p>A file of its own, neither a WAL segment nor a snapshot, so that no restore procedure ever
 * reads <em>copy this file into the data directory</em> -- one short step from copying a whole
 * directory, which is the one thing this store cannot allow.
 *
 * <p>The entry stream ends with a sentinel rather than a count up front, because the writer
 * streams and cannot know the count before the last pair. The count is in the trailer, where it is
 * a check rather than a promise. Otherwise the layout follows the WAL's conventions: big-endian,
 * length-prefixed fields, CRC32 over everything.
 */
public final class DumpCodec {

    /** Leading sentinel: this is a dump, not a WAL segment (whose first field is a length). */
    static final byte[] MAGIC = {'D', 'C', 'D', 'U', 'M', 'P'};

    /** Bytes of {@link #MAGIC}. */
    public static final int MAGIC_BYTES = 6;

    /** Framing version of the file, recorded after the magic and checked on read. */
    public static final int FORMAT_VERSION = 1;

    /**
     * Prefixes one dump may be asked for. A cap rather than an opinion: the count is decoded from
     * the file, so it bounds what a corrupt header can make a reader allocate.
     */
    public static final int MAX_PREFIXES = 64;

    /** The {@code keyLength} value that ends the entry stream; no key can encode as negative. */
    static final int END_OF_ENTRIES = -1;

    /**
     * Upper bound on the header block, derived from {@link KvLimits}: a timestamp and up to
     * {@link #MAX_PREFIXES} prefixes, each bounded by a key.
     */
    static final int MAX_HEADER_BYTES =
            Long.BYTES
                    + Integer.BYTES
                    + MAX_PREFIXES * (Integer.BYTES + KvLimits.MAX_KEY_BYTES);

    private DumpCodec() {
    }

    /** Encodes the header block -- what follows {@code headerLength} in the file. */
    static byte[] encodeHeader(final DumpHeader header) {
        final MessageBufferWriter out = new MessageBufferWriter(64, MAX_HEADER_BYTES);
        out.writeLong(header.createdAtEpochMs());
        final List<ByteBuffer> prefixes = header.prefixes();
        out.writeInt(prefixes.size());
        for (final ByteBuffer prefix : prefixes) {
            out.writeBytes(prefix);
        }
        final ByteBuffer encoded = out.toByteBuffer();
        final byte[] block = new byte[encoded.remaining()];
        encoded.get(block);
        return block;
    }

    /**
     * Decodes a header block, refusing anything the writer could not have produced -- including
     * bytes left over at its end, which {@code headerLength} says are part of the header and the
     * layout has no field for.
     */
    static DumpHeader decodeHeader(final ByteBuffer block) throws DumpFormatException {
        try {
            final MessageBufferReader reader = new MessageBufferReader(block);
            final long createdAtEpochMs = reader.readLong();
            final int prefixCount = reader.readCount(Integer.BYTES);
            if (prefixCount > MAX_PREFIXES) {
                throw new DumpFormatException("Dump header claims " + prefixCount
                        + " prefixes, more than the maximum " + MAX_PREFIXES);
            }
            final List<ByteBuffer> prefixes = new ArrayList<>(prefixCount);
            for (int i = 0; i < prefixCount; i++) {
                prefixes.add(reader.readBytes());
            }
            if (reader.hasRemaining()) {
                throw new DumpFormatException("Dump header has trailing bytes");
            }
            return new DumpHeader(createdAtEpochMs, prefixes);
        } catch (final IllegalArgumentException | BufferUnderflowException malformed) {
            // Exactly the two ways MessageBufferReader reports input it cannot parse. Both mean
            // the same thing here.
            throw new DumpFormatException("Dump header is malformed", malformed);
        }
    }

    /** @throws IllegalArgumentException if a key of this size could not be stored anyway */
    static void checkKeyLength(final int length) {
        if (length < 0 || length > KvLimits.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("Key of " + length
                    + " bytes is outside 0.." + KvLimits.MAX_KEY_BYTES);
        }
    }

    /** @throws IllegalArgumentException if a value of this size could not be stored anyway */
    static void checkValueLength(final int length) {
        if (length < 0 || length > KvLimits.MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("Value of " + length
                    + " bytes is outside 0.." + KvLimits.MAX_VALUE_BYTES);
        }
    }
}
