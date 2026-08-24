/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * Fixed 64-byte snapshot file header
 * <p>
 * Layout (big-endian):
 * [0..3]   magic           4 bytes  0x534E5031 ("SNP1")
 * [4..7]   formatVersion   4 bytes
 * [8..11]  payloadCrc32    4 bytes  CRC of entire payload after header
 * [12..15] headerCrc32     4 bytes  CRC of bytes [0..11]
 * [16..63] reserved        48 bytes (zeroed)
 */
final class SnapshotFileHeader {

    static final int HEADER_SIZE = StorageFormat.SNAPSHOT_HEADER_BYTES;
    public static final int MAGIC = 0x534E5031;

    private final int formatVersion;
    private final int payloadCrc32;

    SnapshotFileHeader(final int formatVersion, final int payloadCrc32) {
        this.formatVersion = formatVersion;
        this.payloadCrc32 = payloadCrc32;
    }

    public int formatVersion() {
        return formatVersion;
    }

    int payloadCrc32() {
        return payloadCrc32;
    }

    public void encode(final ByteBuffer destination) {
        final int startPosition = destination.position();
        destination.order(ByteOrder.BIG_ENDIAN);
        destination.putInt(MAGIC);
        destination.putInt(formatVersion);
        destination.putInt(payloadCrc32);

        final CRC32 crc = new CRC32();
        for (int i = startPosition; i < startPosition + 12; i++) {
            crc.update(destination.get(i));
        }
        destination.putInt((int) crc.getValue());

        for (int i = 16; i < HEADER_SIZE; i++) {
            destination.put((byte) 0);
        }
    }

    static void encodePlaceholderTo(final ByteBuffer destination,
                                           final int formatVersion) {
        new SnapshotFileHeader(formatVersion, 0).encode(destination);
    }

    public static SnapshotFileHeader decode(final ByteBuffer source) {
        if (source.remaining() < HEADER_SIZE) {
            return null;
        }

        final int startPosition = source.position();
        source.order(ByteOrder.BIG_ENDIAN);

        final int magic = source.getInt();
        if (magic != MAGIC) {
            source.position(startPosition + HEADER_SIZE);
            return null;
        }

        final int formatVersion = source.getInt();
        final int payloadCrc = source.getInt();
        final int storedHeaderCrc = source.getInt();

        final CRC32 crc = new CRC32();
        for (int i = startPosition; i < startPosition + 12; i++) {
            crc.update(source.get(i));
        }

        source.position(startPosition + HEADER_SIZE);

        if ((int) crc.getValue() != storedHeaderCrc) {
            return null;
        }

        return new SnapshotFileHeader(formatVersion, payloadCrc);
    }
}
