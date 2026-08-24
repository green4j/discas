/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.KvLimits;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * WAL record on-disk codec
 * <p>
 * On-disk frame:
 * <pre>
 *   [int  frameTotalLength]   total bytes after this field
 *   [long lsn]
 *   [int  recordType]
 *   [payload bytes ...]
 *   [int  recordCrc32]        CRC of (frameTotalLength + lsn + recordType + payload)
 * </pre>
 * <p>
 * {@code frameTotalLength = 8 (lsn) + 4 (recordType) + payloadLength + 4 (crc)}
 * <p>
 * The CRC covers the entire frame including the length prefix so a corrupted
 * length cannot drive the decoder forward before CRC verification rejects it.
 */
public final class WalRecordCodec {

    public static final int FRAME_OVERHEAD = 4 + 8 + 4 + 4;

    /**
     * Upper bound on the on-disk size of any single WAL record, derived from {@link KvLimits}.
     * The largest entries carry a key, up to two ballots (a snapshot entry), a value, and a
     * tombstone flag (see {@code EntryCodec}); framing is {@link #FRAME_OVERHEAD} plus a
     * 4-byte length prefix per length-prefixed field. The WAL write buffer is sized to hold
     * one such record, so a valid record can never overflow it.
     */
    public static final int MAX_RECORD_DISK_BYTES =
            FRAME_OVERHEAD
                    + Integer.BYTES + KvLimits.MAX_KEY_BYTES          // putBytes(key)
                    + 2 * KvLimits.MAX_BALLOT_BYTES                   // up to two ballots
                    + Integer.BYTES + KvLimits.MAX_VALUE_BYTES        // putNullableBytes(value)
                    + 1;                                             // tombstone flag

    private WalRecordCodec() {
    }

    public static void encode(final ByteBuffer destination,
                              final long lsn,
                              final int recordType,
                              final ByteBuffer payload) {
        destination.order(ByteOrder.BIG_ENDIAN);
        final int startPosition = destination.position();
        final int payloadLength = (payload != null) ? payload.remaining() : 0;

        final int frameTotalLength = 8 + 4 + payloadLength + 4;
        destination.putInt(frameTotalLength);
        destination.putLong(lsn);
        destination.putInt(recordType);

        if (payload != null && payloadLength > 0) {
            destination.put(payload);
        }

        final CRC32 crc = new CRC32();
        final int crcEnd = destination.position();
        for (int i = startPosition; i < crcEnd; i++) {
            crc.update(destination.get(i));
        }
        destination.putInt((int) crc.getValue());
    }

    public static int diskSize(final int payloadLength) {
        return FRAME_OVERHEAD + payloadLength;
    }

    public static final class OffsetRecord {

        private final long lsn;
        private final int recordType;
        private final int payloadOffset;
        private final int payloadLength;
        private final int totalDiskBytes;

        public OffsetRecord(final long lsn,
                            final int recordType,
                            final int payloadOffset,
                            final int payloadLength,
                            final int totalDiskBytes) {
            this.lsn = lsn;
            this.recordType = recordType;
            this.payloadOffset = payloadOffset;
            this.payloadLength = payloadLength;
            this.totalDiskBytes = totalDiskBytes;
        }

        public long lsn() {
            return lsn;
        }

        public int recordType() {
            return recordType;
        }

        int payloadOffset() {
            return payloadOffset;
        }

        public int payloadLength() {
            return payloadLength;
        }

        public int totalDiskBytes() {
            return totalDiskBytes;
        }
    }

    /**
     * Decode one record returning offset-based metadata
     * The source buffer's position is advanced past the record on success
     * Returns null on truncation or CRC failure (position unchanged)
     */
    public static OffsetRecord decodeToOffsets(final ByteBuffer source) {
        source.order(ByteOrder.BIG_ENDIAN);
        final int startPosition = source.position();

        if (source.remaining() < 4) {
            return null;
        }

        final int frameTotalLength = source.getInt(startPosition);
        if (frameTotalLength < 8 + 4 + 4) {
            return null;
        }

        final int totalDiskBytes = 4 + frameTotalLength;
        if (startPosition + totalDiskBytes > source.limit()) {
            return null;
        }

        final long lsn = source.getLong(startPosition + 4);
        final int recordType = source.getInt(startPosition + 12);
        final int payloadLength = frameTotalLength - 8 - 4 - 4;
        final int payloadOffset = startPosition + 16;
        final int crcOffset = payloadOffset + payloadLength;
        final int storedCrc = source.getInt(crcOffset);

        final CRC32 crc = new CRC32();
        for (int i = startPosition; i < crcOffset; i++) {
            crc.update(source.get(i));
        }
        if ((int) crc.getValue() != storedCrc) {
            return null;
        }

        source.position(startPosition + totalDiskBytes);

        return new OffsetRecord(lsn, recordType, payloadOffset, payloadLength, totalDiskBytes);
    }
}
