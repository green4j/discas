/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

/**
 * The binary shape of one audit record, shared by the recorder that writes it on the event loop
 * and the {@link AuditEvent} that reads it on the drain thread.
 * <p>
 * A fixed part, then six length-prefixed sections in a fixed order. The fixed part is eight-byte
 * aligned so the {@code long} fields land on their natural offsets.
 * <p>
 * The record names its own digest -- algorithm and salt id -- rather than leaving a reader to
 * assume the settings in force when the line was printed were the ones in force when it was
 * recorded.
 */
final class AuditRecordLayout {

    static final int KIND = 0;
    static final int OP = 1;
    static final int CODE = 2;
    static final int FLAGS = 3;
    static final int HASH_ALGORITHM = 4;
    // 5..7 unused: padding to the first long.
    static final int TIMESTAMP_MILLIS = 8;
    static final int CORRELATION_ID = 16;
    static final int VERSION_COUNTER = 24;
    static final int LOST_RECORDS = 32;
    static final int KEY_SIZE = 40;
    static final int VALUE_SIZE = 44;
    static final int COUNT = 48;
    static final int HASH_SALT_ID = 52;
    /** How many of the kept key bytes are its head; the rest are its tail. */
    static final int KEY_HEAD = 56;
    static final int VALUE_HEAD = 60;
    static final int SECTIONS = 64;

    static final int SECTION_LABEL = 0;
    static final int SECTION_KEY = 1;
    static final int SECTION_VALUE = 2;
    static final int SECTION_KEY_HASH = 3;
    static final int SECTION_VALUE_HASH = 4;
    static final int SECTION_VERSION_NODE = 5;
    static final int SECTION_COUNT = 6;

    static final int FLAG_OK = 1;
    static final int FLAG_KEY_TRUNCATED = 1 << 1;
    static final int FLAG_VALUE_TRUNCATED = 1 << 2;
    static final int FLAG_KEY_TEXT = 1 << 3;
    static final int FLAG_VALUE_TEXT = 1 << 4;
    static final int FLAG_KEY_HASH_SKIPPED = 1 << 5;
    static final int FLAG_VALUE_HASH_SKIPPED = 1 << 6;
    static final int FLAG_SERIALIZABLE = 1 << 7;

    private AuditRecordLayout() {
    }

    static int getInt(final byte[] b, final int offset) {
        return ((b[offset] & 0xff) << 24)
                | ((b[offset + 1] & 0xff) << 16)
                | ((b[offset + 2] & 0xff) << 8)
                | (b[offset + 3] & 0xff);
    }

    static void putInt(final byte[] b, final int offset, final int value) {
        b[offset] = (byte) (value >>> 24);
        b[offset + 1] = (byte) (value >>> 16);
        b[offset + 2] = (byte) (value >>> 8);
        b[offset + 3] = (byte) value;
    }

    static long getLong(final byte[] b, final int offset) {
        return ((long) getInt(b, offset) << 32) | (getInt(b, offset + 4) & 0xffffffffL);
    }

    static void putLong(final byte[] b, final int offset, final long value) {
        putInt(b, offset, (int) (value >>> 32));
        putInt(b, offset + 4, (int) value);
    }
}
