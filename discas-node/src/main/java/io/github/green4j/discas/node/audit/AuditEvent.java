/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.node.acl.ClientOp;

/**
 * One record, read where it lies in the ring, and <b>valid only for the {@link AuditLog#record}
 * call it is passed to</b>: the space goes back to the event loop when that returns, so anything
 * that outlives the call must be copied out. Which fields carry anything depends on
 * {@link #kind()}.
 * <p>
 * One instance is reused for every record, and nothing here allocates: text is appended to a
 * builder the caller already has, and bytes are addressed as an offset and a length into
 * {@link #recordBytes()}.
 */
public final class AuditEvent {

    private byte[] buffer;
    private int offset;
    private final int[] sectionOffset = new int[AuditRecordLayout.SECTION_COUNT];
    private final int[] sectionLength = new int[AuditRecordLayout.SECTION_COUNT];

    /** Points this view at a record; called by the drain, once per record. */
    void wrap(final byte[] recordBuffer, final int recordOffset, final int recordLength) {
        this.buffer = recordBuffer;
        this.offset = recordOffset;
        int at = recordOffset + AuditRecordLayout.SECTIONS;
        final int end = recordOffset + recordLength;
        for (int i = 0; i < AuditRecordLayout.SECTION_COUNT; i++) {
            if (at + Integer.BYTES > end) {
                sectionOffset[i] = at;
                sectionLength[i] = 0;
                continue;
            }
            final int length = AuditRecordLayout.getInt(buffer, at);
            at += Integer.BYTES;
            sectionOffset[i] = at;
            sectionLength[i] = length;
            at += length;
        }
    }

    public AuditEventKind kind() {
        return AuditEventKind.fromCode(buffer[offset + AuditRecordLayout.KIND]);
    }

    /** When the loop recorded this, by the wall clock. */
    public long timestampMillis() {
        return AuditRecordLayout.getLong(buffer, offset + AuditRecordLayout.TIMESTAMP_MILLIS);
    }

    /** The client, as {@code clientId} or {@code clientId (description)}. Always present. */
    public void appendLabel(final StringBuilder out) {
        append(AuditRecordLayout.SECTION_LABEL, out);
    }

    /** For {@link AuditEventKind#REQUEST} and {@link AuditEventKind#RESULT}. */
    public ClientOp op() {
        final byte code = buffer[offset + AuditRecordLayout.OP];
        return code == 0 ? null : ClientOp.fromCode((char) code);
    }

    /** Pairs a result with its request. */
    public long correlationId() {
        return AuditRecordLayout.getLong(buffer, offset + AuditRecordLayout.CORRELATION_ID);
    }

    /** RESULT: whether the operation succeeded. */
    public boolean ok() {
        return flag(AuditRecordLayout.FLAG_OK);
    }

    /** RESULT: how it failed, or {@link ClientErrorCode#NONE}. */
    public ClientErrorCode errorCode() {
        return ClientErrorCode.fromCode(buffer[offset + AuditRecordLayout.CODE]);
    }

    /** SESSION_REFUSED: why. */
    public ClientHelloRespStatus helloStatus() {
        return ClientHelloRespStatus.fromCode(buffer[offset + AuditRecordLayout.CODE]);
    }

    /**
     * A ballot counter: the version a RESULT committed or observed, the version a CAS REQUEST was
     * fenced on. Zero when the operation has none.
     */
    public long versionCounter() {
        return AuditRecordLayout.getLong(buffer, offset + AuditRecordLayout.VERSION_COUNTER);
    }

    /** The node whose ballot {@link #versionCounter()} belongs to; appends nothing when none. */
    public void appendVersionNode(final StringBuilder out) {
        append(AuditRecordLayout.SECTION_VERSION_NODE, out);
    }

    /** RESULT: entries a scan returned. */
    public int count() {
        return AuditRecordLayout.getInt(buffer, offset + AuditRecordLayout.COUNT);
    }

    /** RECORDS_LOST: how many. */
    public long lostRecords() {
        return AuditRecordLayout.getLong(buffer, offset + AuditRecordLayout.LOST_RECORDS);
    }

    /** The key's full size on the wire, whatever share of it was kept. */
    public int keySize() {
        return AuditRecordLayout.getInt(buffer, offset + AuditRecordLayout.KEY_SIZE);
    }

    /** The value's full size on the wire, whatever share of it was kept. */
    public int valueSize() {
        return AuditRecordLayout.getInt(buffer, offset + AuditRecordLayout.VALUE_SIZE);
    }

    /** The array the sections below are addressed in; valid for this call only. */
    public byte[] recordBytes() {
        return buffer;
    }

    /** The kept bytes: the whole key, or its head then its tail -- see {@link #keyTruncated()}. */
    public int keyOffset() {
        return sectionOffset[AuditRecordLayout.SECTION_KEY];
    }

    public int keyLength() {
        return sectionLength[AuditRecordLayout.SECTION_KEY];
    }

    public int valueOffset() {
        return sectionOffset[AuditRecordLayout.SECTION_VALUE];
    }

    public int valueLength() {
        return sectionLength[AuditRecordLayout.SECTION_VALUE];
    }

    /** How many of the kept key bytes are its head; the rest are its tail. */
    public int keyHeadBytes() {
        return AuditRecordLayout.getInt(buffer, offset + AuditRecordLayout.KEY_HEAD);
    }

    public int valueHeadBytes() {
        return AuditRecordLayout.getInt(buffer, offset + AuditRecordLayout.VALUE_HEAD);
    }

    public boolean keyTruncated() {
        return flag(AuditRecordLayout.FLAG_KEY_TRUNCATED);
    }

    public boolean valueTruncated() {
        return flag(AuditRecordLayout.FLAG_VALUE_TRUNCATED);
    }

    /** True when the kept bytes are to be read as text rather than hex. */
    public boolean keyText() {
        return flag(AuditRecordLayout.FLAG_KEY_TEXT);
    }

    public boolean valueText() {
        return flag(AuditRecordLayout.FLAG_VALUE_TEXT);
    }

    /** True when a read asked to be answered from local state rather than a round. */
    public boolean serializable() {
        return flag(AuditRecordLayout.FLAG_SERIALIZABLE);
    }

    /**
     * The digest function this record used. Held as the ordinal: unlike the wire enums, a record
     * is written and read by one build in one process and never leaves it.
     */
    public AuditHashAlgorithm hashAlgorithm() {
        return AuditHashAlgorithm.fromOrdinal(buffer[offset + AuditRecordLayout.HASH_ALGORITHM]);
    }

    /** Public name of the salt the digests were taken with, {@code 0} when unsalted. */
    public int hashSaltId() {
        return AuditRecordLayout.getInt(buffer, offset + AuditRecordLayout.HASH_SALT_ID);
    }

    /** True when the field was too large to digest under the configured limit. */
    public boolean keyHashSkipped() {
        return flag(AuditRecordLayout.FLAG_KEY_HASH_SKIPPED);
    }

    public boolean valueHashSkipped() {
        return flag(AuditRecordLayout.FLAG_VALUE_HASH_SKIPPED);
    }

    public int keyHashOffset() {
        return sectionOffset[AuditRecordLayout.SECTION_KEY_HASH];
    }

    public int keyHashLength() {
        return sectionLength[AuditRecordLayout.SECTION_KEY_HASH];
    }

    public int valueHashOffset() {
        return sectionOffset[AuditRecordLayout.SECTION_VALUE_HASH];
    }

    public int valueHashLength() {
        return sectionLength[AuditRecordLayout.SECTION_VALUE_HASH];
    }

    private boolean flag(final int mask) {
        return (buffer[offset + AuditRecordLayout.FLAGS] & mask) != 0;
    }

    private void append(final int index, final StringBuilder out) {
        AuditText.decode(buffer, sectionOffset[index], sectionLength[index], out);
    }
}
