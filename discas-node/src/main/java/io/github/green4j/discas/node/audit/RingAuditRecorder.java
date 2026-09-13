/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.identity.ClientIdentity;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.node.acl.ClientOp;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Encodes events into the {@link AuditRing}, on the event loop.
 * <p>
 * How much of a key or a value is kept, and whether it is digested, is decided here: the ring keeps
 * only what is copied into it. The digest limit exists because that is the one step whose cost
 * grows with the size of the data.
 * <p>
 * Scratch and digest are held per recorder and reused; there is one recorder per node, touched
 * only by its loop thread. <b>The buffers passed to {@link #requestReceived} are consumed</b> --
 * callers pass a view they own, which is what a decoded {@code ClientMessage} accessor returns.
 */
public final class RingAuditRecorder implements AuditRecorder {

    /** Kept free for the fixed part, section headers, label, version node and digests. */
    private static final int OVERHEAD_BYTES = 1024;
    private static final int MIN_SCRATCH_BYTES = 8 * 1024;
    private static final int MAX_SCRATCH_BYTES = 1024 * 1024;
    private static final int MAX_DIGEST_BYTES = 64;

    private final AuditRing ring;
    private final byte[] scratch;
    private final byte[] digestOut = new byte[MAX_DIGEST_BYTES];

    private volatile AuditConfigSnapshot settings;

    private MessageDigest digest;
    private AuditHashAlgorithm digestAlgorithm = AuditHashAlgorithm.NONE;
    private int position;

    public RingAuditRecorder(final AuditRing ring, final AuditConfigSnapshot settings) {
        this.ring = ring;
        this.settings = settings;
        final int scratchBytes = Math.max(MIN_SCRATCH_BYTES,
                Math.min(MAX_SCRATCH_BYTES, ring.capacity() / 4));
        this.scratch = new byte[scratchBytes];
    }

    /** Applies reloaded settings, on the loop, so no record is encoded half under each. */
    public void settings(final AuditConfigSnapshot updated) {
        this.settings = updated;
    }

    public AuditConfigSnapshot settings() {
        return settings;
    }

    @Override
    public void sessionOpened(final ClientIdentity identity) {
        session(AuditEventKind.SESSION_OPENED, identity, ClientHelloRespStatus.OK);
    }

    @Override
    public void sessionRefused(final ClientIdentity identity, final ClientHelloRespStatus status) {
        session(AuditEventKind.SESSION_REFUSED, identity, status);
    }

    @Override
    public void sessionClosed(final ClientIdentity identity) {
        session(AuditEventKind.SESSION_CLOSED, identity, ClientHelloRespStatus.OK);
    }

    @Override
    public void requestReceived(final ClientIdentity identity, final ClientOp op,
                                final long correlationId, final ByteBuffer key,
                                final ByteBuffer value, final long expectedVersion,
                                final boolean serializable) {
        final AuditConfigSnapshot cfg = settings;
        start(AuditEventKind.REQUEST, cfg);
        scratch[AuditRecordLayout.OP] = (byte) op.code();
        AuditRecordLayout.putLong(scratch, AuditRecordLayout.CORRELATION_ID, correlationId);
        AuditRecordLayout.putLong(scratch, AuditRecordLayout.VERSION_COUNTER, expectedVersion);
        if (serializable) {
            flag(AuditRecordLayout.FLAG_SERIALIZABLE);
        }
        final boolean full = cfg.rendersFull(key);
        final boolean text = cfg.rendersText(key);
        putLabel(identity);
        putField(key, cfg, full, text, AuditRecordLayout.KEY_SIZE, AuditRecordLayout.KEY_HEAD,
                AuditRecordLayout.FLAG_KEY_TRUNCATED, AuditRecordLayout.FLAG_KEY_TEXT);
        putField(value, cfg, full, text, AuditRecordLayout.VALUE_SIZE, AuditRecordLayout.VALUE_HEAD,
                AuditRecordLayout.FLAG_VALUE_TRUNCATED, AuditRecordLayout.FLAG_VALUE_TEXT);
        putDigest(key, cfg, AuditRecordLayout.FLAG_KEY_HASH_SKIPPED);
        putDigest(value, cfg, AuditRecordLayout.FLAG_VALUE_HASH_SKIPPED);
        putSection(null, 0, 0);
        commit(cfg);
    }

    @Override
    public void requestCompleted(final ClientIdentity identity, final ClientOp op,
                                 final long correlationId, final boolean ok,
                                 final ClientErrorCode errorCode, final Ballot version,
                                 final int count) {
        final AuditConfigSnapshot cfg = settings;
        start(AuditEventKind.RESULT, cfg);
        scratch[AuditRecordLayout.OP] = (byte) op.code();
        scratch[AuditRecordLayout.CODE] =
                errorCode == null ? ClientErrorCode.NONE.code() : errorCode.code();
        AuditRecordLayout.putLong(scratch, AuditRecordLayout.CORRELATION_ID, correlationId);
        AuditRecordLayout.putInt(scratch, AuditRecordLayout.COUNT, count);
        if (ok) {
            flag(AuditRecordLayout.FLAG_OK);
        }
        if (version != null) {
            AuditRecordLayout.putLong(scratch, AuditRecordLayout.VERSION_COUNTER, version.counter());
        }
        putLabel(identity);
        putSection(null, 0, 0);
        putSection(null, 0, 0);
        putSection(null, 0, 0);
        putSection(null, 0, 0);
        if (version == null || version.nodeId() == null) {
            putSection(null, 0, 0);
        } else {
            putText(version.nodeId().value());
        }
        commit(cfg);
    }

    private void session(final AuditEventKind kind, final ClientIdentity identity,
                         final ClientHelloRespStatus status) {
        final AuditConfigSnapshot cfg = settings;
        start(kind, cfg);
        scratch[AuditRecordLayout.CODE] = status.code();
        putLabel(identity);
        putSection(null, 0, 0);
        putSection(null, 0, 0);
        putSection(null, 0, 0);
        putSection(null, 0, 0);
        putSection(null, 0, 0);
        commit(cfg);
    }

    private void start(final AuditEventKind kind, final AuditConfigSnapshot cfg) {
        Arrays.fill(scratch, 0, AuditRecordLayout.SECTIONS, (byte) 0);
        scratch[AuditRecordLayout.KIND] = kind.code();
        scratch[AuditRecordLayout.HASH_ALGORITHM] = (byte) cfg.hashAlgorithm().ordinal();
        AuditRecordLayout.putInt(scratch, AuditRecordLayout.KEY_SIZE, -1);
        AuditRecordLayout.putInt(scratch, AuditRecordLayout.VALUE_SIZE, -1);
        AuditRecordLayout.putInt(scratch, AuditRecordLayout.HASH_SALT_ID, cfg.hashSaltIdCode());
        AuditRecordLayout.putLong(scratch, AuditRecordLayout.TIMESTAMP_MILLIS,
                System.currentTimeMillis());
        position = AuditRecordLayout.SECTIONS;
    }

    private void commit(final AuditConfigSnapshot cfg) {
        if (cfg.overflow() == AuditOverflow.WAIT) {
            ring.offerOrWait(scratch, position);
            return;
        }
        ring.offer(scratch, position);
    }

    private void flag(final int mask) {
        scratch[AuditRecordLayout.FLAGS] |= (byte) mask;
    }

    private void putLabel(final ClientIdentity identity) {
        final ClientIdentity known = identity == null ? ClientIdentity.UNAUTHENTICATED : identity;
        final int length = Math.min(known.labelLength(), room());
        AuditRecordLayout.putInt(scratch, position, length);
        position += Integer.BYTES;
        if (length > 0) {
            known.copyLabelTo(scratch, position);
            position += length;
        }
    }

    private void putText(final CharSequence text) {
        final int length = AuditText.encode(text, scratch, position + Integer.BYTES, room());
        AuditRecordLayout.putInt(scratch, position, length);
        position += Integer.BYTES + length;
    }

    private void putSection(final byte[] src, final int offset, final int length) {
        AuditRecordLayout.putInt(scratch, position, length);
        position += Integer.BYTES;
        if (length > 0) {
            System.arraycopy(src, offset, scratch, position, length);
            position += length;
        }
    }

    /**
     * Copies the share of {@code field} the settings allow: all of it under a prefix that says so,
     * otherwise head and tail, bounded by the configured widths and by a share of its size.
     */
    private void putField(final ByteBuffer field, final AuditConfigSnapshot cfg,
                          final boolean full, final boolean text, final int sizeOffset,
                          final int headOffset, final int truncatedFlag, final int textFlag) {
        if (field == null) {
            AuditRecordLayout.putInt(scratch, sizeOffset, -1);
            putSection(null, 0, 0);
            return;
        }
        final int from = field.position();
        final int size = field.remaining();
        AuditRecordLayout.putInt(scratch, sizeOffset, size);
        final int visible = Math.min(cfg.visibleBytes(size, full), room());
        if (visible < size) {
            flag(truncatedFlag);
        }
        if (text) {
            flag(textFlag);
        }
        AuditRecordLayout.putInt(scratch, position, visible);
        position += Integer.BYTES;
        final int head = cfg.headOf(visible);
        final int tail = visible - head;
        AuditRecordLayout.putInt(scratch, headOffset, head);
        // Bulk, not byte by byte: under a full-prefix rule this is thousands of bytes. The
        // transfers are relative, so the position is walked and then put back where the digest
        // below needs it.
        if (head > 0) {
            field.position(from);
            field.get(scratch, position, head);
            position += head;
        }
        if (tail > 0) {
            field.position(from + size - tail);
            field.get(scratch, position, tail);
            position += tail;
        }
        field.position(from);
    }

    /**
     * Digests the whole field, salt first, when it is small enough to be worth the loop time.
     * Consumes {@code field}, so it runs after the copy above has read what it needs.
     */
    private void putDigest(final ByteBuffer field, final AuditConfigSnapshot cfg,
                           final int skippedFlag) {
        final AuditHashAlgorithm algorithm = cfg.hashAlgorithm();
        if (field == null || algorithm == AuditHashAlgorithm.NONE) {
            putSection(null, 0, 0);
            return;
        }
        if (field.remaining() > cfg.hashMaxBytes()) {
            flag(skippedFlag);
            putSection(null, 0, 0);
            return;
        }
        final MessageDigest md = digestFor(algorithm);
        if (md == null) {
            flag(skippedFlag);
            putSection(null, 0, 0);
            return;
        }
        md.reset();
        final byte[] salt = cfg.hashSalt();
        if (salt != null) {
            md.update(salt);
        }
        md.update(field);
        final int length;
        try {
            length = md.digest(digestOut, 0, digestOut.length);
        } catch (final DigestException e) {
            flag(skippedFlag);
            putSection(null, 0, 0);
            return;
        }
        putSection(digestOut, 0, Math.min(length, room()));
    }

    private MessageDigest digestFor(final AuditHashAlgorithm algorithm) {
        if (digest != null && digestAlgorithm == algorithm) {
            return digest;
        }
        try {
            digest = MessageDigest.getInstance(algorithm.jcaName());
            digestAlgorithm = algorithm;
        } catch (final NoSuchAlgorithmException e) {
            digest = null;
            digestAlgorithm = AuditHashAlgorithm.NONE;
        }
        return digest;
    }

    /** Bytes left in the scratch record, with room kept for the sections still to be written. */
    private int room() {
        return Math.max(0, scratch.length - position - OVERHEAD_BYTES);
    }
}
