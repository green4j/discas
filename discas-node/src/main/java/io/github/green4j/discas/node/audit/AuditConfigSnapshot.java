/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * One immutable reading of the audit settings: what is written, how much of it, and how it is
 * rendered.
 * <p>
 * Read from two threads. The event loop consults it to decide how many bytes of a key or a value
 * to copy into the ring and whether to digest them -- that decision cannot be deferred, because
 * what is not copied is gone by the time the drain sees the record. The drain thread consults the
 * same snapshot to render what it was handed.
 * <p>
 * {@link #sink()} and {@link #bufferBytes()} are read once, when the node is built; a reload that
 * changes either is refused, since the ring cannot be resized underneath a live producer.
 */
public final class AuditConfigSnapshot {

    /** The default sink: one line per event through the node's {@code Log}. */
    public static final String SINK_LOG = "log";

    private final String sink;
    private final int bufferBytes;
    private final AuditOverflow overflow;
    private final int headBytes;
    private final int tailBytes;
    private final double maxFraction;
    private final AuditHashAlgorithm hashAlgorithm;
    private final byte[] hashSalt;
    private final int hashSaltIdCode;
    private final int hashMaxBytes;
    private final int fullMaxBytes;
    // Encoded once, here: the match runs on the event loop, per request, per prefix.
    private final ByteBuffer[] fullPrefixes;
    private final ByteBuffer[] textPrefixes;

    public AuditConfigSnapshot(final String sink,
                               final int bufferBytes,
                               final AuditOverflow overflow,
                               final int headBytes,
                               final int tailBytes,
                               final double maxFraction,
                               final AuditHashAlgorithm hashAlgorithm,
                               final byte[] hashSalt,
                               final int hashSaltIdCode,
                               final int hashMaxBytes,
                               final int fullMaxBytes,
                               final List<String> fullPrefixes,
                               final List<String> textPrefixes) {
        this.sink = sink;
        this.bufferBytes = bufferBytes;
        this.overflow = overflow;
        this.headBytes = headBytes;
        this.tailBytes = tailBytes;
        this.maxFraction = maxFraction;
        this.hashAlgorithm = hashAlgorithm;
        this.hashSalt = hashSalt == null ? null : hashSalt.clone();
        this.hashSaltIdCode = hashSaltIdCode;
        this.hashMaxBytes = hashMaxBytes;
        this.fullMaxBytes = fullMaxBytes;
        this.fullPrefixes = encode(fullPrefixes);
        this.textPrefixes = encode(textPrefixes);
    }

    public String sink() {
        return sink;
    }

    /** Size of the ring, in bytes, taken out of the node's heap data budget. */
    public int bufferBytes() {
        return bufferBytes;
    }

    public AuditOverflow overflow() {
        return overflow;
    }

    public AuditHashAlgorithm hashAlgorithm() {
        return hashAlgorithm;
    }

    /** The salt prepended to the bytes before digesting, or {@code null}. Never printed. */
    public byte[] hashSalt() {
        return hashSalt;
    }

    /**
     * Public name of the salt: the first four bytes of the salt's own digest, {@code 0} when the
     * digests are unsalted. A line carries it so a reader knows which salt reproduces the digest
     * without the line saying what the salt is.
     */
    public int hashSaltIdCode() {
        return hashSaltIdCode;
    }

    /** {@link #hashSaltIdCode()} as the eight hex characters a line shows. */
    public static void appendSaltId(final StringBuilder out, final int code) {
        for (int shift = 28; shift >= 0; shift -= 4) {
            out.append(Character.forDigit((code >>> shift) & 0xF, 16));
        }
    }

    /** Above this many bytes nothing is digested, and the record says the digest was skipped. */
    public int hashMaxBytes() {
        return hashMaxBytes;
    }

    /** True when {@code key} falls under a prefix whose values are recorded whole. */
    public boolean rendersFull(final ByteBuffer key) {
        return matches(fullPrefixes, key);
    }

    /** True when {@code key} falls under a prefix whose bytes are rendered as text, not hex. */
    public boolean rendersText(final ByteBuffer key) {
        return matches(textPrefixes, key);
    }

    /**
     * How many bytes of a {@code size}-byte field to copy, and therefore to show.
     * <p>
     * Two limits at once: the head and tail widths, and a share of the whole. The share is what
     * keeps a short value from being quoted in full by a "preview" -- sixteen bytes of head on a
     * twenty-byte value is not a preview, it is the value.
     */
    public int visibleBytes(final int size, final boolean full) {
        if (size <= 0) {
            return 0;
        }
        if (full) {
            return Math.min(size, fullMaxBytes);
        }
        final int widths = headBytes + tailBytes;
        final int share = (int) Math.floor(size * maxFraction);
        return Math.max(0, Math.min(widths, share));
    }

    /** How the {@link #visibleBytes} split between the head and the tail of the field. */
    public int headOf(final int visible) {
        if (visible <= 0) {
            return 0;
        }
        if (tailBytes == 0) {
            return visible;
        }
        final int head = Math.min(headBytes, (visible + 1) / 2);
        return Math.max(1, head);
    }

    public String summary() {
        final StringBuilder sb = new StringBuilder(96);
        sb.append("sink=").append(sink)
                .append(", buffer=").append(bufferBytes).append(" bytes")
                .append(", overflow=").append(overflow.configName())
                .append(", hash=").append(hashAlgorithm.configName());
        if (hashSaltIdCode != 0) {
            sb.append("+salt/");
            appendSaltId(sb, hashSaltIdCode);
        }
        sb.append(", full prefixes=").append(fullPrefixes.length)
                .append(", text prefixes=").append(textPrefixes.length);
        return sb.toString();
    }

    private static boolean matches(final ByteBuffer[] prefixes, final ByteBuffer key) {
        if (key == null) {
            return false;
        }
        for (int i = 0; i < prefixes.length; i++) {
            if (ByteBuffers.startsWith(key, prefixes[i])) {
                return true;
            }
        }
        return false;
    }

    private static ByteBuffer[] encode(final List<String> prefixes) {
        if (prefixes == null) {
            return new ByteBuffer[0];
        }
        final ByteBuffer[] encoded = new ByteBuffer[prefixes.size()];
        for (int i = 0; i < encoded.length; i++) {
            encoded[i] = ByteBuffer.wrap(prefixes.get(i).getBytes(StandardCharsets.UTF_8))
                    .asReadOnlyBuffer();
        }
        return encoded;
    }
}
