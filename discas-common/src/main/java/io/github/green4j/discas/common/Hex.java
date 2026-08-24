/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import java.nio.ByteBuffer;

/**
 * Lowercase hexadecimal encoding, shared by everything that needs to render bytes as text:
 * the agent's {@code X-DisCas-Version} header, the file-watch signature digests, and anything
 * else that would otherwise carry its own copy of the same lookup table.
 */
public final class Hex {

    private static final char[] DIGITS = "0123456789abcdef".toCharArray();

    private Hex() {
    }

    /** Encode {@code bytes} as lowercase hex. */
    public static String encode(final byte[] bytes) {
        final char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            final int v = bytes[i] & 0xFF;
            out[i * 2] = DIGITS[v >>> 4];
            out[i * 2 + 1] = DIGITS[v & 0x0F];
        }
        return new String(out);
    }

    /** Encode a buffer's remaining bytes as lowercase hex, leaving its position untouched. */
    public static String encode(final ByteBuffer buffer) {
        final ByteBuffer view = buffer.duplicate();
        final char[] out = new char[view.remaining() * 2];
        for (int i = 0; view.hasRemaining(); i++) {
            final int v = view.get() & 0xFF;
            out[i * 2] = DIGITS[v >>> 4];
            out[i * 2 + 1] = DIGITS[v & 0x0F];
        }
        return new String(out);
    }

    /**
     * Decode lowercase or uppercase hex to bytes.
     *
     * @return the decoded bytes, or {@code null} if {@code s} is null, odd-length, or contains
     *         a non-hex character. Malformed input is an ordinary outcome here -- the text
     *         arrives from a client-supplied query parameter -- not a fault.
     */
    public static byte[] decode(final String s) {
        if (s == null || (s.length() & 1) != 0) {
            return null;
        }
        final byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            final int hi = Character.digit(s.charAt(i << 1), 16);
            final int lo = Character.digit(s.charAt((i << 1) + 1), 16);
            if (hi < 0 || lo < 0) {
                return null;
            }
            out[i] = (byte) ((hi << 4) | lo);
        }
        return out;
    }
}
