/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

/**
 * UTF-8 to and from a caller's storage, allocating nothing: the record path encodes into the
 * scratch it already has, and the render path decodes into the builder it already has.
 */
final class AuditText {

    private static final char REPLACEMENT = '\uFFFD';

    private AuditText() {
    }

    /**
     * Encodes {@code text} into {@code dst}, stopping at {@code max} bytes rather than overrunning.
     *
     * @return bytes written
     */
    static int encode(final CharSequence text, final byte[] dst, final int offset, final int max) {
        int at = offset;
        final int end = offset + max;
        for (int i = 0; i < text.length(); i++) {
            int cp = text.charAt(i);
            if (Character.isHighSurrogate((char) cp) && i + 1 < text.length()
                    && Character.isLowSurrogate(text.charAt(i + 1))) {
                cp = Character.toCodePoint((char) cp, text.charAt(++i));
            }
            final int width = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
            if (at + width > end) {
                break;
            }
            if (width == 1) {
                dst[at++] = (byte) cp;
            } else if (width == 2) {
                dst[at++] = (byte) (0xC0 | (cp >> 6));
                dst[at++] = (byte) (0x80 | (cp & 0x3F));
            } else if (width == 3) {
                dst[at++] = (byte) (0xE0 | (cp >> 12));
                dst[at++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
                dst[at++] = (byte) (0x80 | (cp & 0x3F));
            } else {
                dst[at++] = (byte) (0xF0 | (cp >> 18));
                dst[at++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
                dst[at++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
                dst[at++] = (byte) (0x80 | (cp & 0x3F));
            }
        }
        return at - offset;
    }

    /**
     * Decodes {@code length} bytes into {@code out}. A sequence cut short -- which a record's
     * head-and-tail truncation can leave at either edge -- decodes to the replacement character.
     */
    static void decode(final byte[] src, final int offset, final int length,
                       final StringBuilder out) {
        int at = offset;
        final int end = offset + length;
        while (at < end) {
            final int b = src[at++] & 0xFF;
            if (b < 0x80) {
                out.append((char) b);
                continue;
            }
            final int width = (b & 0xE0) == 0xC0 ? 2 : (b & 0xF0) == 0xE0 ? 3
                    : (b & 0xF8) == 0xF0 ? 4 : 0;
            if (width == 0 || at + width - 1 > end) {
                out.append(REPLACEMENT);
                at = width == 0 ? at : end;
                continue;
            }
            int cp = b & (0xFF >> (width + 1));
            for (int i = 1; i < width; i++) {
                final int next = src[at++] & 0xFF;
                if ((next & 0xC0) != 0x80) {
                    out.append(REPLACEMENT);
                    cp = -1;
                    break;
                }
                cp = (cp << 6) | (next & 0x3F);
            }
            if (cp >= 0) {
                out.appendCodePoint(cp);
            }
        }
    }
}
