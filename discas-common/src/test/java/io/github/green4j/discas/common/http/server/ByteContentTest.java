/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.ByteArrayContent;
import io.github.green4j.discas.common.http.server.HttpServer.ByteContent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the public {@link HttpServer.ByteContent} contract and the built-in
 * {@link HttpServer.ByteArrayContent} constant: zero-copy relative writes of a {@code byte[]} into a
 * bounded output buffer, exactly as the response body driver drives them. No mocks. (The private
 * pooled wrappers behind {@code setBody}/{@code addChunk} are covered end-to-end by the server's
 * own tests, as is the driver's behaviour when a content under-delivers its declared length.)
 *
 * <p>Contract under test: {@code write(sourceOffset, target)} copies bytes with <em>relative</em> puts
 * (advancing {@code target.position()}), returns the count written ({@code >= 0}), and returns {@code 0}
 * only when not even one unit fits in the remaining room. There is no negative end sentinel: the caller
 * counts to {@link ByteContent#length()} and stops.
 */
@DisplayName("Public ByteContent contract + ByteArrayContent constant - relative, zero-copy writes")
class ByteContentTest {

    /**
     * Emulate the driver: window {@code out} to {@code cap} bytes of room, call {@code write} (which
     * appends with a relative put, advancing the position), then restore the limit. Asserts the write
     * advanced the target position by exactly the returned count.
     */
    private static int pump(final ByteContent c, final int sourceOffset, final ByteBuffer out, final int cap) {
        final int pos = out.position();
        final int savedLimit = out.limit();
        out.limit(Math.min(savedLimit, pos + cap));
        final int n = c.write(sourceOffset, out);
        out.limit(savedLimit);
        assertEquals(pos + n, out.position(), "Relative write must advance target position by n");
        return n;
    }

    /** Drive {@code c} to completion into a fresh target sized to its length, returning the bytes. */
    private static byte[] renderAll(final ByteContent c, final int cap) {
        final ByteBuffer out = ByteBuffer.allocate((int) c.length());
        int off = 0;
        while (off < c.length()) {
            final int n = pump(c, off, out, cap);
            assertTrue(n > 0, "Target sized to full length -> every call makes progress");
            off += n;
        }
        out.flip();
        final byte[] got = new byte[out.remaining()];
        out.get(got);
        return got;
    }

    @Test
    @DisplayName("ByteArrayContent copies its range across bounded targets; 0 when the target is full")
    void byteArrayAcrossCalls() {
        final byte[] data = "abcdefgh".getBytes(StandardCharsets.US_ASCII);
        final ByteArrayContent c = new ByteArrayContent(data);
        assertEquals(8, c.length());
        // Rendered in one pass through a roomy target, the bytes come back unchanged.
        assertArrayEquals(data, renderAll(c, 16));

        final ByteBuffer out = ByteBuffer.allocate(3);
        assertEquals(3, pump(c, 0, out, 3));       // capped by target room
        assertEquals(0, pump(c, 3, out, 3));       // target full -> 0 written (data not yet exhausted)
        out.clear();
        assertEquals(3, pump(c, 3, out, 3));
        out.clear();
        assertEquals(2, pump(c, 6, out, 3));       // only 2 bytes left before length()
    }
}
