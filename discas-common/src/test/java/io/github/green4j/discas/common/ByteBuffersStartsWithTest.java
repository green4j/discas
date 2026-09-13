/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ByteBuffers.startsWith -- the prefix test the scan and the ACL run per key")
class ByteBuffersStartsWithTest {

    private static ByteBuffer utf8(final String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();
    }

    @Test
    void matchesWhatItShould() {
        assertTrue(ByteBuffers.startsWith(utf8("app/user/1"), utf8("app/")));
        assertTrue(ByteBuffers.startsWith(utf8("app/"), utf8("app/")));
        assertTrue(ByteBuffers.startsWith(utf8("app/user/1"), utf8("")));
        assertTrue(ByteBuffers.startsWith(utf8(""), utf8("")));
    }

    @Test
    void refusesWhatItShould() {
        assertFalse(ByteBuffers.startsWith(utf8("app/user/1"), utf8("lock/")));
        // A difference in the last byte of the prefix, which a length check alone would miss.
        assertFalse(ByteBuffers.startsWith(utf8("app-user/1"), utf8("app/")));
        assertFalse(ByteBuffers.startsWith(utf8("app"), utf8("app/")));
        assertFalse(ByteBuffers.startsWith(utf8(""), utf8("a")));
    }

    @Test
    @DisplayName("Both buffers are read from their positions, and neither is disturbed")
    void respectsPositionsWithoutMovingThem() {
        final ByteBuffer key = utf8("__app/user/1");
        key.position(2);
        final ByteBuffer prefix = utf8("##app/");
        prefix.position(2);

        assertTrue(ByteBuffers.startsWith(key, prefix));
        assertEquals(2, key.position());
        assertEquals(2, prefix.position());
        // Still usable for the next key in the page, which is what the scan relies on.
        assertTrue(ByteBuffers.startsWith(utf8("app/user/2"), prefix));
    }

    @Test
    @DisplayName("Long keys take the vectorised path and still compare byte for byte")
    void longKeys() {
        final String prefix = "a".repeat(64);
        assertTrue(ByteBuffers.startsWith(utf8(prefix + "tail"), utf8(prefix)));
        assertFalse(ByteBuffers.startsWith(utf8("a".repeat(63) + "b" + "tail"), utf8(prefix)));
    }
}
