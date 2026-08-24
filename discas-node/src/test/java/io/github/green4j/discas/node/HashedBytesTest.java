/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.KeyHash;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("HashedBytes -- storage bytes, and the zero-copy bounds that compare against them")
class HashedBytesTest {

    private static ByteBuffer utf8(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A buffer whose content starts partway in, which is what a cursor sliced out of a frame
     * looks like. Every read here is an absolute {@code get(i)}, so a factory that duplicated
     * instead of sliced would silently read the padding.
     */
    private static ByteBuffer offsetView(final String padding, final String value) {
        final ByteBuffer buf = utf8(padding + value);
        buf.position(padding.length());
        return buf;
    }

    @Test
    @DisplayName("viewOf reads from the source's position, not from index zero")
    void viewOfHonoursPosition() {
        final ByteBuffer source = offsetView("XXXX", "key");

        final HashedBytes view = HashedBytes.viewOf(source);

        assertEquals(3, view.size());
        assertEquals(new HashedBytes(utf8("key")), view);
        assertEquals(0, view.compareTo(utf8("key")));
        assertTrue(view.startsWith(utf8("ke")));
        assertFalse(view.startsWith(utf8("XX")));
    }

    @Test
    @DisplayName("viewOf leaves the source buffer's position alone")
    void viewOfDoesNotConsumeTheSource() {
        final ByteBuffer source = offsetView("XXXX", "key");

        final HashedBytes view = HashedBytes.viewOf(source);
        view.startsWith(utf8("k"));
        view.hashCode();
        view.toBuffer();

        assertEquals(4, source.position());
        assertEquals(3, source.remaining());
    }

    @Test
    @DisplayName("Adopt matches the copying constructor, including equality and hash")
    void adoptMatchesTheCopyingConstructor() {
        final ByteBuffer source = offsetView("pad", "value");

        final HashedBytes adopted = HashedBytes.adopt(source);
        final HashedBytes copied = new HashedBytes(utf8("value"));

        assertEquals(copied, adopted);
        assertEquals(copied.hashCode(), adopted.hashCode());
        assertEquals(0, adopted.compareTo(copied));
        assertNull(HashedBytes.adopt(null));
    }

    @Test
    @DisplayName("startsWith: empty matches everything, longer than the key matches nothing")
    void startsWithEdges() {
        final HashedBytes key = new HashedBytes(utf8("app/1"));

        assertTrue(key.startsWith(utf8("")));
        assertTrue(key.startsWith(utf8("app/1")));
        assertFalse(key.startsWith(utf8("app/12")));
        assertFalse(key.startsWith(utf8("b")));
        assertTrue(HashedBytes.EMPTY.startsWith(utf8("")));
        assertFalse(HashedBytes.EMPTY.startsWith(utf8("a")));
    }

    @Test
    @DisplayName("startsWith moves neither buffer's position, so a prefix can be reused per key")
    void startsWithIsPositionNeutral() {
        final ByteBuffer prefix = offsetView("pad", "app/");
        final HashedBytes first = new HashedBytes(utf8("app/1"));
        final HashedBytes second = new HashedBytes(utf8("app/2"));

        assertTrue(first.startsWith(prefix));
        assertTrue(second.startsWith(prefix), "A consumed prefix would match vacuously here");
        assertEquals(3, prefix.position());
        assertEquals(4, prefix.remaining());
    }

    @Test
    @DisplayName("compareTo(ByteBuffer) orders the same way as compareTo(HashedBytes)")
    void compareToBufferAgreesWithCompareToHashedBytes() {
        final HashedBytes b = new HashedBytes(utf8("b"));

        assertTrue(b.compareTo(utf8("a")) > 0);
        assertTrue(b.compareTo(utf8("c")) < 0);
        assertEquals(0, b.compareTo(utf8("b")));
        assertEquals(
                Integer.signum(b.compareTo(new HashedBytes(utf8("c")))),
                Integer.signum(b.compareTo(utf8("c"))));
    }

    @Test
    @DisplayName("rangeOf agrees with KeyHash -- client routing and node partitioning share one hash")
    void rangeOfAgreesWithKeyHash() {
        for (int i = 0; i < 64; i++) {
            final String key = "key-" + i;
            assertEquals(KeyHash.rangeOf(utf8(key), LocalStore.NUM_RANGES),
                    new HashedBytes(utf8(key)).rangeOf(LocalStore.NUM_RANGES),
                    "Range disagreement would mis-route reads for " + key);
        }
        // And through a view whose source is positioned partway in, which is how a cursor arrives.
        assertEquals(KeyHash.rangeOf(utf8("key-7"), LocalStore.NUM_RANGES),
                HashedBytes.viewOf(offsetView("pad", "key-7")).rangeOf(LocalStore.NUM_RANGES));
    }

    @Test
    @DisplayName("The copying constructor does not alias the source")
    void constructorCopies() {
        final byte[] backing = "value".getBytes(StandardCharsets.UTF_8);
        final HashedBytes copied = new HashedBytes(ByteBuffer.wrap(backing));
        final int hashBefore = copied.hashCode();

        backing[0] = 'V';

        assertEquals(hashBefore, copied.hashCode());
        assertEquals(new HashedBytes(utf8("value")), copied);
        assertNotEquals(new HashedBytes(utf8("Value")), copied);
    }
}
