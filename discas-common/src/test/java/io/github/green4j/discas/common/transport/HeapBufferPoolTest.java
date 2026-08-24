/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("HeapBufferPool -- acquire/release semantics")
class HeapBufferPoolTest {

    @Test
    void releasedBufferIsReused() {
        final HeapBufferPool pool = new HeapBufferPool(1024, 4);
        final ByteBuffer first = pool.acquire();
        assertNotNull(first);
        assertFalse(first.isDirect());
        assertEquals(1024, first.capacity());
        assertEquals(1024, first.remaining());
        assertEquals(0, pool.pooledCount());

        pool.release(first);
        assertEquals(1, pool.pooledCount());

        final ByteBuffer second = pool.acquire();
        assertTrue(first == second);
    }

    @Test
    void constructorRejectsInvalidArgs() {
        assertThrows(IllegalArgumentException.class, () -> new HeapBufferPool(0, 4));
        assertThrows(IllegalArgumentException.class, () -> new HeapBufferPool(64, 0));
    }
}
