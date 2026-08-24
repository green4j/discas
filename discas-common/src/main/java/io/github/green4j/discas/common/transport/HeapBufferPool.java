/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Fixed-size heap ByteBuffer pool for transport RX buffers.
 * <p>
 * Thread safety: accessed only from the owning EventLoop thread.
 * Buffers are returned in write mode (cleared) by {@link #acquire()}.
 */
public final class HeapBufferPool {
    private final int bufferSize;
    private final int maxPooled;
    private final Deque<ByteBuffer> pool = new ArrayDeque<>();
    private boolean closed = false;

    public HeapBufferPool(final int bufferSize, final int maxPooled) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be > 0");
        }
        if (maxPooled <= 0) {
            throw new IllegalArgumentException("maxPooled must be > 0");
        }
        this.bufferSize = bufferSize;
        this.maxPooled = maxPooled;
    }

    public ByteBuffer acquire() {
        if (closed) {
            throw new IllegalStateException("HeapBufferPool is closed");
        }
        final ByteBuffer pooled = pool.pollFirst();
        if (pooled != null) {
            pooled.clear();
            return pooled;
        }
        return ByteBuffer.allocate(bufferSize);
    }

    public void release(final ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() != bufferSize || buffer.isDirect()) {
            return;
        }
        if (closed) {
            return;
        }
        if (pool.size() < maxPooled) {
            pool.addLast(buffer);
        }
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        pool.clear();
    }

    public int pooledCount() {
        return pool.size();
    }

    public int bufferSize() {
        return bufferSize;
    }
}
