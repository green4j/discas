/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

import java.nio.ByteBuffer;

/**
 * The random per-acquire secret that identifies one holder of a lock.
 * <p>
 * Release and renew are CASes conditioned on this token, so it -- not the client id -- is what
 * separates the current holder from a previous one: two successive holders of the same key from
 * the same client still get different tokens, and the displaced holder's operations fail.
 * <p>
 * Value type with by-content {@code equals}. The bytes are copied on construction and exposed
 * only as read-only duplicates, so a caller cannot mutate a token after handing it over.
 */
public final class LockToken {
    private final ByteBuffer bytes;

    /** Copies {@code bytes} from its current position; the argument's position is not consumed. */
    public LockToken(final ByteBuffer bytes) {
        final ByteBuffer src = bytes.duplicate();
        final ByteBuffer copy = ByteBuffer.allocate(src.remaining());
        if (copy.capacity() > 0) {
            copy.put(src);
            copy.flip();
        }
        this.bytes = copy.asReadOnlyBuffer();
    }

    public LockToken(final byte[] bytes) {
        this(ByteBuffer.wrap(bytes));
    }

    /** A read-only duplicate of the token bytes, positioned at the start. */
    public ByteBuffer bytes() {
        return bytes.duplicate();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LockToken)) {
            return false;
        }
        return bytes.equals(((LockToken) o).bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }
}
