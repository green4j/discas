/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

import java.nio.ByteBuffer;

/**
 * The random per-acquire mark that distinguishes one holder of a lock from the next.
 * <p>
 * Release and renew are CASes conditioned on this token, so it -- not the owner id -- is what
 * separates the current holder from a previous one: two successive holders of the same key get
 * different tokens even under the same name, and the displaced one's operations fail against the
 * record its successor wrote.
 * <p>
 * That is the whole of what it protects against. It is not a secret: the token sits in the clear
 * inside the lock record, so whoever may read the key may read it. Keeping the key out of the
 * wrong hands is the ACL's job, not the token's. DisCas does not go out of its way to spread one
 * either -- a reading of a lock never carries the holder's token, and a holder that lost its own
 * asks for it back by name through {@code recoverLock}.
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
