/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * Local view of a held lock's state. Exposes both the immutable
 * acquire-time snapshot and the most recent locally-known lease.
 * <p>
 * After a successful {@link DistributedLock#renew(java.time.Duration)},
 * {@link #current()} reflects the updated lease end; {@link #snapshot()}
 * still returns the original acquire-time {@link LockInfo}. If
 * {@code renew()} has never been called, {@code current()} equals
 * {@code snapshot()}.
 * <p>
 * Neither value is a fresh remote read -- for that, use
 * {@link DistributedLock#info()}.
 */
public final class LockInfoView {

    private final LockInfo snapshot;
    private final LockInfo current;

    public LockInfoView(final LockInfo snapshot, final LockInfo current) {
        this.snapshot = snapshot;
        this.current = current;
    }

    /** The lock exactly as it was at acquire time; never changes for the life of the lock. */
    public LockInfo snapshot() {
        return snapshot;
    }

    /** The latest lease this client knows of, advanced by each successful renew. */
    public LockInfo current() {
        return current;
    }
}
