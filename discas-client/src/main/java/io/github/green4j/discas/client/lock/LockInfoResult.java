/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * The outcome of a lock-info read: a status and, when there is one, the lock it describes.
 * <p>
 * Named factories rather than a public constructor, the same shape as its siblings
 * {@link LockAcquireResult} and {@link io.github.green4j.discas.client.WatchResult}.
 */
public final class LockInfoResult {
    private final LockInfoStatus status;
    private final LockInfo info;

    private LockInfoResult(final LockInfoStatus status, final LockInfo info) {
        this.status = status;
        this.info = info;
    }

    /** No lock is held on the key -- it is absent, tombstoned, or holds a release marker. */
    public static LockInfoResult unlocked() {
        return new LockInfoResult(LockInfoStatus.UNLOCKED, null);
    }

    /** The key holds a live lock. */
    public static LockInfoResult locked(final LockInfo info) {
        return new LockInfoResult(LockInfoStatus.LOCKED, info);
    }

    /** The key holds a lock whose lease has run out. */
    public static LockInfoResult expired(final LockInfo info) {
        return new LockInfoResult(LockInfoStatus.EXPIRED, info);
    }

    /** The key holds a value that is not a lock record -- an ordinary outcome on a shared key. */
    public static LockInfoResult notLockRecord() {
        return new LockInfoResult(LockInfoStatus.NOT_LOCK_RECORD, null);
    }

    /** What was found on the key. */
    public LockInfoStatus status() {
        return status;
    }

    /**
     * The lock record found, or {@code null} for {@link LockInfoStatus#UNLOCKED} and
     * {@link LockInfoStatus#NOT_LOCK_RECORD}. Present for {@link LockInfoStatus#EXPIRED}, which
     * is what lets a would-be successor see whom it is taking over from.
     */
    public LockInfo info() {
        return info;
    }
}
