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

    /** No lock is held on the key and nothing says one ever was: absent or tombstoned. */
    public static LockInfoResult unlocked() {
        return new LockInfoResult(LockInfoStatus.UNLOCKED, null);
    }

    /**
     * No lock is held, and the release marker left behind still names the holder that let it go;
     * {@code info} is that marker, so its lease reads as zero and long expired.
     */
    public static LockInfoResult released(final LockInfo info) {
        return new LockInfoResult(LockInfoStatus.UNLOCKED, info);
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
     * The lock record found, or {@code null} for {@link LockInfoStatus#NOT_LOCK_RECORD} and for an
     * {@link LockInfoStatus#UNLOCKED} key that holds nothing at all. Present for
     * {@link LockInfoStatus#EXPIRED}, which is what lets a would-be successor see whom it is
     * taking over from, and for an {@code UNLOCKED} key holding a release marker, where it names
     * the holder that released it. In both of those the key is free; only {@code LOCKED} is not.
     */
    public LockInfo info() {
        return info;
    }
}
