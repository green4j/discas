/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * The outcome of an acquire attempt: a {@link LockAcquireStatus} and, where there is one, the
 * lock that was taken or the {@link LockInfo} describing who holds it instead.
 * <p>
 * Contention is reported here rather than thrown, because losing a race for a lock is a normal
 * result; only an inability to reach the cluster fails the future. Named factories rather than a
 * public constructor, matching {@link LockInfoResult}.
 */
public final class LockAcquireResult {
    private final LockAcquireStatus status;
    private final DistributedLock lock;
    private final LockInfo lockInfo;

    private LockAcquireResult(final LockAcquireStatus status,
                              final DistributedLock lock,
                              final LockInfo lockInfo) {
        this.status = status;
        this.lock = lock;
        this.lockInfo = lockInfo;
    }

    /** The lock was taken; {@code lockInfo} is its acquire-time snapshot. */
    public static LockAcquireResult acquired(final DistributedLock lock) {
        return new LockAcquireResult(LockAcquireStatus.ACQUIRED, lock, lock.lockInfo().snapshot());
    }

    /** Another holder has a live lease; {@code lockInfo} says who and until when. */
    public static LockAcquireResult heldByOther(final LockInfo lockInfo) {
        return new LockAcquireResult(LockAcquireStatus.HELD_BY_OTHER, null, lockInfo);
    }

    /** The key holds a non-lock value, so nothing was written and there is nothing to describe. */
    public static LockAcquireResult notLockRecord() {
        return new LockAcquireResult(LockAcquireStatus.NOT_LOCK_RECORD, null, null);
    }

    /** The wait budget ran out; {@code lockInfo} is the last holder seen while waiting. */
    public static LockAcquireResult timedOut(final LockInfo lockInfo) {
        return new LockAcquireResult(LockAcquireStatus.TIMED_OUT, null, lockInfo);
    }

    /** Why the attempt ended. */
    public LockAcquireStatus status() {
        return status;
    }

    /** Shorthand for {@code status() == ACQUIRED} -- the only case with a usable lock. */
    public boolean acquired() {
        return status == LockAcquireStatus.ACQUIRED;
    }

    /** The acquired lock, or {@code null} unless {@link #acquired()}. */
    public DistributedLock lock() {
        return lock;
    }

    /** The acquired lock's holder token, or {@code null} unless {@link #acquired()}. */
    public LockToken token() {
        return lock != null ? lock.token() : null;
    }

    /**
     * On success the acquire-time snapshot of the lock just taken; on
     * {@link LockAcquireStatus#HELD_BY_OTHER} or {@link LockAcquireStatus#TIMED_OUT} the state of
     * the holder that blocked it; {@code null} for {@link LockAcquireStatus#NOT_LOCK_RECORD}.
     */
    public LockInfo lockInfo() {
        return lockInfo;
    }
}
