/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * The outcome of an acquire attempt: a {@link LockAcquireStatus} and exactly one of the two things
 * an attempt can end with -- the lock that was taken, or the record that stood in the way.
 * <p>
 * Never both. On success the lock is the whole answer and anything it already tells you is not
 * repeated here; on a refusal there is no lock and {@link #observed()} says what was found instead.
 * <p>
 * Contention is reported here rather than thrown, because losing a race for a lock is a normal
 * result; only an inability to reach the cluster fails the future. Named factories rather than a
 * public constructor, matching {@link LockInfoResult}.
 */
public final class LockAcquireResult {
    private final LockAcquireStatus status;
    private final Lock lock;
    private final LockInfo observed;

    private LockAcquireResult(final LockAcquireStatus status,
                              final Lock lock,
                              final LockInfo observed) {
        this.status = status;
        this.lock = lock;
        this.observed = observed;
    }

    /** The lock was taken. */
    public static LockAcquireResult acquired(final Lock lock) {
        return new LockAcquireResult(LockAcquireStatus.ACQUIRED, lock, null);
    }

    /** Another holder has a live lease; {@code observed} says who and until when. */
    public static LockAcquireResult heldByOther(final LockInfo observed) {
        return new LockAcquireResult(LockAcquireStatus.HELD_BY_OTHER, null, observed);
    }

    /**
     * A live lease already stands in the caller's own name. Deliberately carries no {@link Lock}:
     * an acquire that finds the key already its own has taken nothing, and handing one back here
     * would silently grant mutual exclusion to a second caller that merely shares an owner id.
     * {@code recoverLock} is the operation that turns this record into a usable lock, and asking
     * for it is the caller stating that the id really is theirs.
     */
    public static LockAcquireResult heldBySelf(final LockInfo observed) {
        return new LockAcquireResult(LockAcquireStatus.HELD_BY_SELF, null, observed);
    }

    /** Nothing holds the key, so a recovery found nothing of the caller's to hand back. */
    public static LockAcquireResult notHeld() {
        return new LockAcquireResult(LockAcquireStatus.NOT_HELD, null, null);
    }

    /** The key holds a non-lock value, so nothing was written and there is nothing to describe. */
    public static LockAcquireResult notLockRecord() {
        return new LockAcquireResult(LockAcquireStatus.NOT_LOCK_RECORD, null, null);
    }

    /** The wait budget ran out; {@code observed} is the last holder seen while waiting. */
    public static LockAcquireResult timedOut(final LockInfo observed) {
        return new LockAcquireResult(LockAcquireStatus.TIMED_OUT, null, observed);
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
    public Lock lock() {
        return lock;
    }

    /**
     * The record that stood in the way, and {@code null} when none did -- on
     * {@link LockAcquireStatus#ACQUIRED}, where the lock itself is the answer, and on
     * {@link LockAcquireStatus#NOT_HELD} and {@link LockAcquireStatus#NOT_LOCK_RECORD}, where
     * there was no lock record to report.
     * <p>
     * Named as in {@link LockWriteResult#observed()}, and for the same reason: a refused operation
     * says what it saw, so the caller learns who it lost to without a second round trip.
     */
    public LockInfo observed() {
        return observed;
    }
}
