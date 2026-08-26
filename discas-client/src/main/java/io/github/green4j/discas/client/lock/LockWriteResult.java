/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * The outcome of a release or a renew: a {@link LockWriteStatus} and, when the round saw one, the
 * lock record that was actually on the key.
 * <p>
 * Carrying the record is what separates this from a boolean: a caller that was displaced learns
 * who displaced it from the same round trip that refused the write, exactly as a lost
 * {@link io.github.green4j.discas.client.CasResult} carries the value that won.
 * <p>
 * Named factories rather than a public constructor, the same shape as its siblings
 * {@link LockAcquireResult} and {@link LockInfoResult}.
 */
public final class LockWriteResult {
    private final LockWriteStatus status;
    private final LockInfo observed;

    private LockWriteResult(final LockWriteStatus status, final LockInfo observed) {
        this.status = status;
        this.observed = observed;
    }

    /** The write committed. */
    public static LockWriteResult ok() {
        return new LockWriteResult(LockWriteStatus.APPLIED, null);
    }

    /** The key holds no lock of the caller's -- absent, tombstoned, or somebody else's marker. */
    public static LockWriteResult notHeld() {
        return new LockWriteResult(LockWriteStatus.NOT_HELD, null);
    }

    /**
     * This caller's own release had already landed; {@code observed} is the marker it wrote, so
     * the generation the lock ran under is still readable from it.
     */
    public static LockWriteResult alreadyReleased(final LockInfo observed) {
        return new LockWriteResult(LockWriteStatus.ALREADY_RELEASED, observed);
    }

    /** Someone else holds the key; {@code observed} is their record. */
    public static LockWriteResult heldByOther(final LockInfo observed) {
        return new LockWriteResult(LockWriteStatus.HELD_BY_OTHER, observed);
    }

    /** The key holds a value that is not a lock record. */
    public static LockWriteResult notLockRecord() {
        return new LockWriteResult(LockWriteStatus.NOT_LOCK_RECORD, null);
    }

    /** The caller's own record is there but its lease has run out; {@code observed} is it. */
    public static LockWriteResult expired(final LockInfo observed) {
        return new LockWriteResult(LockWriteStatus.EXPIRED, observed);
    }

    /**
     * The fenced write lost the compare. {@code observed} is whatever the round found instead,
     * or {@code null} if that was not a lock record.
     */
    public static LockWriteResult contended(final LockInfo observed) {
        return new LockWriteResult(LockWriteStatus.CONTENDED, observed);
    }

    /** Why the write did or did not apply. */
    public LockWriteStatus status() {
        return status;
    }

    /**
     * Shorthand for {@code status() == APPLIED} -- the only case in which anything was written.
     * A predicate beside a status is the shape {@link LockAcquireResult#acquired()} and
     * {@link io.github.green4j.discas.client.ScanPage#quorumReached()} already have: it drops
     * nothing, because the status is on the same object, and it saves every caller that only
     * wants "did it work" from spelling out a comparison.
     */
    public boolean applied() {
        return status == LockWriteStatus.APPLIED;
    }

    /**
     * The lock record the round found on the key, or {@code null} when there was none to report
     * ({@link LockWriteStatus#APPLIED}, {@link LockWriteStatus#NOT_HELD},
     * {@link LockWriteStatus#NOT_LOCK_RECORD}, and a {@link LockWriteStatus#CONTENDED} whose
     * winner is not a lock record). On {@link LockWriteStatus#ALREADY_RELEASED} it is a release
     * marker: no lease left to read, but the owner and the generation still say what was let go.
     */
    public LockInfo observed() {
        return observed;
    }
}
