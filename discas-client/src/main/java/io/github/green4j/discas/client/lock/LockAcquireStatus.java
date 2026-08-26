/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * Why an acquire attempt ended, or what a recovery found. Only {@link #ACQUIRED} yields a usable
 * {@link Lock}; the rest are ordinary outcomes on a contended or shared key, not errors -- a
 * failure to reach the cluster at all surfaces as a failed future instead.
 */
public enum LockAcquireStatus {
    /**
     * The lock is held by this client: either just taken, or -- from
     * {@code recoverLock} -- found already standing in the caller's name and handed back.
     */
    ACQUIRED,
    /** Someone else holds a live lease. Nothing was written. */
    HELD_BY_OTHER,
    /**
     * A live lease is already recorded under the owner id this acquire was made with, so nothing
     * was written and nothing was handed back.
     * <p>
     * Normally this is an acquire whose outcome was never reported -- it landed, and the answer
     * was lost -- in which case {@code recoverLock} turns the record back into a usable
     * {@link Lock}. Retrying the acquire instead would only wait out a lease the caller already
     * owns, which is why waiting forms stop here rather than spending their budget.
     * <p>
     * It can also mean two holders were given the same owner id, and then it is a bug in the
     * caller rather than a recovery: see the owner id contract on the acquire methods.
     */
    HELD_BY_SELF,
    /**
     * The key already holds a value that is not a lock record. Acquiring would overwrite
     * unrelated data, so the attempt stops instead.
     */
    NOT_LOCK_RECORD,
    /** The caller's wait budget ran out while the lock stayed held by someone else. */
    TIMED_OUT,
    /**
     * Only from {@code recoverLock}: nobody holds the key, so there was nothing of the caller's to
     * recover. The acquire being recovered did not land, and may be issued again.
     */
    NOT_HELD
}
