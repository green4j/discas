/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * Why an acquire attempt ended. Only {@link #ACQUIRED} yields a usable {@link Lock}; the other
 * three are ordinary outcomes on a contended or shared key, not errors -- a failure to reach the
 * cluster at all surfaces as a failed future instead.
 */
public enum LockAcquireStatus {
    /** The lock is now held by this client. */
    ACQUIRED,
    /** Someone else holds a live lease. Nothing was written. */
    HELD_BY_OTHER,
    /**
     * The key already holds a value that is not a lock record. Acquiring would overwrite
     * unrelated data, so the attempt stops instead.
     */
    NOT_LOCK_RECORD,
    /** The caller's wait budget ran out while the lock stayed held by someone else. */
    TIMED_OUT
}
