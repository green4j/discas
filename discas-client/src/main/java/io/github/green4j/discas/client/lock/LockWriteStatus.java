/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * Why a write conditioned on the holder's {@link LockToken} -- a release or a renew -- did or did
 * not apply. Only {@link #APPLIED} changed anything; the rest are ordinary outcomes on a contended
 * or shared key, not errors, and a failure to reach the cluster surfaces as a failed future
 * instead.
 * <p>
 * The distinctions exist because the caller acts differently on each: a holder that is
 * {@link #EXPIRED} must re-acquire, one that is {@link #CONTENDED} may simply try again, and one
 * that is {@link #HELD_BY_OTHER} has already been displaced and must not.
 */
public enum LockWriteStatus {
    /** The write committed. */
    APPLIED,
    /** The key holds no lock: it is absent, tombstoned, or holds a release marker. */
    NOT_HELD,
    /** A live lock record under a different token: this caller has been displaced. */
    HELD_BY_OTHER,
    /** The key holds a value that is not a lock record at all. */
    NOT_LOCK_RECORD,
    /**
     * The caller's own record is still there, but its lease has run out, so any waiter was already
     * entitled to take over. Renewing from here would resurrect a lock the holder no longer owns;
     * a release still applies, because writing the marker is the right cleanup either way.
     */
    EXPIRED,
    /**
     * The key was the caller's when read and had moved on by the time the fenced write landed.
     * Nothing was written and nothing is known to be lost -- re-read and decide again.
     */
    CONTENDED
}
