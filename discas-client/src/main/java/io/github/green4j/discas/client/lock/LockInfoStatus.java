/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * What a lock-info read found on the key. {@link #EXPIRED} is kept distinct from
 * {@link #UNLOCKED} because the record -- and with it the owner and generation -- is still
 * there to be inspected, which is what makes a takeover auditable.
 */
public enum LockInfoStatus {
    /** No lock on the key: absent, tombstoned, or holding a release marker. */
    UNLOCKED,
    /** A lock record whose lease has not run out. */
    LOCKED,
    /** A lock record whose lease end is in the past; it can be taken over. */
    EXPIRED,
    /** The key holds a value that is not a lock record at all. */
    NOT_LOCK_RECORD
}
