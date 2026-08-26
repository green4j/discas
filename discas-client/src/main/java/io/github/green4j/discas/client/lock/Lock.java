/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A lock this client currently believes it holds, handed out by a successful acquire.
 * <p>
 * Nothing here is a mutual-exclusion guarantee on its own. The lock is a lease: it is held for a
 * bounded TTL, and a holder that stalls past its lease -- a long GC pause, a VM freeze, a partition
 * -- can be displaced by a successor while its own {@code Lock} object still looks valid.
 * That is inherent to lease-based locking, not a gap in this implementation, and it is why
 * {@link #fencingToken()} exists: guard the work the lock protects with the token rather than
 * with the fact that you hold this object.
 * <p>
 * Every remote operation is a CAS on the lock key conditioned on the holder's
 * {@link LockToken}, so a displaced holder's release or renew fails rather than stomping the
 * successor.
 */
public interface Lock {
    /**
     * Releases the lock, writing a release marker under the holder's token. Anything but
     * {@link LockWriteStatus#APPLIED} means the CAS did not apply -- normally because this holder
     * was already displaced, in which case the lock now belongs to someone else and must not be
     * touched, and {@link LockWriteResult#observed()} says to whom.
     * <p>
     * The one exception is {@link LockWriteStatus#ALREADY_RELEASED}, which also wrote nothing but
     * means the opposite: an earlier attempt of this same release had landed. Retrying a release
     * whose answer was lost is therefore safe and tells you so.
     */
    CompletableFuture<LockWriteResult> release();

    /**
     * Extends the lease by {@code newLeaseTtl} measured from now, via a CAS conditioned on the
     * holder's token. Anything but {@link LockWriteStatus#APPLIED} means the lease was lost;
     * renewing is not retryable in that state, and the status says whether to re-acquire
     * ({@link LockWriteStatus#EXPIRED}) or to stop ({@link LockWriteStatus#HELD_BY_OTHER}).
     */
    CompletableFuture<LockWriteResult> renew(Duration newLeaseTtl);

    /**
     * Reads the lock key's current state from the cluster. Unlike {@link #lockInfo()} this is a
     * fresh remote read, so it is the only way to learn about a change another client made.
     */
    CompletableFuture<LockInfoResult> info();

    /** The holder token every release/renew CAS is conditioned on. */
    LockToken token();

    /**
     * The name this lock stands under in the record. Worth asking of a lock that came from
     * {@code recoverLock} or from another component, which the caller did not name itself.
     */
    String ownerId();

    /**
     * How much of this lease is left, measured on the monotonic clock from when the lease was last
     * requested -- so a step in the wall clock cannot lengthen or shorten it under its holder. This
     * is the reading to renew against, and the only one this object offers about time: the stored
     * deadline is wall-clock, and it exists so <em>other</em> clients can judge the lease, not so
     * its holder can measure how much of it has gone.
     * <p>
     * {@link Duration#ZERO} once it has run out. That is this holder's own opinion and not a
     * cluster fact: a successor may already have taken the lock, which only a fresh
     * {@link #info()} can tell you -- and even that answer is stale the moment it arrives, which is
     * why {@link #fencingToken()} and not a check like it is what makes the work safe.
     */
    Duration remainingLease();

    /**
     * Monotonic per-key generation captured at acquire time. Use this as a
     * fencing token: pass it to downstream services or include it in
     * conditional writes so that operations from a displaced (but
     * locally-still-thinks-it-holds-the-lock) holder can be rejected.
     */
    long fencingToken();
}
