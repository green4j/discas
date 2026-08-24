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
     * How much of this lease is left, measured on the monotonic clock from when the lease was last
     * requested -- so a step in the wall clock cannot lengthen or shorten it under its holder.
     * <p>
     * This is the reading to renew against. {@link #lockInfo()} carries the stored wall-clock
     * deadline, which exists so <em>other</em> clients can judge the lease; it is the wrong clock
     * for measuring how much time has passed here, and the right one for saying when the lease ends
     * to someone else.
     * <p>
     * {@link Duration#ZERO} once it has run out. That is this holder's own opinion and not a
     * cluster fact: a successor may already have taken the lock, which only {@link #validate()} or
     * a fresh {@link #info()} can tell you.
     */
    Duration remainingLease();

    /**
     * Local lock state. Exposes both the immutable acquire-time
     * snapshot and the latest locally-known lease (updated on each
     * successful {@link #renew(Duration)}).
     */
    LockInfoView lockInfo();

    /**
     * Monotonic per-key generation captured at acquire time. Use this as a
     * fencing token: pass it to downstream services or include it in
     * conditional writes so that operations from a displaced (but
     * locally-still-thinks-it-holds-the-lock) holder can be rejected.
     */
    long fencingToken();

    /**
     * Returns true if the cluster's current generation for this lock key
     * still matches {@link #fencingToken()}. A holder that was paused past its lease -- or whose
     * lease was taken by a successor that read a stale cluster value as "expired" -- can use this
     * to discover it has been displaced and abort safely.
     */
    CompletableFuture<Boolean> validate();
}
