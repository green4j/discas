/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

import io.github.green4j.discas.client.ClusterClock;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * The lease-based {@link Lock} handed out by a successful acquire.
 * <p>
 * Holds no connection of its own: every remote call goes through the narrow
 * {@link LockClientOps} view it was built with, so the lock cannot be used to reach the rest of
 * the key space. The mutable state is the locally-tracked current lease, advanced by
 * {@link #renew}; it is {@code volatile} because a lock is routinely renewed from a keep-alive
 * thread while the work it guards runs on another.
 *
 * <h2>Two clocks, two jobs</h2>
 * The stored {@code leaseUntilEpochMs} is wall-clock, corrected by {@link ClusterClock}, because it
 * crosses processes -- a successor has to be able to read it. The holder's own question, <em>how
 * much of my lease is left</em>, is answered from {@link #remainingLease()} on the monotonic clock
 * instead, anchored when the lease was requested. An NTP step can therefore neither lengthen nor
 * shorten a lease under its owner, which the wall clock alone could do in either direction.
 * <p>
 * The anchor is taken <em>before</em> the request goes out, so the remaining time reported here is
 * always a little short of what was stored. Under-reporting is the safe direction: a holder renews
 * marginally early rather than believing in a lease that has already gone.
 */
public final class DistributedLock implements Lock {
    private final ByteBuffer key;
    private final LockToken token;
    private final LockInfo snapshot;
    private final LockClientOps clientOps;
    private final ClusterClock clock;
    private volatile LockInfo currentLease;
    /** When this lease runs out on the monotonic clock; see the class javadoc. */
    private volatile long leaseExpiryNanos;

    /**
     * Binds an already-acquired lock on {@code key} to the {@code token} it was taken under.
     * Callers get one of these from an acquire rather than constructing it: a token that was
     * never written to the cluster produces a lock whose every operation fails the CAS.
     */
    public DistributedLock(final ByteBuffer key,
                           final LockToken token,
                           final LockInfo lockInfo,
                           final LockClientOps clientOps,
                           final ClusterClock clock,
                           final long leaseExpiryNanos) {
        this.key = key;
        this.token = token;
        this.snapshot = lockInfo;
        this.currentLease = lockInfo;
        this.clientOps = clientOps;
        this.clock = clock;
        this.leaseExpiryNanos = leaseExpiryNanos;
    }

    @Override
    public CompletableFuture<LockWriteResult> release() {
        return clientOps.release(key.duplicate(), token);
    }

    /**
     * Extends the lease via CAS. On success, updates the locally tracked
     * "current" lease used by {@link #lockInfo()}; {@link LockInfoView#snapshot()}
     * still returns the original acquire-time info.
     */
    @Override
    public CompletableFuture<LockWriteResult> renew(final Duration newLeaseTtl) {
        // Both anchors taken before the call: the monotonic one is what this holder judges its own
        // lease by, the corrected wall one is what the record will say to everyone else. Each is a
        // slight underestimate of what renewLock stores a few milliseconds later, which is the safe
        // side for a caller deciding when to renew again.
        final long requestedAtNanos = clock.monotonicNanos();
        final long requestedAtMs = clock.nowMillis();
        return clientOps.renewLock(key.duplicate(), token, newLeaseTtl)
                .thenApply(result -> {
                    if (result.applied()) {
                        leaseExpiryNanos = requestedAtNanos + newLeaseTtl.toNanos();
                        currentLease = new LockInfo(
                                snapshot.ownerId(),
                                snapshot.token(),
                                snapshot.acquiredAtEpochMs(),
                                requestedAtMs + newLeaseTtl.toMillis(),
                                snapshot.generation(),
                                false);
                    }
                    return result;
                });
    }

    @Override
    public CompletableFuture<LockInfoResult> info() {
        return clientOps.getLockInfo(key.duplicate());
    }

    @Override
    public LockToken token() {
        return token;
    }

    @Override
    public Duration remainingLease() {
        // Subtraction, not comparison: the form that stays correct across a nanoTime wraparound.
        final long remainingNanos = leaseExpiryNanos - clock.monotonicNanos();
        return remainingNanos <= 0 ? Duration.ZERO : Duration.ofNanos(remainingNanos);
    }

    @Override
    public LockInfoView lockInfo() {
        return new LockInfoView(snapshot, currentLease);
    }

    @Override
    public long fencingToken() {
        return snapshot.generation();
    }

    @Override
    public CompletableFuture<Boolean> validate() {
        final long mine = snapshot.generation();
        return clientOps.getLockInfo(key.duplicate()).thenApply(result -> {
            if (result.status() != LockInfoStatus.LOCKED) {
                return false;
            }
            final LockInfo current = result.info();
            return current != null && current.generation() == mine;
        });
    }
}
