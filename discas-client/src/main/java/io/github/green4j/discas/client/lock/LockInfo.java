/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

/**
 * An immutable description of one lock record: who holds it, since when, and for how long.
 * <p>
 * A point-in-time reading, never a live view. {@link #expired()} in particular is decided against
 * the clock supplied to {@link #fromRecord}, so a {@code LockInfo} kept around outgrows its own
 * answer -- re-read rather than re-test an old instance.
 */
public final class LockInfo {
    private final String ownerId;
    private final LockToken token;
    private final long acquiredAtEpochMs;
    private final long leaseUntilEpochMs;
    private final long generation;
    private final boolean expired;

    LockInfo(final String ownerId,
             final LockToken token,
             final long acquiredAtEpochMs,
             final long leaseUntilEpochMs,
             final long generation,
             final boolean expired) {
        this.ownerId = ownerId;
        this.token = token;
        this.acquiredAtEpochMs = acquiredAtEpochMs;
        this.leaseUntilEpochMs = leaseUntilEpochMs;
        this.generation = generation;
        this.expired = expired;
    }

    /**
     * Decodes a stored lock record into a reading taken at {@code now} (epoch millis), which is
     * the instant {@link #expired()} is settled against.
     */
    public static LockInfo fromRecord(final LockValueCodec.LockRecord record, final long now) {
        return new LockInfo(
                record.ownerId(),
                new LockToken(record.token()),
                record.acquiredAtEpochMs(),
                record.leaseUntilEpochMs(),
                record.generation(),
                record.leaseUntilEpochMs() <= now
        );
    }

    /**
     * The name the holder acquired under. Not a credential -- only the {@link Lock#token() token}
     * decides who may release or renew -- but not decoration either: it is the one field of a record
     * that its writer chose before writing it, which is what lets that writer recognise its own
     * lease afterwards when an acquire's outcome was never reported.
     * <p>
     * Comparing it against your own id is what {@code tryLock} does to answer
     * {@link LockAcquireStatus#HELD_BY_SELF}, and what {@code recoverLock} acts on. Both are only
     * as sound as the id's uniqueness among concurrent holders, which is the caller's to
     * guarantee.
     */
    public String ownerId() {
        return ownerId;
    }

    /**
     * The holder's token. Deliberately not public: a reading of a lock is available to anyone who
     * can read the key, and the token is what release and renew are conditioned on, so publishing
     * it here would let any reader end a lease it does not hold. A holder gets its own token from
     * {@link Lock#token()}, and a holder that lost it gets the lock back from {@code recoverLock}.
     */
    LockToken token() {
        return token;
    }

    /** When the lock was taken, in epoch millis on the cluster's clock as the acquirer read it. */
    public long acquiredAtEpochMs() {
        return acquiredAtEpochMs;
    }

    /**
     * When the lease runs out, in epoch millis on the cluster's clock -- every client corrects its
     * own against a coordinator's before writing or judging this, so two clients whose machines
     * disagree about the time still read the same deadline. See
     * {@link io.github.green4j.discas.client.ClusterClock}.
     * <p>
     * A holder asking how much of <em>its own</em> lease is left wants
     * {@link io.github.green4j.discas.client.lock.Lock#remainingLease()} instead: this is a
     * wall-clock instant, and elapsed time is not what a wall clock measures.
     */
    public long leaseUntilEpochMs() {
        return leaseUntilEpochMs;
    }

    /**
     * Per-key monotonic generation, bumped on every successful acquire.
     * Acts as the lock's fencing token: an application or downstream that
     * needs to verify "you still hold the lock" can compare this against
     * a fresh {@link LockInfoResult} from {@code getLockInfo}.
     */
    public long generation() {
        return generation;
    }

    /** Whether the lease had already run out at the instant this reading was taken. */
    public boolean expired() {
        return expired;
    }
}
