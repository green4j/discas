/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Records lock acquire/release events across all workers and enforces the
 * at-most-one-holder invariant in-process.
 *
 * <p>Each lock key has an {@link AtomicReference} naming its current holder
 * (worker's owner id). A worker that has just acquired the lock distributedly
 * MUST be able to {@link AtomicReference#compareAndSet}{@code (null, me)} on
 * the tracker; if not, two workers believe they hold the same lock at the same
 * time -- a violation. Symmetric check on release.
 *
 * <p>Violations are recorded to {@link #violations()} (do not throw inline so
 * the chaos run keeps going and surfaces the full picture).
 */
final class ChaosLockMonitor {
    static final class Violation {
        final String kind;
        final String key;
        final String observedHolder;
        final String attemptingHolder;
        final long timestampMs;

        Violation(final String kind, final String key, final String observedHolder,
                  final String attemptingHolder, final long timestampMs) {
            this.kind = kind;
            this.key = key;
            this.observedHolder = observedHolder;
            this.attemptingHolder = attemptingHolder;
            this.timestampMs = timestampMs;
        }

        @Override
        public String toString() {
            return kind + "{key=" + key + ", observed=" + observedHolder
                    + ", attempting=" + attemptingHolder + ", t=" + timestampMs + "}";
        }
    }

    private final Map<String, AtomicReference<String>> holders = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> highestFencingToken = new ConcurrentHashMap<>();
    private final List<Violation> violations = new CopyOnWriteArrayList<>();
    private final AtomicLong acquireCount = new AtomicLong();
    private final AtomicLong releaseCount = new AtomicLong();
    private final AtomicLong staleReleaseCount = new AtomicLong();

    /**
     * Called by a worker that distributedly acquired the lock. Records the
     * acquisition and verifies no other worker thinks it holds the same key.
     * Also asserts that the fencing token is strictly greater than the highest
     * one previously observed for the same key. Returns true if the in-process
     * tracker accepted this acquire; false indicates a concurrent-holder or
     * fencing-token violation (already recorded).
     */
    boolean recordAcquire(final String key, final String ownerId, final long fencingToken) {
        acquireCount.incrementAndGet();
        final AtomicLong highest = highestFencingToken.computeIfAbsent(key, k -> new AtomicLong());
        while (true) {
            final long current = highest.get();
            if (fencingToken <= current) {
                violations.add(new Violation("fencing-token-not-monotonic", key,
                        Long.toString(current), ownerId + "@" + fencingToken,
                        System.currentTimeMillis()));
                return false;
            }
            if (highest.compareAndSet(current, fencingToken)) {
                break;
            }
        }
        final AtomicReference<String> ref = holders.computeIfAbsent(key, k -> new AtomicReference<>());
        if (!ref.compareAndSet(null, ownerId)) {
            violations.add(new Violation("concurrent-acquire", key, ref.get(), ownerId,
                    System.currentTimeMillis()));
            return false;
        }
        return true;
    }

    /**
     * Called by a worker before it asks the cluster to release. If the
     * tracker still names this worker as the holder, clears it. If not, the
     * lock has been observed by another acquire -- this is acceptable only when
     * the lease expired and another worker stole the slot; we record it as a
     * stale release rather than a hard violation.
     */
    void recordRelease(final String key, final String ownerId) {
        releaseCount.incrementAndGet();
        final AtomicReference<String> ref = holders.get(key);
        if (ref == null) {
            return;
        }
        if (!ref.compareAndSet(ownerId, null)) {
            staleReleaseCount.incrementAndGet();
        }
    }

    /**
     * Called when a worker observes (via getLockInfo) that the lock is held
     * by some owner. If the in-process tracker thinks a different owner holds
     * the lock, that is a divergence -- flag it.
     */
    void observeHolder(final String key, final String observedOwner) {
        final AtomicReference<String> ref = holders.get(key);
        if (ref == null) {
            return;
        }
        final String tracked = ref.get();
        if (tracked != null && !tracked.equals(observedOwner)) {
            violations.add(new Violation("observation-divergence", key, observedOwner,
                    tracked, System.currentTimeMillis()));
        }
    }

    List<Violation> violations() {
        return violations;
    }

    long acquireCount() {
        return acquireCount.get();
    }

    long releaseCount() {
        return releaseCount.get();
    }

    long staleReleaseCount() {
        return staleReleaseCount.get();
    }
}
