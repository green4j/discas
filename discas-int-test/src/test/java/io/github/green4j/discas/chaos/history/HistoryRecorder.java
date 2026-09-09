/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos.history;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.node.HashedBytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a {@link DisCasClient} and records every operation it performs as an
 * {@link OpRecord} -- capturing the real-time invoke/return interval and outcome -- for
 * later verification by {@link RegisterLinearizabilityChecker}. Each call blocks until
 * the client future resolves (or times out), so within a single recorder operations are
 * sequential; concurrency across the whole test comes from running several recorders on
 * separate threads. The recorded list is synchronised, so one recorder is safe to share,
 * though the chaos test uses one recorder per client and merges the lists at the end.
 */
public final class HistoryRecorder {

    private final DisCasClient client;
    private final long timeoutMs;
    private final List<OpRecord> history = Collections.synchronizedList(new ArrayList<>());

    /**
     * Operations issued, as distinct from records appended. A fenced CAS is a read and then a write,
     * so it contributes one call and one or two records; a caller checking that every worker
     * finished wants this, and the checker wants the records.
     */
    private final AtomicInteger calls = new AtomicInteger();

    public HistoryRecorder(final DisCasClient client, final long timeoutMs) {
        this.client = client;
        this.timeoutMs = timeoutMs;
    }

    private static ByteBuffer buf(final HashedBytes b) {
        return b == null ? null : b.toBuffer();
    }

    public void put(final HashedBytes key, final HashedBytes value) {
        calls.incrementAndGet();
        final long invoke = System.nanoTime();
        Version committed = null;
        OpRecord.Status status;
        try {
            committed = client.put(key.toBuffer(), value.toBuffer())
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            status = OpRecord.Status.OK;
        } catch (final Exception e) {
            status = OpRecord.Status.UNKNOWN;
        }
        history.add(OpRecord.put(key, value, committed, status, invoke, System.nanoTime()));
    }

    public void delete(final HashedBytes key) {
        calls.incrementAndGet();
        final long invoke = System.nanoTime();
        Version committed = null;
        OpRecord.Status status;
        try {
            committed = client.delete(key.toBuffer()).get(timeoutMs, TimeUnit.MILLISECONDS);
            status = OpRecord.Status.OK;
        } catch (final Exception e) {
            status = OpRecord.Status.UNKNOWN;
        }
        history.add(OpRecord.delete(key, committed, status, invoke, System.nanoTime()));
    }

    public HashedBytes get(final HashedBytes key) {
        calls.incrementAndGet();
        final long invoke = System.nanoTime();
        HashedBytes observed = null;
        Version observedAt = null;
        OpRecord.Status status;
        try {
            final GetResult result = client.get(key.toBuffer())
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            final ByteBuffer value = result.value();
            observed = value == null ? null : new HashedBytes(value);
            observedAt = result.version();
            status = OpRecord.Status.OK;
        } catch (final Exception e) {
            status = OpRecord.Status.UNKNOWN;
        }
        history.add(OpRecord.get(key, observed, observedAt, status, invoke, System.nanoTime()));
        return observed;
    }

    /**
     * A compare-and-set with {@code expected} as its intended precondition, performed the only way
     * the store offers: read the version, then write fenced on it.
     *
     * <h4>What each outcome constrains</h4>
     * <ul>
     *   <li><b>Swapped.</b> Recorded as the fenced CAS it was. The fence is what the checker
     *       replays, so a swap that could only have happened at one version is held to it.</li>
     *   <li><b>Lost the compare.</b> Nothing of the caller's was written, and the refusal carries
     *       the value and version in force. That is an observation, so it is recorded as a
     *       {@code GET} of them. Recording it as a failed CAS would say less: the coordinator may
     *       have committed the state it found before answering, so the version that comes back is
     *       not necessarily the one the swap lost to.</li>
     *   <li><b>Unknown.</b> Recorded as a CAS with {@code UNKNOWN}, which the checker explores both
     *       ways -- took effect and did not -- but only where the fence could have held.</li>
     * </ul>
     * The intervening read is itself recorded, so its constraint is not lost either.
     */
    public void cas(final HashedBytes key, final HashedBytes expected, final HashedBytes desired) {
        calls.incrementAndGet();
        final HashedBytes observedBefore = getVersionedRecorded(key);
        if (observedBefore != null && !observedBefore.equals(expected)) {
            // The precondition does not hold, so the caller's CAS has nothing to attempt. The read
            // above already recorded what was seen.
            return;
        }
        if (observedBefore == null && expected != null) {
            return;
        }
        final Version fence = lastVersionRead;
        if (fence == null) {
            return; // the read failed; it recorded UNKNOWN and there is no version to fence on
        }

        final long invoke = System.nanoTime();
        try {
            final CasResult result =
                    client.cas(key.toBuffer(), fence, buf(desired))
                            .get(timeoutMs, TimeUnit.MILLISECONDS);
            final HashedBytes observed = result.value() == null
                    ? null : new HashedBytes(result.value());
            if (result.swapped()) {
                history.add(OpRecord.cas(key, expected, desired, fence, true, observed,
                        result.version(), OpRecord.Status.OK, invoke, System.nanoTime()));
            } else {
                history.add(OpRecord.get(key, observed, result.version(), OpRecord.Status.OK,
                        invoke, System.nanoTime()));
            }
        } catch (final Exception e) {
            history.add(OpRecord.cas(key, expected, desired, fence, false, null,
                    null, OpRecord.Status.UNKNOWN, invoke, System.nanoTime()));
        }
    }

    /** The version from the most recent {@link #getVersionedRecorded}, or null if that read failed. */
    private Version lastVersionRead;

    /** A recorded linearizable read that also captures the version the CAS above fences on. */
    private HashedBytes getVersionedRecorded(final HashedBytes key) {
        final long invoke = System.nanoTime();
        HashedBytes observed = null;
        OpRecord.Status status;
        lastVersionRead = null;
        try {
            final GetResult read = client
                    .get(key.toBuffer(), ReadConsistency.LINEARIZABLE)
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            observed = read.value() == null ? null : new HashedBytes(read.value());
            lastVersionRead = read.version();
            status = OpRecord.Status.OK;
        } catch (final Exception e) {
            status = OpRecord.Status.UNKNOWN;
        }
        history.add(OpRecord.get(key, observed, lastVersionRead, status, invoke, System.nanoTime()));
        return observed;
    }

    /** How many operations were issued through this recorder. */
    public int callCount() {
        return calls.get();
    }

    /** An immutable copy of everything recorded so far. */
    public List<OpRecord> snapshot() {
        synchronized (history) {
            return new ArrayList<>(history);
        }
    }
}
