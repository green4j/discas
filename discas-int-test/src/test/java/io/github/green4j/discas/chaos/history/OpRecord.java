/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos.history;

import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.node.HashedBytes;

/**
 * One recorded client operation against a single key, with the real-time interval
 * ({@link #invokeNanos}..{@link #returnNanos}) during which it was in flight. This is
 * the unit consumed by {@link RegisterLinearizabilityChecker}: the checker searches for
 * a sequential ordering of these records -- consistent with their real-time intervals --
 * that satisfies a register's sequential specification.
 * <p>
 * A record carries versions as well as bytes, because the store's register is a (value, version)
 * pair and its compare-and-set fences on the version. {@link #version()} is the version the
 * operation observed or committed at, and {@link #fencedOn()} is the version a CAS required. Both
 * are null when the operation's outcome was never learned.
 */
public final class OpRecord {

    public enum Kind { GET, PUT, CAS, DELETE }

    /**
     * OK   -- the operation completed with the recorded result (definite).
     * FAIL -- the operation definitely did not take effect (a clean rejection).
     * UNKNOWN -- indeterminate (timeout/exception): it may or may not have taken effect,
     *            so the checker treats mutating ops as optionally linearized.
     */
    public enum Status { OK, FAIL, UNKNOWN }

    private final HashedBytes key;
    private final Kind kind;
    private final HashedBytes a;         // PUT value, or CAS expected
    private final HashedBytes b;         // CAS desired
    private final HashedBytes observed;  // GET result, or CAS observed current value
    private final Version fencedOn;      // CAS precondition
    private final Version version;       // version observed or committed at, null if never learned
    private final boolean swapped; // CAS outcome
    private final Status status;
    private final long invokeNanos;
    private final long returnNanos;

    private OpRecord(final HashedBytes key, final Kind kind, final HashedBytes a, final HashedBytes b,
                     final HashedBytes observed, final Version fencedOn, final Version version,
                     final boolean swapped, final Status status,
                     final long invokeNanos, final long returnNanos) {
        this.key = key;
        this.kind = kind;
        this.a = a;
        this.b = b;
        this.observed = observed;
        this.fencedOn = fencedOn;
        this.version = version;
        this.swapped = swapped;
        this.status = status;
        this.invokeNanos = invokeNanos;
        this.returnNanos = returnNanos;
    }

    public static OpRecord get(final HashedBytes key, final HashedBytes observed, final Version version,
                               final Status status, final long invokeNanos, final long returnNanos) {
        return new OpRecord(key, Kind.GET, null, null, observed, null, version, false, status,
                invokeNanos, returnNanos);
    }

    public static OpRecord put(final HashedBytes key, final HashedBytes value, final Version version,
                               final Status status, final long invokeNanos, final long returnNanos) {
        return new OpRecord(key, Kind.PUT, value, null, null, null, version, false, status,
                invokeNanos, returnNanos);
    }

    public static OpRecord delete(final HashedBytes key, final Version version, final Status status,
                                  final long invokeNanos, final long returnNanos) {
        return new OpRecord(key, Kind.DELETE, null, null, null, null, version, false, status,
                invokeNanos, returnNanos);
    }

    public static OpRecord cas(final HashedBytes key, final HashedBytes expected, final HashedBytes desired,
                               final Version fencedOn, final boolean swapped, final HashedBytes observed,
                               final Version version, final Status status,
                               final long invokeNanos, final long returnNanos) {
        return new OpRecord(key, Kind.CAS, expected, desired, observed, fencedOn, version, swapped,
                status, invokeNanos, returnNanos);
    }

    public HashedBytes key() {
        return key;
    }

    public Kind kind() {
        return kind;
    }

    /** PUT value or CAS expected. */
    public HashedBytes arg() {
        return a;
    }

    /** CAS desired. */
    public HashedBytes desired() {
        return b;
    }

    public HashedBytes observed() {
        return observed;
    }

    /**
     * The version this CAS required the register to be at, or null when the operation carried no
     * fence. A fenced write applies exactly while the register is at this version, which is the
     * constraint a value alone cannot express: identical bytes can be written twice.
     */
    public Version fencedOn() {
        return fencedOn;
    }

    /**
     * The version the operation observed (a read) or committed at (a write), or null when the
     * client never learned it -- an indeterminate write commits at a ballot nobody recorded.
     */
    public Version version() {
        return version;
    }

    public boolean swapped() {
        return swapped;
    }

    public Status status() {
        return status;
    }

    public long invokeNanos() {
        return invokeNanos;
    }

    public long returnNanos() {
        return returnNanos;
    }

    public boolean mutating() {
        return kind != Kind.GET;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(kind.name()).append('(');
        switch (kind) {
            case GET:
                sb.append("->").append(observed);
                break;
            case PUT:
                sb.append(a);
                break;
            case DELETE:
                break;
            case CAS:
                sb.append(a).append("=>").append(b)
                        .append(" fencedOn=").append(fencedOn)
                        .append(" swapped=").append(swapped);
                break;
            default:
                break;
        }
        return sb.append(") ").append(status).append(" @").append(version)
                .append(" [").append(invokeNanos).append("..").append(returnNanos).append(']').toString();
    }
}
