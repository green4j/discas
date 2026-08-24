/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;

/**
 * The opaque, monotonic version of one key: what a caller fences a compare-and-set on and what a
 * watch waits to move past (analogous to a Consul {@code X-Consul-Index} or an etcd
 * {@code mod_revision}, but per key only -- discas is CASPaxos and has no global revision).
 *
 * <p>Called a version and nothing else. {@link ScanPage#nextCursor()} is the API's <em>cursor</em>,
 * and it is a different thing entirely -- a key to resume paging from.
 *
 * <p>A version wraps the key's accepted {@link Ballot}: it advances on every commit, including
 * tombstones, and is comparable within a single key (it is <em>not</em> dense and carries no
 * meaning across keys). {@link #INITIAL} is the "before anything" version -- watching a key from
 * {@code INITIAL} fires as soon as the key has any committed value.
 *
 * <p>{@link #token()} / {@link #parse(String)} round-trip the version as a compact string so it
 * can travel over a transport that only speaks text (e.g. the agent's {@code ?version=} query
 * parameter and {@code X-DisCas-Version} response header).
 */
public final class Version implements Comparable<Version> {

    /** The version that precedes every committed value; watching from here fires immediately. */
    public static final Version INITIAL = new Version(Ballot.ZERO);

    private final Ballot ballot;

    Version(final Ballot ballot) {
        this.ballot = ballot == null ? Ballot.ZERO : ballot;
    }

    Ballot ballot() {
        return ballot;
    }

    @Override
    public int compareTo(final Version other) {
        return ballot.compareTo(other.ballot);
    }

    /**
     * A compact, stable string form of this version: {@code "<counter>:<nodeId>"}. The counter is
     * a decimal {@code long} (never contains {@code ':'}), so {@link #parse(String)} splits on the
     * first colon and the node id -- which may itself contain colons -- is the remainder.
     */
    public String token() {
        return ballot.counter() + ":" + ballot.nodeId().value();
    }

    /**
     * Inverse of {@link #token()}. {@code null}, empty, or malformed input yields {@link #INITIAL}
     * so a client that sends a bad version simply gets "notify me on any value" rather than an
     * error.
     */
    public static Version parse(final String token) {
        if (token == null || token.isEmpty()) {
            return INITIAL;
        }
        final int colon = token.indexOf(':');
        if (colon < 0) {
            return INITIAL;
        }
        final long counter;
        try {
            counter = Long.parseLong(token.substring(0, colon));
        } catch (final NumberFormatException e) {
            return INITIAL;
        }
        final String node = token.substring(colon + 1);
        if (counter == 0 && node.isEmpty()) {
            return INITIAL;
        }
        return new Version(new Ballot(counter, node.isEmpty() ? NodeId.NONE : NodeId.of(node)));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return ballot.equals(((Version) o).ballot);
    }

    @Override
    public int hashCode() {
        return ballot.hashCode();
    }

    @Override
    public String toString() {
        return "Version(" + token() + ")";
    }
}
