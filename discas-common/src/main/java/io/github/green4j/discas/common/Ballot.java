/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import io.github.green4j.discas.common.identity.NodeId;

/**
 * A Paxos ballot: a counter, broken ties by the proposing {@link NodeId}, so that every ballot in
 * the cluster is totally ordered and no two proposers can issue the same one.
 * <p>
 * Also a key's version as clients see it. It advances on every commit, tombstones included, so a
 * caller that holds one can fence a later write on it.
 */
public class Ballot implements Comparable<Ballot> {

    /** No ballot: what a key that has never been committed reports as its version. */
    public static final Ballot ZERO = new Ballot(0, NodeId.NONE);

    private final long counter;
    private final NodeId nodeId;

    public Ballot(final long counter,
                  final NodeId nodeId) {
        this.counter = counter;
        this.nodeId = nodeId;
    }

    /** The ballot number. Higher wins; equal counters are broken by {@link #nodeId()}. */
    public long counter() {
        return counter;
    }

    /** The proposer that issued it, which makes ballots from different proposers comparable. */
    public NodeId nodeId() {
        return nodeId;
    }

    @Override
    public int compareTo(final Ballot other) {
        final int compared = Long.compare(counter, other.counter);
        return compared != 0 ? compared : nodeId.compareTo(other.nodeId);
    }

    /** Whether this is {@link #ZERO}. */
    public boolean isZero() {
        return counter == 0 && nodeId.equals(NodeId.NONE);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Ballot ballot = (Ballot) o;
        return counter == ballot.counter && nodeId.equals(ballot.nodeId);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(counter);
        result = 31 * result + nodeId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Ballot(" + counter + "," + nodeId + ")";
    }
}
