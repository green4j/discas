/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;

public class KeyDigest implements Comparable<KeyDigest> {
    private final HashedBytes key;
    private final Ballot accepted;
    private final HashedBytes valueHash;
    private final boolean tombstone;

    public KeyDigest(final HashedBytes key,
                     final Ballot accepted,
                     final HashedBytes valueHash,
                     final boolean tombstone) {
        this.key = key;
        this.accepted = accepted;
        this.valueHash = valueHash;
        this.tombstone = tombstone;
    }

    public HashedBytes key() {
        return key;
    }

    public Ballot accepted() {
        return accepted;
    }

    public HashedBytes valueHash() {
        return valueHash;
    }

    public boolean tombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(final KeyDigest other) {
        return key.compareTo(other.key);
    }

    @Override
    public String toString() {
        return "KeyDigest{"
                + "key=" + key
                + ", accepted=" + accepted
                + ", valueHash=" + valueHash
                + ", tombstone=" + tombstone
                + '}';
    }
}
