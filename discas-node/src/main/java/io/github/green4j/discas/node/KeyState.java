/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;

public final class KeyState {
    public Ballot promised;
    public Ballot accepted;
    public HashedBytes value;       // when tombstoned or never written, null
    public boolean tombstone;
    public long promisedAtNanos;

    public KeyState() {
        this.promised = Ballot.ZERO;
        this.accepted = Ballot.ZERO;
        this.value = null;
        this.tombstone = false;
    }

    public KeyState(final Ballot promised,
                    final Ballot accepted,
                    final HashedBytes value,
                    final boolean tombstone) {
        this.promised = promised;
        this.accepted = accepted;
        this.value = value;
        this.tombstone = tombstone;
    }

    public KeyDigest digest(final HashedBytes key) {
        final HashedBytes valHash = (value != null) ? value.sha256() : HashedBytes.EMPTY;
        return new KeyDigest(key, accepted, valHash, tombstone);
    }
}