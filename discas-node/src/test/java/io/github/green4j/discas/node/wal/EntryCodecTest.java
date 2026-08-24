/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("EntryCodec -- WAL entry encode/decode edge cases")
class EntryCodecTest {

    private static final NodeId N1 = NodeId.of("1");
    private static final NodeId N2 = NodeId.of("2");

    @Test
    void acceptWithNullValueAndTombstone() {
        final HashedBytes key = new HashedBytes(new byte[]{6});
        final Ballot ballot = new Ballot(5, N1);
        final Wal.Entry.Accept original = new Wal.Entry.Accept(key, ballot, null, true);

        final ByteBuffer encoded = EntryCodec.encode(original);
        final Wal.Entry decoded = EntryCodec.decode(EntryCodec.TYPE_ACCEPT, encoded);

        assertInstanceOf(Wal.Entry.Accept.class, decoded);
        final Wal.Entry.Accept accept = (Wal.Entry.Accept) decoded;
        assertNull(accept.value());
        assertTrue(accept.tombstone());
    }

    @Test
    @DisplayName("A purge carries the ballot it was decided about, or it decides nothing")
    void purgeRoundtrip() {
        final HashedBytes key = new HashedBytes(new byte[]{7, 7});
        final Ballot tombstone = new Ballot(41, N2);

        final ByteBuffer encoded = EntryCodec.encode(new Wal.Entry.Purge(key, tombstone));
        final Wal.Entry decoded = EntryCodec.decode(EntryCodec.TYPE_PURGE, encoded);

        assertInstanceOf(Wal.Entry.Purge.class, decoded);
        final Wal.Entry.Purge purge = (Wal.Entry.Purge) decoded;
        assertEquals(key, purge.key());
        assertEquals(tombstone, purge.ballot());
    }

    @Test
    void snapshotEntryRoundtrip() {
        final HashedBytes key = new HashedBytes(new byte[]{1, 2});
        final Ballot promised = new Ballot(10, N1);
        final Ballot accepted = new Ballot(8, N2);
        final HashedBytes value = new HashedBytes(new byte[]{99});
        final Wal.SnapshotEntry original = new Wal.SnapshotEntry(key, promised, accepted, value, false);

        final ByteBuffer encoded = EntryCodec.encodeSnapshotEntry(original);
        final Wal.SnapshotEntry decoded = EntryCodec.decodeSnapshotEntry(encoded);

        assertEquals(key, decoded.key());
        assertEquals(promised, decoded.promised());
        assertEquals(accepted, decoded.accepted());
        assertEquals(value, decoded.value());
        assertFalse(decoded.tombstone());
    }
}
