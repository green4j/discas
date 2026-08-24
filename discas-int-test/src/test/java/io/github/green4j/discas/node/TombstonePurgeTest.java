/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The record and the store half of tombstone collection: what a purge removes, what it must leave
 * alone, and what it has to leave behind.
 * <p>
 * A purge is the one record in the log that removes state instead of raising a bound, so it is the
 * one that can lose something. Two things it must not lose: a key written again above the purged
 * tombstone's ballot, and the promise the dropped key was carrying.
 */
@DisplayName("Purge -- collecting a tombstone the cluster agreed nobody can resurrect")
class TombstonePurgeTest {

    private static final NodeId SELF = NodeId.of("1");
    private static final NodeId PEER = NodeId.of("9");
    private static final HashedBytes KEY = TestBytes.hashed("k");

    private static LocalStore recovered(final Wal wal) {
        final LocalStore store = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        while (loader.loadBatch(1024)) {
            // drain
        }
        return store;
    }

    @Test
    @DisplayName("The key leaves the store, both indexes and its range digest")
    void purgeDropsTheKeyEverywhereItWasIndexed() {
        final LocalStore store = recovered(new InMemoryWal());
        final Ballot value = new Ballot(4L, PEER);
        final Ballot tombstone = new Ballot(7L, PEER);
        store.writeAccept(KEY, value, TestBytes.hashed("v"), false);
        store.writeAccept(KEY, tombstone, null, true);

        final Map<Integer, HashedBytes> digestsWithTombstone = store.computeRangeDigests();
        final int range = KEY.rangeOf(LocalStore.NUM_RANGES);
        assertNotNull(digestsWithTombstone.get(range), "Precondition: the tombstone is advertised");

        assertTrue(store.purge(KEY, tombstone));

        assertNull(store.get(KEY), "The key is absent, not tombstoned");
        assertEquals(0, store.keyCount());
        assertTrue(store.scanLocal(null, null, 10, key -> true).entries().isEmpty(),
                "A purged key must not be scannable");
        assertTrue(store.keyDigestsForRange(range, null, 10).digests().isEmpty(),
                "Nor offered to a peer as a key digest");
        assertNull(store.computeRangeDigests().get(range),
                "A replica that purged must stop advertising the tombstone it no longer holds");
    }

    @Test
    @DisplayName("A purged key and a key that never existed are the same store to a peer")
    void aPurgedKeyComparesEqualToOneThatNeverHadIt() {
        // What lets a cluster mid-purge settle instead of flapping: the two members are not
        // 'divergent, repair it', they are equal. A purge that left any trace -- a range digest, a
        // key digest, a scan entry -- would make every collection a permanent disagreement between
        // the members that applied it and the ones whose storage was replaced.
        final LocalStore purged = recovered(new InMemoryWal());
        final Ballot value = new Ballot(4L, PEER);
        final Ballot tombstone = new Ballot(7L, PEER);
        purged.writeAccept(KEY, value, TestBytes.hashed("v"), false);
        purged.writeAccept(KEY, tombstone, null, true);
        assertTrue(purged.purge(KEY, tombstone));

        final LocalStore neverHadIt = recovered(new InMemoryWal());
        final int range = KEY.rangeOf(LocalStore.NUM_RANGES);

        assertEquals(neverHadIt.computeRangeDigests(), purged.computeRangeDigests(),
                "The ranges a peer compares first");
        assertEquals(neverHadIt.keyDigestsForRange(range, null, 10).digests(),
                purged.keyDigestsForRange(range, null, 10).digests(),
                "And the keys it drills into when a range does not match");
        assertEquals(neverHadIt.keyCount(), purged.keyCount());
    }

    @ParameterizedTest(name = "purged={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("A purge in the tail recovers to an absent key; without it, to a tombstone")
    void purgeInTheTailRecoversToAbsent(final boolean purged, @TempDir final Path directory)
            throws Exception {
        final Ballot tombstone = new Ballot(7L, PEER);

        final FileWal writing = new FileWal(StorageConfig.builder().baseDirectory(directory).build());
        writing.initialize();
        final LocalStore store = recovered(writing);
        store.writeAccept(KEY, new Ballot(4L, PEER), TestBytes.hashed("v"), false);
        store.writeAccept(KEY, tombstone, null, true);
        if (purged) {
            assertTrue(store.purge(KEY, tombstone));
        }
        writing.force();
        writing.close();

        final FileWal reading = new FileWal(StorageConfig.builder().baseDirectory(directory).build());
        reading.initialize();
        final LocalStore reopened = recovered(reading);

        // Verified by removal: the same directory without the purge record recovers the tombstone,
        // so this assertion is about the record and not about a store that lost the key anyway.
        if (purged) {
            assertNull(reopened.get(KEY), "The purge must survive the restart that reads it");
            assertEquals(0, reopened.keyCount());
        } else {
            assertTrue(reopened.get(KEY).tombstone, "Without a purge the tombstone is kept forever");
        }
        reading.close();
    }

    @Test
    @DisplayName("A key written again above the tombstone's ballot is not what was decided")
    void purgeLeavesAKeyRewrittenAboveIt() {
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore store = recovered(wal);
        final Ballot tombstone = new Ballot(7L, PEER);
        store.writeAccept(KEY, tombstone, null, true);
        store.writeAccept(KEY, new Ballot(9L, PEER), TestBytes.hashed("again"), false);

        assertFalse(store.purge(KEY, tombstone),
                "The state this node holds is not the state the cluster agreed about");
        assertEquals(TestBytes.hashed("again"), store.get(KEY).value);
        assertEquals(0, wal.countEntries(Wal.Entry.Purge.class),
                "A decision this node cannot apply must not be logged either");
    }

    @Test
    @DisplayName("Replaying a purge twice reaches the same state as replaying it once")
    void purgeIsIdempotentUnderReplay() {
        final InMemoryWal wal = new InMemoryWal();
        final Ballot tombstone = new Ballot(7L, PEER);
        wal.append(new Wal.Entry.Accept(KEY, tombstone, null, true));
        wal.append(new Wal.Entry.Purge(KEY, tombstone));
        wal.append(new Wal.Entry.Purge(KEY, tombstone));
        // And a key written after the purge, to pin that a repeated purge does not follow it.
        wal.append(new Wal.Entry.Accept(KEY, new Ballot(11L, PEER), TestBytes.hashed("after"), false));
        wal.append(new Wal.Entry.Purge(KEY, tombstone));

        final LocalStore store = recovered(wal);

        assertEquals(TestBytes.hashed("after"), store.get(KEY).value,
                "The purge applies to the ballot it names, whatever order it arrives in");
    }

    @Test
    @DisplayName("A node that cannot account for itself drops state no peer confirms, and only that")
    void unaccountedStateIsDropped() {
        // The rule of item 8, at the level it is decided: everything at or below a floor this node
        // had to be given is on the far side of a hole in its own history. Anti-entropy is what
        // says no peer holds the key; this says what may be done about it.
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore store = recovered(wal);
        final Ballot value = new Ballot(4L, PEER);
        store.writeAccept(KEY, value, TestBytes.hashed("v"), false);

        assertFalse(store.dropUnaccountedFor(KEY),
                "A node that replayed its own log can account for every key in it");

        assertTrue(store.adoptRecoveryFloor(50L), "Precondition: this node takes its floor");
        final HashedBytes above = TestBytes.hashed("written-since");
        store.writeAccept(above, new Ballot(51L, PEER), TestBytes.hashed("v2"), false);

        assertFalse(store.dropUnaccountedFor(above),
                "State written above the floor is ordinary state, whatever this node lost before it");
        assertFalse(store.dropUnaccountedFor(TestBytes.hashed("never-held")),
                "And a key this node does not hold is nothing to drop");

        assertTrue(store.dropUnaccountedFor(KEY));
        assertNull(store.get(KEY), "The value nobody confirms was never chosen");
        assertEquals(1, wal.countEntries(Wal.Entry.Purge.class),
                "And a record says so, or replay brings it back");
    }

    @Test
    @DisplayName("The promise the dropped key carried becomes the recovery floor")
    void purgeLeavesTheForgottenPromiseAsAFloor() {
        final LocalStore store = recovered(new InMemoryWal());
        final Acceptor acceptor = new Acceptor(SELF, store);
        final Ballot tombstone = new Ballot(7L, PEER);

        assertTrue(acceptor.handlePrepare(new PeerMessage.PrepareReq(PEER, 1L, KEY, tombstone)).ok());
        assertTrue(acceptor.handleAccept(
                new PeerMessage.AcceptReq(PEER, 2L, KEY, tombstone, null, true)).ok());
        assertTrue(store.purge(KEY, tombstone));

        assertTrue(store.recoveryPromiseFloor() >= tombstone.counter(),
                "The floor has to cover a promise this node can no longer produce, was "
                        + store.recoveryPromiseFloor());

        // The load-bearing assertion. An accept delayed since before the delete finds no state at
        // all now, so keyState.promised is zero and only the floor is left to refuse it. Without
        // that, this write resurrects a value the cluster agreed was gone -- on every member, since
        // anti-entropy then copies it to the ones that purged.
        final PeerMessage.AcceptResp late = acceptor.handleAccept(new PeerMessage.AcceptReq(
                PEER, 3L, KEY, new Ballot(5L, PEER), TestBytes.hashed("delayed"), false));
        assertFalse(late.ok(), "An accept below the purged tombstone's ballot must be refused");

        // The refusal still materialises a KeyState -- handleAccept calls getOrCreate before it
        // decides anything -- so what has to be checked is that nothing of the value survives it.
        // A promise-only state is invisible to readers, to peers and to snapshots, and
        // evictPromiseOnly reclaims it; a value at ballot 5 would be the resurrection.
        final KeyState after = store.get(KEY);
        assertTrue(after == null || after.accepted.isZero(),
                "A refused accept must leave no accepted state behind");
        assertNull(after == null ? null : after.value, "And no value for a reader to find");
        assertTrue(store.scanLocal(null, null, 10, key -> true).entries().isEmpty(),
                "The purged key must not come back to readers as a phantom");
    }
}
