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
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Recovery -- replay WAL + snapshot into LocalStore")
class RecoveryTest {

    private static final NodeId N1 = NodeId.of("1");

    private void drainLoader(final LocalStore.ThrottledLoader loader) {
        while (!loader.isDone()) {
            loader.loadBatch(1000);
        }
    }

    @Test
    @DisplayName("Recover from WAL only (no snapshot)")
    void recoverFromWalOnly() {
        final InMemoryWal wal = new InMemoryWal();
        wal.append(new Wal.Entry.Accept(TestBytes.hashed("k1"), new Ballot(1, N1), TestBytes.hashed("v1"), false));
        wal.append(new Wal.Entry.Accept(TestBytes.hashed("k2"), new Ballot(2, N1), TestBytes.hashed("v2"), false));

        final LocalStore store = new LocalStore(wal);
        drainLoader(store.beginRecovery());

        assertEquals(TestBytes.hashed("v1"), store.get(TestBytes.hashed("k1")).value);
        assertEquals(TestBytes.hashed("v2"), store.get(TestBytes.hashed("k2")).value);
    }

    @Test
    @DisplayName("Recover from snapshot only (no WAL tail)")
    void recoverFromSnapshotOnly() {
        final InMemoryWal wal = new InMemoryWal();

        // Populate store, take snapshot, then create new store from snapshot
        final LocalStore original = new LocalStore(wal);
        original.writeAccept(TestBytes.hashed("k1"), new Ballot(1, N1), TestBytes.hashed("v1"), false);
        original.writeAccept(TestBytes.hashed("k2"), new Ballot(2, N1), TestBytes.hashed("v2"), false);

        final LocalStore.ThrottledSnapshot snap = original.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }

        final LocalStore recovered = new LocalStore(wal);
        drainLoader(recovered.beginRecovery());

        assertEquals(TestBytes.hashed("v1"), recovered.get(TestBytes.hashed("k1")).value);
        assertEquals(TestBytes.hashed("v2"), recovered.get(TestBytes.hashed("k2")).value);
    }

    @Test
    @DisplayName("Recover from snapshot plus WAL tail")
    void recoverFromSnapshotPlusWalTail() {
        final InMemoryWal wal = new InMemoryWal();

        final LocalStore original = new LocalStore(wal);
        original.writeAccept(TestBytes.hashed("k1"), new Ballot(1, N1), TestBytes.hashed("v1"), false);

        final LocalStore.ThrottledSnapshot snap = original.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }

        original.writeAccept(TestBytes.hashed("k2"), new Ballot(2, N1), TestBytes.hashed("v2"), false);

        final LocalStore recovered = new LocalStore(wal);
        drainLoader(recovered.beginRecovery());

        assertEquals(TestBytes.hashed("v1"), recovered.get(TestBytes.hashed("k1")).value);
        assertEquals(TestBytes.hashed("v2"), recovered.get(TestBytes.hashed("k2")).value);
    }

    @Test
    @DisplayName("WAL tail overrides snapshot state when the ballot is higher")
    void walTailOverridesSnapshotState() {
        final InMemoryWal wal = new InMemoryWal();

        final LocalStore original = new LocalStore(wal);
        original.writeAccept(TestBytes.hashed("k"), new Ballot(2, N1), TestBytes.hashed("old"), false);

        final LocalStore.ThrottledSnapshot snap = original.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }

        original.writeAccept(TestBytes.hashed("k"), new Ballot(5, N1), TestBytes.hashed("new"), false);

        final LocalStore recovered = new LocalStore(wal);
        drainLoader(recovered.beginRecovery());

        assertEquals(new Ballot(5, N1), recovered.get(TestBytes.hashed("k")).accepted);
        assertEquals(TestBytes.hashed("new"), recovered.get(TestBytes.hashed("k")).value);
    }

    @Test
    @DisplayName("BallotBump entry recovered from the WAL")
    void ballotBumpRecoveredFromWal() {
        final InMemoryWal wal = new InMemoryWal();
        wal.append(new Wal.Entry.BallotBump(500));

        final LocalStore store = new LocalStore(wal);
        drainLoader(store.beginRecovery());

        assertEquals(500, store.reservedProposerBallot());
    }

    @Test
    @DisplayName("Reserved-ballot recovered from snapshot footer")
    void ballotBumpRecoveredFromSnapshot() {
        final InMemoryWal wal = new InMemoryWal();

        final LocalStore original = new LocalStore(wal);
        original.reserveProposerBallot(200);
        original.writeAccept(TestBytes.hashed("k"), new Ballot(1, N1), TestBytes.hashed("v"), false);

        final LocalStore.ThrottledSnapshot snap = original.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }

        final LocalStore recovered = new LocalStore(wal);
        drainLoader(recovered.beginRecovery());

        assertTrue(recovered.reservedProposerBallot() >= 200);
    }

    @Test
    @DisplayName("ThrottledLoader processes recovery in batches")
    void throttledLoaderBatchesWork() {
        final InMemoryWal wal = new InMemoryWal();

        final LocalStore original = new LocalStore(wal);
        for (int i = 0; i < 25; i++) {
            original.writeAccept(TestBytes.hashed("k" + i), new Ballot(i + 1, N1), TestBytes.hashed("v" + i), false);
        }
        final LocalStore.ThrottledSnapshot snap = original.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }

        // Recover: snapshot has 25 entries; loading batch of 10 should return true (more work)
        final LocalStore recovered = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = recovered.beginRecovery();
        assertEquals(25, loader.snapshotSize());

        assertTrue(loader.loadBatch(10), "First batch should indicate more work");
        assertFalse(loader.isDone());

        while (!loader.isDone()) {
            loader.loadBatch(10);
        }
        assertEquals(25, recovered.keyCount());
    }
}
