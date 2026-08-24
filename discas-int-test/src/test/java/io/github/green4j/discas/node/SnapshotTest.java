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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Snapshot -- fuzzy snapshot write + recovery")
class SnapshotTest {

    private static final NodeId N1 = NodeId.of("1");

    private InMemoryWal wal;
    private LocalStore store;

    @BeforeEach
    void setUp() {
        wal = new InMemoryWal();
        store = new LocalStore(wal);
    }

    @Test
    @DisplayName("Snapshot captures every accepted key, including tombstones")
    void snapshotCapturesAllAcceptedKeys() {
        store.writeAccept(TestBytes.hashed("k1"), new Ballot(1, N1), TestBytes.hashed("v1"), false);
        store.writeAccept(TestBytes.hashed("k2"), new Ballot(2, N1), TestBytes.hashed("v2"), false);
        store.writeAccept(TestBytes.hashed("k3"), new Ballot(3, N1), null, true);

        final LocalStore.ThrottledSnapshot snap = store.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }
        assertTrue(snap.isCommitted());

        final LocalStore recovered = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = recovered.beginRecovery();
        while (!loader.isDone()) {
            loader.loadBatch(1000);
        }

        assertNotNull(recovered.get(TestBytes.hashed("k1")));
        assertEquals(TestBytes.hashed("v1"), recovered.get(TestBytes.hashed("k1")).value);
        assertNotNull(recovered.get(TestBytes.hashed("k2")));
        assertNotNull(recovered.get(TestBytes.hashed("k3")));
        assertTrue(recovered.get(TestBytes.hashed("k3")).tombstone);
    }

    @Test
    @DisplayName("Snapshot excludes promise-only keys (never accepted)")
    void snapshotExcludesPromiseOnlyKeys() {
        store.writePromise(TestBytes.hashed("promise-only"), new Ballot(1, N1));
        store.writeAccept(TestBytes.hashed("accepted"), new Ballot(2, N1), TestBytes.hashed("v"), false);

        final LocalStore.ThrottledSnapshot snap = store.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }

        final LocalStore recovered = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = recovered.beginRecovery();
        while (!loader.isDone()) {
            loader.loadBatch(1000);
        }

        assertNull(recovered.get(TestBytes.hashed("promise-only")));
        assertNotNull(recovered.get(TestBytes.hashed("accepted")));
    }

    @Test
    @DisplayName("Snapshot writes in batches and commits when drained")
    void snapshotWritesBatchedAndCommits() {
        for (int i = 0; i < 20; i++) {
            store.writeAccept(TestBytes.hashed("k" + i), new Ballot(i + 1, N1), TestBytes.hashed("v" + i), false);
        }

        final LocalStore.ThrottledSnapshot snap = store.beginSnapshot();

        int batches = 0;
        while (snap.writeBatch(5)) {
            batches++;
        }
        assertTrue(batches > 0, "Should require multiple batches");
        assertTrue(snap.isCommitted());
    }

    @Test
    @DisplayName("Snapshot abort before commit cleans up; not committed")
    void snapshotAbortCleansUp() {
        store.writeAccept(TestBytes.hashed("k"), new Ballot(1, N1), TestBytes.hashed("v"), false);

        final LocalStore.ThrottledSnapshot snap = store.beginSnapshot();
        snap.abort();

        assertFalse(snap.isCommitted());

        final LocalStore.ThrottledSnapshot snap2 = store.beginSnapshot();
        while (snap2.writeBatch(100)) {
            // keep writing
        }
        assertTrue(snap2.isCommitted());
    }

    @Test
    @DisplayName("Snapshot preserves reserved proposer ballot for recovery")
    void snapshotPreservesReservedBallot() {
        store.reserveProposerBallot(500);
        store.writeAccept(TestBytes.hashed("k"), new Ballot(1, N1), TestBytes.hashed("v"), false);

        final LocalStore.ThrottledSnapshot snap = store.beginSnapshot();
        while (snap.writeBatch(100)) {
            // keep writing
        }

        final LocalStore recovered = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = recovered.beginRecovery();
        while (!loader.isDone()) {
            loader.loadBatch(1000);
        }

        assertTrue(recovered.reservedProposerBallot() >= 500);
    }
}
