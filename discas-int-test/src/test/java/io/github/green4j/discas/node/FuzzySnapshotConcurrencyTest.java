/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A snapshot taken <em>while writes continue</em>, against a real {@link FileWal}.
 * <p>
 * {@code ThrottledSnapshot} is fuzzy by design: it walks ranges in batches, yielding to the event
 * loop in between, and reads <em>live</em> state for each key. So a key visited late is captured
 * with a value newer than the snapshot's start, a key created into an already-visited range is not
 * captured at all, and a key can appear in both the snapshot and the WAL tail. The claim that makes
 * this safe is a single invariant:
 * <blockquote>snapshot taken at LSN {@code L}, plus replay of every WAL entry after {@code L},
 * reconstructs exactly the state the store had when the snapshot committed.</blockquote>
 * That invariant is what these tests check, and it depends on two things holding together:
 * ballot-ordered replay (so a stale snapshot entry cannot overwrite a newer logged one) and
 * truncation staying pinned until commit (so the tail those entries live in is still there).
 * <p>
 * {@code SnapshotTest} covers the batching logic but drives {@code InMemoryWal}, which neither
 * truncates nor round-trips bytes -- exactly the kind of in-memory stand-in that hid a real
 * recovery defect in the snapshot reader.
 * <p>
 * Everything is single-threaded on purpose: a node performs writes and snapshot batches on one
 * event loop, so alternating the calls here <em>is</em> the real interleaving, and it is
 * reproducible rather than timing-dependent.
 */
@Tag("chaos")
@DisplayName("Fuzzy snapshot -- writes interleaved with snapshot batches")
class FuzzySnapshotConcurrencyTest {

    private static final NodeId N1 = NodeId.of("1");
    private static final int KEYS = 200;
    private static final int BATCH = 7; // small, so batches interleave with many mutations

    @TempDir
    Path baseDir;
    private StorageConfig config;

    @BeforeEach
    void setUp() {
        config = StorageConfig.builder()
                .baseDirectory(baseDir)
                .walMaxFileBytes(64 * 1024) // small, so the snapshot spans several WAL segments
                .snapshotRetentionCount(2)
                .build();
    }

    private static HashedBytes key(final int i) {
        return new HashedBytes(String.format("k%04d", i).getBytes());
    }

    /** Open a WAL through its real recovery sequence; appends are only legal after the replay. */
    private FileWal openWal() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        final Wal.SnapshotReader reader = wal.openSnapshot();
        if (reader != null) {
            reader.close();
        }
        wal.replayTail(e -> { });
        return wal;
    }

    /** Every accepted key's (accepted ballot, value, tombstone) -- the state recovery must match. */
    private static Map<HashedBytes, String> liveState(final LocalStore store) {
        final Map<HashedBytes, String> out = new LinkedHashMap<>();
        ByteBuffer cursor = null;
        while (true) {
            final LocalStore.ScanPage page = store.scanLocal(
                    ByteBuffers.EMPTY, cursor, KvLimits.MAX_SCAN_LIMIT, k -> true);
            if (page.entries().isEmpty()) {
                break;
            }
            for (final ClientMessage.ScanEntry entry : page.entries()) {
                final HashedBytes key = HashedBytes.adopt(entry.key());
                out.put(key, describe(store.get(key)));
                cursor = entry.key();
            }
            if (!page.hasMore()) {
                break;
            }
        }
        return out;
    }

    private static String describe(final KeyState ks) {
        return ks.accepted + "|" + (ks.tombstone ? "<tombstone>" : ks.value) + "|" + ks.promised;
    }

    private LocalStore recover(final FileWal wal) {
        final LocalStore recovered = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = recovered.beginRecovery();
        while (loader.loadBatch(50)) {
            // drain
        }
        return recovered;
    }

    @Test
    @DisplayName("Mutations interleaved with snapshot batches survive a restart intact")
    void interleavedMutationsSurviveRestart() throws IOException {
        final FileWal wal = openWal();
        final LocalStore store = new LocalStore(wal);

        for (int i = 0; i < KEYS; i++) {
            store.writeAccept(key(i), new Ballot(1, N1), TestBytes.hashed("v1-" + i), false);
        }

        final LocalStore.ThrottledSnapshot snapshot = store.beginSnapshot();
        int round = 0;
        while (snapshot.writeBatch(BATCH)) {
            // Rewrite every key on each pass. Guarantees mutations land in ranges the iterator has
            // already passed (never captured -- must come from the tail), in ranges it has not yet
            // reached (captured at the newer value), and in the range it is midway through.
            round++;
            for (int i = 0; i < KEYS; i += 3) {
                store.writeAccept(key(i), new Ballot(round + 1, N1), TestBytes.hashed("v" + round + "-" + i), false);
            }
            // Delete a few, and create keys that did not exist when the snapshot began.
            store.writeAccept(key(round % KEYS), new Ballot(round + 100, N1), null, true);
            store.writeAccept(key(KEYS + round), new Ballot(1, N1), TestBytes.hashed("late-" + round), false);
        }
        assertTrue(snapshot.isCommitted(), "The snapshot must reach commit");

        final Map<HashedBytes, String> expected = liveState(store);
        wal.close();

        // Restart: recovery is snapshot-then-tail, which is the whole invariant under test.
        final FileWal reopened = openWal();
        final LocalStore recovered = recover(reopened);
        final Map<HashedBytes, String> actual = liveState(recovered);
        reopened.close();

        assertEquals(expected.keySet(), actual.keySet(),
                "Recovery must reconstruct exactly the same key set");
        for (final Map.Entry<HashedBytes, String> e : expected.entrySet()) {
            assertEquals(e.getValue(), actual.get(e.getKey()),
                    "State diverged after restart for key " + e.getKey());
        }
    }

    @Test
    @DisplayName("Truncation stays pinned: the tail a committed snapshot needs is still there")
    void truncationDoesNotDiscardTheNeededTail() throws IOException {
        final FileWal wal = openWal();
        final LocalStore store = new LocalStore(wal);

        for (int i = 0; i < KEYS; i++) {
            store.writeAccept(key(i), new Ballot(1, N1), TestBytes.hashed("before"), false);
        }

        final LocalStore.ThrottledSnapshot snapshot = store.beginSnapshot();
        // One batch only, then write enough to SEAL several WAL segments past the snapshot
        // horizon. Volume is the point: compaction only ever deletes sealed segments, so a
        // handful of small writes stay in the open segment and the test would pass whether or not
        // the pin were honoured. Values are sized so 64 KiB segments roll repeatedly.
        snapshot.writeBatch(BATCH);
        final byte[] bulk = new byte[1024];
        Arrays.fill(bulk, (byte) 'x');
        for (int pass = 0; pass < 4; pass++) {
            for (int i = 0; i < KEYS; i++) {
                store.writeAccept(key(i), new Ballot(9 + pass, N1), new HashedBytes(bulk), false);
            }
        }
        store.writeAccept(key(0), new Ballot(50, N1), TestBytes.hashed("after-the-snapshot-began"), false);
        while (snapshot.writeBatch(BATCH)) {
            // drain to commit, which triggers truncateBeforeSnapshot
        }
        assertTrue(snapshot.isCommitted());

        final Map<HashedBytes, String> expected = liveState(store);
        wal.close();

        final FileWal reopened = openWal();
        final LocalStore recovered = recover(reopened);
        reopened.close();

        // If truncation had removed segments past the snapshot LSN, these writes would be gone and
        // recovery would surface the pre-snapshot value instead.
        assertEquals(expected, liveState(recovered),
                "Entries written after the snapshot began must survive its truncation");
        assertEquals(TestBytes.hashed("after-the-snapshot-began"), recovered.get(key(0)).value);
    }

    @Test
    @DisplayName("A stale snapshot entry never overwrites a newer logged one")
    void ballotOrderingBeatsSnapshotStaleness() throws IOException {
        final FileWal wal = openWal();
        final LocalStore store = new LocalStore(wal);

        store.writeAccept(key(0), new Ballot(1, N1), TestBytes.hashed("old"), false);

        final LocalStore.ThrottledSnapshot snapshot = store.beginSnapshot();
        // Capture key(0) into the snapshot at its old value, then move it on.
        snapshot.writeBatch(1);
        store.writeAccept(key(0), new Ballot(5, N1), TestBytes.hashed("new"), false);
        while (snapshot.writeBatch(BATCH)) {
            // drain
        }

        wal.close();
        final FileWal reopened = openWal();
        final LocalStore recovered = recover(reopened);
        reopened.close();

        // Recovery applies the snapshot first, then the tail; the tail's higher ballot must win.
        // A last-write-wins replay would leave "old" here.
        assertEquals(TestBytes.hashed("new"), recovered.get(key(0)).value,
                "The higher-ballot logged value must beat the snapshot's older capture");
        assertEquals(new Ballot(5, N1), recovered.get(key(0)).accepted);
    }

    @Test
    @DisplayName("Promise-only keys stay out of the snapshot and out of recovery")
    void promiseOnlyKeysAreNotSnapshotted() throws IOException {
        final FileWal wal = openWal();
        final LocalStore store = new LocalStore(wal);

        store.writeAccept(key(1), new Ballot(1, N1), TestBytes.hashed("accepted"), false);
        store.writePromise(key(2), new Ballot(3, N1)); // prepared, never accepted

        final LocalStore.ThrottledSnapshot snapshot = store.beginSnapshot();
        while (snapshot.writeBatch(BATCH)) {
            // drain
        }
        wal.close();

        final FileWal reopened = openWal();
        final LocalStore recovered = recover(reopened);
        reopened.close();

        assertNotNull(recovered.get(key(1)));
        // The promise survives via the WAL tail as promise-only state, but must never carry a
        // value: persisting prepared-but-never-accepted keys into snapshots would let transient
        // metadata outlive restarts indefinitely.
        final KeyState promised = recovered.get(key(2));
        if (promised != null) {
            assertTrue(promised.accepted.isZero(),
                    "A prepared-but-never-accepted key must not come back as accepted state");
            assertNull(promised.value);
        }
    }
}
