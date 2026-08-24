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
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("LocalStore -- in-memory state behind the WAL")
class LocalStoreTest {

    private static final NodeId N1 = NodeId.of("1");

    private InMemoryWal wal;
    private LocalStore store;

    @BeforeEach
    void setUp() {
        wal = new InMemoryWal();
        store = new LocalStore(wal);
    }

    @Test
    @DisplayName("writePromise creates KeyState with the promised ballot")
    void writePromiseCreatesKeyState() {
        final HashedBytes k = TestBytes.hashed("k");
        final Ballot b = new Ballot(1, N1);

        store.writePromise(k, b);

        final KeyState ks = store.get(k);
        assertNotNull(ks);
        assertEquals(b, ks.promised);
        assertTrue(ks.accepted.isZero());
    }

    @Test
    @DisplayName("writePromise ignores a lower ballot than already promised")
    void writePromiseIgnoresLowerBallot() {
        final HashedBytes k = TestBytes.hashed("k");
        final Ballot high = new Ballot(5, N1);
        final Ballot low = new Ballot(3, N1);

        store.writePromise(k, high);
        store.writePromise(k, low);

        assertEquals(high, store.get(k).promised);
    }

    @Test
    @DisplayName("writeAccept materialises KeyState with the value")
    void writeAcceptCreatesKeyStateWithValue() {
        final HashedBytes k = TestBytes.hashed("k");
        final HashedBytes v = TestBytes.hashed("v");
        final Ballot b = new Ballot(1, N1);

        store.writeAccept(k, b, v, false);

        final KeyState ks = store.get(k);
        assertEquals(b, ks.accepted);
        assertEquals(v, ks.value);
        assertFalse(ks.tombstone);
    }

    @Test
    @DisplayName("writeAccept ignores a lower ballot than already accepted")
    void writeAcceptIgnoresLowerBallot() {
        final HashedBytes k = TestBytes.hashed("k");
        final Ballot high = new Ballot(5, N1);
        final Ballot low = new Ballot(3, N1);

        store.writeAccept(k, high, TestBytes.hashed("v1"), false);
        store.writeAccept(k, low, TestBytes.hashed("v2"), false);

        assertEquals(high, store.get(k).accepted);
        assertEquals(TestBytes.hashed("v1"), store.get(k).value);
    }

    @Test
    @DisplayName("reserveProposerBallot persists via the WAL (BallotBump entry)")
    void reserveProposerBallotPersistsViaWal() {
        store.reserveProposerBallot(100);

        assertEquals(100, store.reservedProposerBallot());
        assertEquals(100, wal.reservedProposerBallot());

        final boolean hasBallotBump = wal.entries().stream()
                .anyMatch(e -> e instanceof Wal.Entry.BallotBump
                        && ((Wal.Entry.BallotBump) e).reservedUpTo() == 100);
        assertTrue(hasBallotBump);
    }

    @Test
    @DisplayName("evictPromiseOnly removes stale promise-only KeyStates")
    void evictPromiseOnlyRemovesStalePromises() throws Exception {
        final HashedBytes k = TestBytes.hashed("stale");
        store.writePromise(k, new Ballot(1, N1));

        // Age the promise past the eviction threshold.
        final KeyState ks = store.get(k);
        ks.promisedAtNanos = System.nanoTime() - LocalStore.MIN_PROMISE_AGE_NANOS - 1;

        final int evicted = store.evictPromiseOnly();
        assertEquals(1, evicted);
        assertNull(store.get(k));
    }

    @Test
    @DisplayName("evictPromiseOnly keeps already-accepted keys regardless of age")
    void evictPromiseOnlyKeepsAcceptedKeys() throws Exception {
        final HashedBytes k = TestBytes.hashed("accepted");
        store.writeAccept(k, new Ballot(1, N1), TestBytes.hashed("v"), false);

        // Even if promisedAtNanos is old, accepted keys should not be evicted
        final KeyState ks = store.get(k);
        ks.promisedAtNanos = System.nanoTime() - LocalStore.MIN_PROMISE_AGE_NANOS - 1;

        final int evicted = store.evictPromiseOnly();
        assertEquals(0, evicted);
        assertNotNull(store.get(k));
    }

    @Test
    @DisplayName("Range digests are recomputed only for ranges marked dirty")
    void rangeDigestsRecomputeOnlyDirtyRanges() {
        final HashedBytes k1 = TestBytes.hashed("k1");
        store.writeAccept(k1, new Ballot(1, N1), TestBytes.hashed("v1"), false);

        final Map<Integer, HashedBytes> digests1 = store.computeRangeDigests();
        assertFalse(digests1.isEmpty());

        // Calling again without changes should return same digests
        final Map<Integer, HashedBytes> digests2 = store.computeRangeDigests();
        assertEquals(digests1, digests2);

        // Write to a different key (possibly different range)
        store.writeAccept(TestBytes.hashed("k2"), new Ballot(2, N1), TestBytes.hashed("v2"), false);
        final Map<Integer, HashedBytes> digests3 = store.computeRangeDigests();
        assertNotNull(digests3);
    }

    @Test
    @DisplayName("keyDigestsForRange excludes promise-only KeyStates")
    void keyDigestsForRangeExcludesPromiseOnly() {
        final HashedBytes k = TestBytes.hashed("promise-only");
        store.writePromise(k, new Ballot(1, N1));

        final int range = k.rangeOf(LocalStore.NUM_RANGES);
        final List<KeyDigest> digests =
                store.keyDigestsForRange(range, null, KvLimits.MAX_ANTI_ENTROPY_KEYS_PER_PAGE).digests();

        final boolean containsKey = digests.stream()
                .anyMatch(d -> d.key().equals(k));
        assertFalse(containsKey, "Promise-only keys should not appear in digests");
    }

    /** Whole-keyspace page, mirroring the pre-pagination scanLocal() the tests were written against. */
    private List<ClientMessage.ScanEntry> scanAll() {
        return store.scanLocal(ByteBuffers.EMPTY, null, KvLimits.MAX_SCAN_LIMIT, k -> true).entries();
    }

    @Test
    @DisplayName("scanLocal excludes promise-only KeyStates")
    void scanLocalExcludesPromiseOnly() {
        store.writePromise(TestBytes.hashed("promise-only"), new Ballot(1, N1));
        store.writeAccept(TestBytes.hashed("accepted"), new Ballot(2, N1), TestBytes.hashed("v"), false);

        final List<ClientMessage.ScanEntry> entries = scanAll();
        assertEquals(1, entries.size());
        assertEquals(TestBytes.utf8("accepted"), entries.get(0).key());
    }

    @Test
    @DisplayName("scanLocal includes tombstones (filtered out at scan-merge layer)")
    void scanLocalIncludesTombstones() {
        store.writeAccept(TestBytes.hashed("dead"), new Ballot(1, N1), null, true);

        final List<ClientMessage.ScanEntry> entries = scanAll();
        assertEquals(1, entries.size());
        assertTrue(entries.get(0).tombstone());
    }

    @Test
    @DisplayName("Recently-touched tracking flags writes and clears on demand")
    void recentlyTouchedTracking() {
        final HashedBytes k = TestBytes.hashed("k");
        assertFalse(store.wasTouchedRecently(k));

        store.writeAccept(k, new Ballot(1, N1), TestBytes.hashed("v"), false);
        assertTrue(store.wasTouchedRecently(k));

        store.clearRecentlyTouched();
        assertFalse(store.wasTouchedRecently(k));
    }

    private void accept(final String k) {
        store.writeAccept(TestBytes.hashed(k), new Ballot(1, N1), TestBytes.hashed("v"), false);
    }

    private static List<String> keysOf(final LocalStore.ScanPage page) {
        final List<String> out = new ArrayList<>();
        for (final ClientMessage.ScanEntry e : page.entries()) {
            out.add(TestBytes.string(e.key()));
        }
        return out;
    }

    @Test
    @DisplayName("scanLocal returns keys in ascending byte order")
    void scanLocalIsOrdered() {
        accept("c");
        accept("a");
        accept("b");

        assertEquals(List.of("a", "b", "c"), keysOf(
                store.scanLocal(ByteBuffers.EMPTY, null, 10, k -> true)));
    }

    @Test
    @DisplayName("scanLocal pages with the cursor and reports hasMore exactly")
    void scanLocalPages() {
        accept("a");
        accept("b");
        accept("c");

        final LocalStore.ScanPage first = store.scanLocal(ByteBuffers.EMPTY, null, 2, k -> true);
        assertEquals(List.of("a", "b"), keysOf(first));
        assertTrue(first.hasMore(), "A third key remains");

        final LocalStore.ScanPage second = store.scanLocal(ByteBuffers.EMPTY, TestBytes.utf8("b"), 2, k -> true);
        assertEquals(List.of("c"), keysOf(second));
        assertFalse(second.hasMore(), "The key set is exhausted");
    }

    @Test
    @DisplayName("A page that exactly consumes the key set reports no more")
    void exactPageIsNotHasMore() {
        accept("a");
        accept("b");

        final LocalStore.ScanPage page = store.scanLocal(ByteBuffers.EMPTY, null, 2, k -> true);
        assertEquals(List.of("a", "b"), keysOf(page));
        assertFalse(page.hasMore(),
                "Limit reached with nothing left must not claim another page exists");
    }

    @Test
    @DisplayName("scanLocal restricts to the prefix and stops at its upper edge")
    void scanLocalHonoursPrefix() {
        accept("app/1");
        accept("app/2");
        accept("zzz");

        final LocalStore.ScanPage page = store.scanLocal(TestBytes.utf8("app/"), null, 10, k -> true);
        assertEquals(List.of("app/1", "app/2"), keysOf(page));
        assertFalse(page.hasMore());
    }

    @Test
    @DisplayName("A cursor sorting before the prefix still returns the prefix range")
    void cursorBeforePrefixDoesNotTruncate() {
        accept("app/1");
        accept("app/2");

        // "aaa" < "app/": a naive tailSet(cursor) would start on non-matching keys and the
        // ascending-order break would report the range as empty.
        final LocalStore.ScanPage page = store.scanLocal(TestBytes.utf8("app/"), TestBytes.utf8("aaa"), 10, k -> true);
        assertEquals(List.of("app/1", "app/2"), keysOf(page));
    }

    @Test
    @DisplayName("A prefix and a cursor are read from their position, not from index zero")
    void scanBoundsHonourBufferPosition() {
        accept("app/1");
        accept("app/2");
        accept("app/3");

        // What a cursor sliced out of a frame looks like: content starting partway in. A seek
        // bound built from index zero rather than remaining() would silently use the padding.
        final ByteBuffer prefix = TestBytes.utf8("XXXXapp/");
        prefix.position(4);
        final ByteBuffer cursor = TestBytes.utf8("XXXXapp/1");
        cursor.position(4);

        final LocalStore.ScanPage page = store.scanLocal(prefix, cursor, 10, k -> true);

        assertEquals(List.of("app/2", "app/3"), keysOf(page));
        assertEquals(4, prefix.position(), "The bounds must not be consumed");
        assertEquals(4, cursor.position());
    }

    @Test
    @DisplayName("The accept filter is applied while filling, so hasMore stays exact")
    void filterAppliedDuringFill() {
        accept("a");
        accept("deny-1");
        accept("deny-2");
        accept("b");

        // Filtering after the fact would yield {a} from the first two keys and look exhausted.
        final LocalStore.ScanPage page = store.scanLocal(
                ByteBuffers.EMPTY, null, 2, k -> !TestBytes.string(k).startsWith("deny-"));
        assertEquals(List.of("a", "b"), keysOf(page));
        assertFalse(page.hasMore());
    }

    @Test
    @DisplayName("Promise-only keys are skipped without consuming page budget")
    void promiseOnlyKeysAreNotPagedOver() {
        store.writePromise(TestBytes.hashed("a-promise"), new Ballot(1, N1));
        accept("b");
        accept("c");

        final LocalStore.ScanPage page = store.scanLocal(ByteBuffers.EMPTY, null, 2, k -> true);
        assertEquals(List.of("b", "c"), keysOf(page));
        assertFalse(page.hasMore());
    }
}
