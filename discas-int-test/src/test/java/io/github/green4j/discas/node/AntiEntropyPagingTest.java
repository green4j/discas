/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Anti-entropy compares a range one bounded page at a time. Carrying every key digest in one
 * {@code KeysResp} would let a dense range exceed the peer transport's frame and inflight budgets
 * and have the connection torn down by its own backpressure.
 */
@DisplayName("Anti-entropy -- paged range comparison")
class AntiEntropyPagingTest {

    private static final NodeId N1 = NodeId.of("1");

    private InMemoryWal wal;
    private LocalStore store;

    @BeforeEach
    void setUp() {
        wal = new InMemoryWal();
        store = new LocalStore(wal);
    }

    /** Keys that land in {@code range}, in ascending order, after writing {@code count} keys. */
    private List<HashedBytes> seedRange(final int count) {
        final List<HashedBytes> written = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final HashedBytes k = TestBytes.hashed(String.format("k%04d", i));
            store.writeAccept(k, new Ballot(1, N1), TestBytes.hashed("v"), false);
            written.add(k);
        }
        return written;
    }

    @Test
    @DisplayName("A range page is ordered, bounded by limit, and reports hasMore exactly")
    void pageIsOrderedAndBounded() {
        seedRange(64);
        // Whichever range this key falls in, page through just that range.
        final int range = TestBytes.hashed("k0000").rangeOf(LocalStore.NUM_RANGES);

        final LocalStore.KeyDigestPage all = store.keyDigestsForRange(range, null, 1000);
        final int total = all.digests().size();
        assertFalse(all.hasMore(), "The whole range fits well inside the limit");
        assertTrue(total >= 1, "Expected at least one key in this range");

        // Ascending order is what makes the cursor meaningful.
        for (int i = 1; i < total; i++) {
            assertTrue(all.digests().get(i - 1).key().compareTo(all.digests().get(i).key()) < 0,
                    "Range page must be in ascending key order");
        }

        final LocalStore.KeyDigestPage first = store.keyDigestsForRange(range, null, 1);
        assertEquals(1, first.digests().size());
        assertEquals(total > 1, first.hasMore(),
                "hasMore must reflect whether the range actually holds more");
    }

    @Test
    @DisplayName("Paging the whole range yields every key exactly once, in order")
    void pagingCoversTheRangeExactlyOnce() {
        seedRange(200);
        final int range = TestBytes.hashed("k0000").rangeOf(LocalStore.NUM_RANGES);

        final List<String> expected = new ArrayList<>();
        for (final KeyDigest d : store.keyDigestsForRange(range, null, 1000).digests()) {
            expected.add(TestBytes.string(d.key()));
        }

        final List<String> seen = new ArrayList<>();
        ByteBuffer cursor = null;
        int guard = 0;
        while (true) {
            final LocalStore.KeyDigestPage page = store.keyDigestsForRange(range, cursor, 2);
            for (final KeyDigest d : page.digests()) {
                seen.add(TestBytes.string(d.key()));
            }
            if (!page.hasMore()) {
                break;
            }
            cursor = page.digests().get(page.digests().size() - 1).key().toBuffer();
            assertTrue(++guard < 10_000, "Paging did not terminate");
        }

        assertEquals(expected, seen,
                "A gap means the cursor skipped keys; a duplicate means it is not exclusive");
    }

    @Test
    @DisplayName("A range cursor is read from its position, not from index zero")
    void rangeCursorHonoursBufferPosition() {
        final List<HashedBytes> written = seedRange(200);
        final int range = TestBytes.hashed("k0000").rangeOf(LocalStore.NUM_RANGES);
        final HashedBytes firstInRange = store.keyDigestsForRange(range, null, 1).digests().get(0).key();

        // A cursor arriving off the wire is a slice: its content starts at the buffer's position.
        final ByteBuffer padded = ByteBuffer.allocate(4 + firstInRange.size());
        padded.put(new byte[] {9, 9, 9, 9}).put(firstInRange.toBuffer()).flip().position(4);

        final LocalStore.KeyDigestPage page = store.keyDigestsForRange(range, padded, 1000);

        assertFalse(page.digests().isEmpty(), "The range holds more than one key");
        assertTrue(page.digests().get(0).key().compareTo(firstInRange) > 0,
                "The cursor is exclusive, so the page must start strictly after it");
        assertEquals(4, padded.position(), "The cursor must not be consumed");
        assertFalse(written.isEmpty());
    }

    @Test
    @DisplayName("A peer's requested limit is clamped to the configured cap")
    void limitIsClamped() {
        seedRange(64);
        final int range = TestBytes.hashed("k0000").rangeOf(LocalStore.NUM_RANGES);

        // A peer asking for more than the cap must not be able to force an oversized message.
        final LocalStore.KeyDigestPage page =
                store.keyDigestsForRange(range, null, Integer.MAX_VALUE);
        assertTrue(page.digests().size() <= KvLimits.MAX_ANTI_ENTROPY_KEYS_PER_PAGE);
    }

    private static PeerMessage.KeysResp peerPage(final boolean hasMore, final String... keys) {
        final List<KeyDigest> digests = new ArrayList<>();
        for (final String k : keys) {
            digests.add(new KeyDigest(TestBytes.hashed(k), new Ballot(1, N1), HashedBytes.EMPTY, false));
        }
        return new PeerMessage.KeysResp(N1, 1L, 0, digests, hasMore);
    }

    private static LocalStore.KeyDigestPage localPage(final boolean hasMore, final String... keys) {
        final List<KeyDigest> digests = new ArrayList<>();
        for (final String k : keys) {
            digests.add(new KeyDigest(TestBytes.hashed(k), new Ballot(1, N1), HashedBytes.EMPTY, false));
        }
        return new LocalStore.KeyDigestPage(digests, hasMore);
    }

    @Test
    @DisplayName("Everyone exhausted -> no bound, the range is finished")
    void noBoundWhenAllExhausted() {
        assertNull(AntiEntropy.pageTrustBound(
                localPage(false, "a", "b"), List.of(peerPage(false, "a", "c"))));
    }

    @Test
    @DisplayName("Bound is the smallest last-key among truncated responders")
    void boundIsSmallestLastKey() {
        // The peer stopped at "c" with more to come; trusting "m" would treat everything the peer
        // holds between "c" and "m" as locally-only and fire a repair round per key for nothing.
        assertEquals(TestBytes.utf8("c"), AntiEntropy.pageTrustBound(
                localPage(true, "a", "m"), List.of(peerPage(true, "a", "c"))));
    }

    @Test
    @DisplayName("The local page counts as a responder")
    void localPageBoundsTheComparison() {
        // Local cut at "c"; the peer is complete. Without counting the local page, every peer key
        // above "c" would look like it was missing locally.
        assertEquals(TestBytes.utf8("c"), AntiEntropy.pageTrustBound(
                localPage(true, "a", "c"), List.of(peerPage(false, "a", "m"))));
    }

    @Test
    @DisplayName("An exhausted or empty responder imposes no bound")
    void exhaustedOrEmptyImposesNoBound() {
        assertEquals(TestBytes.utf8("m"), AntiEntropy.pageTrustBound(
                localPage(false, "a", "b"), List.of(peerPage(true), peerPage(true, "a", "m"))));
    }

    @Test
    @DisplayName("The paging fields survive the peer wire")
    void pagingFieldsRoundTrip() {
        final PeerMessage.KeysReq req = (PeerMessage.KeysReq) PeerMessageCodec.decode(
                PeerMessageCodec.encode(new PeerMessage.KeysReq(N1, 7L, 3, TestBytes.utf8("cursor"), 64)));
        assertEquals(3, req.range());
        assertEquals(TestBytes.utf8("cursor"), req.startAfter());
        assertEquals(64, req.limit());

        final PeerMessage.KeysReq firstPage = (PeerMessage.KeysReq) PeerMessageCodec.decode(
                PeerMessageCodec.encode(new PeerMessage.KeysReq(N1, 7L, 3, null, 64)));
        assertNull(firstPage.startAfter(), "A null cursor must stay null across the wire");

        final PeerMessage.KeysResp resp = (PeerMessage.KeysResp) PeerMessageCodec.decode(
                PeerMessageCodec.encode(peerPage(true, "a", "b")));
        assertTrue(resp.hasMore());
        assertEquals(2, resp.digests().size());
        assertEquals(TestBytes.hashed("a"), resp.digests().get(0).key());
    }
}
