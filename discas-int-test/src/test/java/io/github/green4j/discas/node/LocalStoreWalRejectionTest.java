/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * In-memory state must never reflect a write the log refused.
 * <p>
 * {@code FileWal.append} turns an {@code IOException} into a one-way degraded flag and returns
 * normally, so a store that applied the entry regardless would hold a promise or accept no replay
 * could reproduce. A degraded node still answers serializable reads, scans and anti-entropy
 * digests from memory, so the phantom would be observable -- and a lost ballot reservation lets
 * the proposer reissue a counter it had already handed out.
 */
@DisplayName("LocalStore -- a rejected WAL append must not mutate memory")
class LocalStoreWalRejectionTest {

    private static final NodeId N1 = NodeId.of("1");

    private RejectingWal wal;
    private LocalStore store;

    /**
     * Mirrors {@code FileWal}'s failure shape: an append that fails <em>degrades the log as a
     * result</em>. Crucially {@link #isDegraded()} stays false until that first failing append, so
     * the write path's opening {@code isDegraded()} check still passes and the entry really does
     * reach {@code append}. A fake that degraded up front would let that opening check short
     * circuit the write and would never exercise the guard under test.
     */
    private static final class RejectingWal implements Wal {
        private final IncarnationId incarnation = IncarnationId.generate();
        private final List<Entry> accepted = new ArrayList<>();
        private boolean failAppends;
        private boolean degraded;

        @Override
        public IncarnationId incarnation() {
            return incarnation;
        }

        /** Never asked here: these tests drive {@code LocalStore} directly, with no node around it. */
        @Override
        public boolean ceilingIsProven() {
            return true;
        }

        @Override
        public boolean append(final Entry entry) {
            if (degraded) {
                return false;
            }
            if (failAppends) {
                degraded = true; // the write failed, and the log degrades because of it
                return false;
            }
            accepted.add(entry);
            return true;
        }

        @Override
        public boolean isDegraded() {
            return degraded;
        }

        @Override
        public String degradedReason() {
            return degraded ? "test-induced" : null;
        }

        @Override
        public SnapshotReader openSnapshot() {
            return null;
        }

        @Override
        public void replayTail(final Consumer<Entry> consumer) {
        }

        @Override
        public SnapshotWriter beginSnapshot() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncateBeforeSnapshot() {
        }
    }

    @BeforeEach
    void setUp() {
        wal = new RejectingWal();
        store = new LocalStore(wal);
    }

    /**
     * Reject only from now on, without setting the degraded flag first. This is the exact shape of
     * the bug: the log fails mid-append and degrades as a result, so the guard cannot come from
     * the {@code isDegraded()} pre-check at the top of the write path -- it has to come from the
     * append's own verdict.
     */
    private void failNextAppends() {
        wal.failAppends = true;
    }

    @Test
    @DisplayName("A rejected promise leaves no KeyState behind")
    void rejectedPromiseIsNotApplied() {
        failNextAppends();
        store.writePromise(TestBytes.hashed("k"), new Ballot(5, N1));

        assertNull(store.get(TestBytes.hashed("k")),
                "A promise the log refused must not be visible in memory");
        assertTrue(wal.accepted.isEmpty());
    }

    @Test
    @DisplayName("A rejected accept leaves the previous value intact")
    void rejectedAcceptIsNotApplied() {
        store.writeAccept(TestBytes.hashed("k"), new Ballot(1, N1), TestBytes.hashed("durable"), false);
        assertEquals(TestBytes.hashed("durable"), store.get(TestBytes.hashed("k")).value);

        failNextAppends();
        store.writeAccept(TestBytes.hashed("k"), new Ballot(2, N1), TestBytes.hashed("phantom"), false);

        assertEquals(TestBytes.hashed("durable"), store.get(TestBytes.hashed("k")).value,
                "The refused accept must not overwrite the last durable value");
        assertEquals(new Ballot(1, N1), store.get(TestBytes.hashed("k")).accepted);
    }

    @Test
    @DisplayName("A rejected accept is not offered to scans or anti-entropy")
    void rejectedAcceptIsNotObservable() {
        failNextAppends();
        store.writeAccept(TestBytes.hashed("ghost"), new Ballot(1, N1), TestBytes.hashed("v"), false);

        // These are the two paths a degraded node still answers from memory.
        assertTrue(store.scanLocal(ByteBuffers.EMPTY, null, 10, k -> true).entries().isEmpty(),
                "A scan must not surface a key the log refused");
        final int range = TestBytes.hashed("ghost").rangeOf(LocalStore.NUM_RANGES);
        assertTrue(store.keyDigestsForRange(
                        range, null, KvLimits.MAX_ANTI_ENTROPY_KEYS_PER_PAGE).digests().isEmpty(),
                "An anti-entropy digest must not advertise a key the log refused");
    }

    @Test
    @DisplayName("A rejected ballot reservation is not believed")
    void rejectedBallotReservationIsNotApplied() {
        store.reserveProposerBallot(100);
        assertEquals(100, store.reservedProposerBallot());

        failNextAppends();
        store.reserveProposerBallot(5000);

        assertEquals(100, store.reservedProposerBallot(),
                "Believing an unlogged reservation lets the proposer reissue a used ballot "
                        + "counter after a restart");
    }
}
