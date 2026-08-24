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

/**
 * A promise is acknowledged to the proposer before the WAL is forced, so an unclean shutdown loses
 * up to {@code walForceInterval} of promises. A forgotten promise lets this acceptor accept a ballot
 * it already promised against, and two different values can then be chosen -- a **safety** failure,
 * because unlike a lost value a promise was never replicated and so no quorum can repair it.
 * <p>
 * These tests drive that fault directly: append, do not force, drop the tail, recover.
 */
@DisplayName("Acceptor -- a forgotten promise must not become an accepted ballot")
class PromiseDurabilityTest {

    private static final NodeId SELF = NodeId.of("1");
    private static final NodeId PEER = NodeId.of("9");
    private static final HashedBytes KEY = TestBytes.hashed("k");

    /** Mirrors Acceptor.CEILING_CHUNK; asserted below rather than assumed. */
    private static final long CHUNK = 1024;

    private static LocalStore recovered(final InMemoryWal wal) {
        final LocalStore store = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        while (loader.loadBatch(1024)) {
            // drain
        }
        return store;
    }

    @Test
    @DisplayName("A promise lost with the unforced tail still blocks its own ballot after recovery")
    void promiseSurvivesALostTail() {
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore before = recovered(wal);
        final Acceptor acceptor = new Acceptor(SELF, before);

        // Promise ballot 9. The ceiling record this triggers is forced; the Promise record itself is
        // not -- which is exactly the ordering the design relies on.
        final Ballot nine = new Ballot(9L, PEER);
        assertTrue(acceptor.handlePrepare(prepare(nine)).ok(), "The first promise should be given");

        // The crash. The Promise record is gone; the forced ceiling is not.
        wal.loseUnforcedTail();
        assertEquals(0, wal.countEntries(Wal.Entry.Promise.class),
                "Precondition: the promise was in the unforced tail and is now gone");
        assertEquals(1, wal.countEntries(Wal.Entry.PromiseCeiling.class),
                "Precondition: the ceiling was forced and survived");

        final LocalStore after = recovered(wal);
        final Acceptor restarted = new Acceptor(SELF, after);

        assertEquals(0L, after.getOrCreate(KEY).promised.counter(),
                "The per-key promise really is forgotten -- this is what used to be the whole story");
        assertEquals(9L + CHUNK - 1, after.recoveryPromiseFloor(),
                "The floor is the surviving ceiling, so it covers the forgotten promise");

        // The load-bearing assertion. Without the floor this prepare would be accepted, because
        // keyState.promised is Ballot.ZERO and 9 >= 0.
        final PeerMessage.PrepareResp replay = restarted.handlePrepare(prepare(nine));
        assertFalse(replay.ok(),
                "Ballot 9 may have been promised before the crash, so it must not be promised again");

        // And the whole band below the floor, since any of it could have been promised.
        assertFalse(restarted.handlePrepare(prepare(new Ballot(CHUNK, PEER))).ok(),
                "Everything up to the floor is refused -- conservative by construction");
    }

    @Test
    @DisplayName("The refusal tells the proposer where to go, so it makes progress in one round")
    void refusalCarriesAUsableBallot() {
        final InMemoryWal wal = new InMemoryWal();
        final Acceptor acceptor = new Acceptor(SELF, recovered(wal));
        acceptor.handlePrepare(prepare(new Ballot(9L, PEER)));
        wal.loseUnforcedTail();

        final LocalStore after = recovered(wal);
        final Acceptor restarted = new Acceptor(SELF, after);
        final long floor = after.recoveryPromiseFloor();

        final PeerMessage.PrepareResp refused = restarted.handlePrepare(prepare(new Ballot(9L, PEER)));
        assertFalse(refused.ok());
        // Proposer.observeExternalBallot bumps past whatever a NACK reports. Reporting only the
        // (forgotten, zero) per-key promise would send the proposer back into the refused band once
        // per attempt until its retries ran out.
        assertTrue(refused.promised().counter() >= floor,
                "A NACK must report a ballot that clears the floor, was "
                        + refused.promised().counter() + " against a floor of " + floor);

        assertTrue(restarted.handlePrepare(prepare(new Ballot(floor + 1, PEER))).ok(),
                "One bump past the reported ballot must be enough");
    }

    @Test
    @DisplayName("The ceiling costs one forced write per chunk, not one per promise")
    void ceilingIsReservedOncePerChunk() {
        final InMemoryWal wal = new InMemoryWal();
        final Acceptor acceptor = new Acceptor(SELF, recovered(wal));

        // A run of consecutive ballots inside one chunk. The first reserves; the rest must not.
        for (long counter = 1; counter <= 50; counter++) {
            assertTrue(acceptor.handlePrepare(prepare(new Ballot(counter, PEER))).ok(),
                    "ballot " + counter + " should be promised");
        }
        assertEquals(1, wal.countEntries(Wal.Entry.PromiseCeiling.class),
                "50 promises inside one chunk must cost exactly one reservation");

        // Crossing the chunk boundary reserves again.
        assertTrue(acceptor.handlePrepare(prepare(new Ballot(CHUNK + 1, PEER))).ok());
        assertEquals(2, wal.countEntries(Wal.Entry.PromiseCeiling.class),
                "Crossing the ceiling must reserve once more");
    }

    @Test
    @DisplayName("An accept is fenced by the floor too -- it can arrive without a prepare here")
    void acceptIsFencedByTheFloor() {
        final InMemoryWal wal = new InMemoryWal();
        final Acceptor acceptor = new Acceptor(SELF, recovered(wal));
        acceptor.handlePrepare(prepare(new Ballot(9L, PEER)));
        wal.loseUnforcedTail();

        final Acceptor restarted = new Acceptor(SELF, recovered(wal));

        // A proposer can reach prepare quorum without us and then broadcast the accept to everyone,
        // so handleAccept cannot rely on handlePrepare having run here first.
        final PeerMessage.AcceptResp refused = restarted.handleAccept(new PeerMessage.AcceptReq(
                PEER, 7L, KEY, new Ballot(9L, PEER), TestBytes.hashed("v"), false));
        assertFalse(refused.ok(), "An accept below the floor must be refused, not just a prepare");
    }

    @Test
    @DisplayName("A promise dropped by eviction still refuses its own ballot")
    void evictedPromiseStillBlocksItsBallot() {
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore store = recovered(wal);
        final Acceptor acceptor = new Acceptor(SELF, store);

        final Ballot nine = new Ballot(9L, PEER);
        assertTrue(acceptor.handlePrepare(prepare(nine)).ok());

        // Age the promise past the eviction threshold instead of sleeping through it.
        store.getOrCreate(KEY).promisedAtNanos =
                System.nanoTime() - LocalStore.MIN_PROMISE_AGE_NANOS - 1L;
        assertEquals(1, store.evictPromiseOnly(), "Precondition: the promise-only state is reclaimed");
        assertEquals(0L, store.getOrCreate(KEY).promised.counter(),
                "Precondition: the per-key promise really is gone");

        // Reclaiming the state is the same fault as losing it in an unforced tail, so it takes the
        // same answer: the floor covers what the store can no longer produce. The age threshold
        // bounds how stale such an accept can be; it is not what makes the eviction safe.
        final PeerMessage.AcceptResp refused = acceptor.handleAccept(new PeerMessage.AcceptReq(
                PEER, 1L, KEY, nine, TestBytes.hashed("v"), false));
        assertFalse(refused.ok(), "An accept for an evicted promise's ballot must be refused");
    }

    @Test
    @DisplayName("The ceiling survives the truncation that follows a snapshot")
    void ceilingSurvivesSnapshotTruncation() {
        final InMemoryWal wal = new InMemoryWal();
        final LocalStore store = recovered(wal);
        final Acceptor acceptor = new Acceptor(SELF, store);

        final Ballot ballot = new Ballot(9L, PEER);
        acceptor.handlePrepare(prepare(ballot));
        acceptor.handleAccept(new PeerMessage.AcceptReq(
                PEER, 1L, KEY, ballot, TestBytes.hashed("v"), false));
        final long ceilingBefore = store.promisedUpTo();
        assertTrue(ceilingBefore > 0, "Precondition: a ceiling was reserved");

        // Snapshot, then truncate. The snapshot footer carries the proposer reservation but not this
        // one, so the ceiling has to be re-asserted into the surviving tail or it vanishes here.
        final LocalStore.ThrottledSnapshot snapshot = store.beginSnapshot();
        while (snapshot.writeBatch(256)) {
            // drain
        }

        final LocalStore after = recovered(wal);
        assertEquals(ceilingBefore, after.promisedUpTo(),
                "The ceiling must survive compaction, or the floor silently drops to zero");
        assertEquals(ceilingBefore, after.recoveryPromiseFloor());
    }

    private static PeerMessage.PrepareReq prepare(final Ballot ballot) {
        return new PeerMessage.PrepareReq(PEER, 1L, KEY, ballot);
    }
}
