/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Workstream A -- opt-in serializable reads. A {@code SERIALIZABLE} get must return the
 * target node's locally-committed value with no Paxos round and no WAL promise, while a
 * {@code LINEARIZABLE} get still runs a round (which promises on the acceptors it reaches).
 */
@DisplayName("Cheap (serializable) reads")
class CheapReadTest {

    private TestCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new TestCluster(3, 1);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }



    /** Total Promise WAL entries across every node's in-memory WAL. */
    private long totalPromises() {
        long count = 0;
        for (final int id : cluster.nodeIds()) {
            for (final Wal.Entry entry : cluster.wal(id).entries()) {
                if (entry instanceof Wal.Entry.Promise) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Wait for in-flight best-effort accept broadcasts to land, so a WAL count is not a race with
     * them. Waits for the count to stop moving rather than sleeping a guessed interval: a broadcast
     * that had not arrived yet would otherwise read exactly like one that never will.
     */
    private void quiesce() throws Exception {
        TestAwait.untilStable("the WAL promise count", this::totalPromises,
                TestAwait.QUIET, Duration.ofSeconds(30));
    }

    /**
     * Wait until every node has the key in its own log.
     * <p>
     * Not tidiness -- it is what makes a promise count mean anything here. Anti-entropy runs a
     * repair cycle the moment a node serves, whatever its interval is configured to be
     * ({@code AntiEntropy.startPeriodicRepair}), so that first cycle overlaps the first write. A
     * cycle that compares keys while an accept broadcast is still landing sees the key on some
     * members and not others, calls it divergence, and runs a repair round -- three more promises,
     * arriving after {@link #quiesce()} has already found the count quiet. A converged cluster gives
     * that cycle nothing to repair, which removes the race rather than waiting longer for it.
     */
    private void awaitConverged(final String key) throws Exception {
        final HashedBytes target = new HashedBytes(TestBytes.utf8(key));
        for (final int id : cluster.nodeIds()) {
            TestAwait.until("node " + id + " to hold " + key, Duration.ofSeconds(10), () -> {
                for (final Wal.Entry entry : cluster.wal(id).entries()) {
                    if (entry instanceof Wal.Entry.Accept
                            && target.equals(((Wal.Entry.Accept) entry).key())) {
                        return;
                    }
                }
                throw new IllegalStateException("Node " + id + " has no accept for " + key);
            });
        }
    }

    @Test
    void serializableReadReturnsLocalValueWithoutPromise() throws Exception {
        final DisCasClient client = cluster.client(0);
        // Absent before the write: a local read of a key nobody wrote is null, not an error.
        assertNull(client.get(TestBytes.utf8("k"), ReadConsistency.SERIALIZABLE)
                .get(5, TimeUnit.SECONDS).value());
        client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(5, TimeUnit.SECONDS);
        awaitConverged("k");
        quiesce();

        final long before = totalPromises();
        final String got = TestBytes.string(client.get(TestBytes.utf8("k"), ReadConsistency.SERIALIZABLE)
                .get(5, TimeUnit.SECONDS).value());
        quiesce();

        assertEquals("v", got);
        assertEquals(before, totalPromises(),
                "A serializable read must not append any WAL promise");

        // And a tombstone read locally reports null rather than the value it replaced.
        client.delete(TestBytes.utf8("k")).get(5, TimeUnit.SECONDS);
        quiesce();
        assertNull(client.get(TestBytes.utf8("k"), ReadConsistency.SERIALIZABLE)
                .get(5, TimeUnit.SECONDS).value());
    }

    @Test
    void linearizableReadRunsARoundAndPromises() throws Exception {
        final DisCasClient client = cluster.client(0);
        client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(5, TimeUnit.SECONDS);
        quiesce();

        final long before = totalPromises();
        final String got = TestBytes.string(client.get(TestBytes.utf8("k"), ReadConsistency.LINEARIZABLE)
                .get(5, TimeUnit.SECONDS).value());
        quiesce();

        assertEquals("v", got);
        assertTrue(totalPromises() > before,
                "A linearizable read runs a Paxos round that promises on the acceptors");
    }

}
