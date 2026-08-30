/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestCluster;
import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.KeyHash;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a {@code SERIALIZABLE} watch does when the member its key resolves to is behind the cluster.
 *
 * <p>Without rotation this is the case that defeats a serializable watch, and it defeats it
 * quietly. Nothing fails: the lagging member answers every poll, promptly and with its own
 * committed state, so no failure is recorded, no backoff moves the polls elsewhere, and the watch
 * addresses that one member for its whole budget. It then reports the key as quiet while two
 * reachable members hold the change, and hands back the laggard's older version -- which
 * {@link WatchResult#version()} tells the caller to feed into the next watch.
 *
 * <p>The fixture separates the two roles that normally coincide. A key is chosen whose home
 * coordinator, in the watcher's peer order, is member 3; member 3 is then cut off from its peers so
 * it misses every accept that follows, while still answering clients from local state -- the
 * serializable branch of a read is served before the coordinator's majority check, so a member
 * that cannot reach a quorum still answers a serializable read, with whatever it last committed.
 * The writer is a second client whose peer order resolves the same key to member 2, so nothing the
 * writer does puts member 3 into the watcher's peer backoff: the watcher polls its home
 * coordinator exactly as it would with no faults at all.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Serializable watch against a lagging member")
class SerializableWatchLagTest {

    private static final int LAGGARD = 3;
    private static final Duration WATCH_BUDGET = Duration.ofSeconds(3);

    private TestCluster cluster;
    private DisCasClient watcher;
    private DisCasClient writer;
    private EventLoop writerLoop;
    private ByteBuffer key;

    @BeforeEach
    void setup() throws Exception {
        cluster = new TestCluster(3, 1);
        cluster.start();
        cluster.awaitReady();

        watcher = cluster.client(0);

        // The watcher's peer order is [1, 2, 3]; the writer's is [3, 1, 2]. One key therefore has
        // two different home coordinators -- member 3 for the watcher, member 2 for the writer --
        // which is what lets the writer commit without ever addressing the member the watcher
        // polls. (Two clients ordering their lists differently is the thing the operator guide
        // warns against for exactly this reason; here it is the point.)
        key = keyWhoseHomeIndexIs(2);

        final ClientId writerId = ClientId.of("writer");
        writerLoop = new EventLoop("writer-loop");
        writer = new DisCasClient(writerId, new InProcessClientTransport(writerLoop,
                List.of(NodeId.of("3"), NodeId.of("1"), NodeId.of("2")), writerId), writerLoop);
    }

    @AfterEach
    void tearDown() {
        if (writer != null) {
            try {
                writer.close();
            } catch (final Exception ignored) {
            }
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    /**
     * Both properties in one run: a change any reachable member holds is observed, and the version
     * handed back never falls below the one the caller passed in.
     */
    @Test
    @DisplayName("A lagging home coordinator no longer hides the change, nor rolls the version back")
    void aLaggingHomeCoordinatorDoesNotHideTheChange() throws Exception {
        final Version v0 = writer.put(key.duplicate(), TestBytes.utf8("v0")).get(8, TimeUnit.SECONDS);

        cutOffTheLaggard();

        final Version v1 = writer.put(key.duplicate(), TestBytes.utf8("v1")).get(8, TimeUnit.SECONDS);
        assertTrue(v1.compareTo(v0) > 0, "The second write must have committed at a higher version");

        // Sanity: the watcher's home coordinator is the member that missed it.
        final GetResult stale = watcher.get(key.duplicate(), ReadConsistency.SERIALIZABLE)
                .get(8, TimeUnit.SECONDS);
        assertEquals(v0, stale.version(), "The polled member must be the one that is behind");
        assertEquals("v0", TestBytes.string(stale.value()));

        // Order matters here, and finding that out is half of what this fixture is for: a
        // linearizable poll is refused by the isolated member, which records a peer failure and
        // puts it in the client's backoff, and every later poll then skips it and reads from a
        // healthy member -- masking the very thing under test. The serializable assertions
        // therefore run while the watcher is still routing to its home coordinator, and the
        // linearizable contrast comes last.

        // Successive polls address successive members, so the change is found on one of the two
        // that hold it rather than waited out on the one that does not.
        final WatchResult observed = watcher
                .watch(key.duplicate(), v0, WATCH_BUDGET, ReadConsistency.SERIALIZABLE)
                .get(30, TimeUnit.SECONDS);
        assertTrue(observed.changed(),
                "A change two reachable members hold must be observed");
        assertEquals(v1, observed.version());
        assertEquals("v1", TestBytes.string(observed.value()));

        // Watching from the version the caller already holds: nothing is above it, so this reports
        // quiet -- but at the caller's own version, not at the laggard's older one. Feeding
        // version() into the next watch, which is what its javadoc says to do, cannot regress.
        final WatchResult quiet = watcher
                .watch(key.duplicate(), v1, WATCH_BUDGET, ReadConsistency.SERIALIZABLE)
                .get(30, TimeUnit.SECONDS);
        assertFalse(quiet.changed());
        assertTrue(quiet.version().compareTo(v1) >= 0,
                "The result must never carry a version below the one the caller passed in");
        assertEquals(v1, quiet.version());
        assertNotNull(quiet.value());
        assertEquals("v1", TestBytes.string(quiet.value()),
                "The value must travel with the version it was read at");

        // The change is genuinely there and genuinely reachable: a linearizable watch sees it,
        // because its poll runs a round, is refused by the isolated member and moves on.
        final WatchResult linearizable = watcher
                .watch(key.duplicate(), v0, WATCH_BUDGET, ReadConsistency.LINEARIZABLE)
                .get(30, TimeUnit.SECONDS);
        assertTrue(linearizable.changed(), "The change must be visible to a linearizable watch");
        assertEquals(v1, linearizable.version());
    }

    /** Member 3 keeps serving clients from local state, but takes part in no more rounds. */
    private void cutOffTheLaggard() {
        for (final int nodeId : cluster.nodeIds()) {
            if (nodeId == LAGGARD) {
                continue;
            }
            cluster.transport(nodeId).isolate(LAGGARD);
            cluster.transport(LAGGARD).isolate(nodeId);
        }
    }

    /**
     * A key the watcher's client routes to {@code peers.get(index)} on its first attempt --
     * {@code peerIndex} is {@code (distributionHash + attempt) mod M}, so this is that function
     * read backwards.
     */
    private static ByteBuffer keyWhoseHomeIndexIs(final int index) {
        for (int i = 0; i < 10_000; i++) {
            final ByteBuffer candidate = TestBytes.utf8("watch/key-" + i);
            if (Integer.remainderUnsigned(KeyHash.distributionHash(candidate.duplicate()), 3) == index) {
                return candidate;
            }
        }
        throw new IllegalStateException("No key hashed to index " + index);
    }
}
