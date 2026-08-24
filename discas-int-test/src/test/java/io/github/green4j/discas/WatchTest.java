/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.WatchResult;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client-side watch / blocking query (coalescing, per-key {@link Version} cursor). Verifies the
 * four cases from the design: fire-from-INITIAL on an existing key, wake on a concurrent write,
 * time out unchanged when nothing happens, and wake on a delete (tombstone advances the version).
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
// One cluster for the file: each test watches a key of its own.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Watch (blocking query)")
class WatchTest {

    private TestCluster cluster;

    @BeforeAll
    void setUp() throws Exception {
        cluster = new TestCluster(3, 1);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterAll
    void tearDown() {
        cluster.close();
    }



    @Test
    void watchFromInitialReturnsCurrentValueImmediately() throws Exception {
        final DisCasClient client = cluster.client(0);
        client.put(TestBytes.utf8("watch-initial"), TestBytes.utf8("v0")).get(5, TimeUnit.SECONDS);

        final WatchResult r = client.watch("watch-initial", Version.INITIAL, Duration.ofSeconds(5))
                .get(5, TimeUnit.SECONDS);

        assertTrue(r.changed(), "An existing key must fire immediately from INITIAL");
        assertEquals("v0", TestBytes.string(r.value()));
        assertTrue(r.version().compareTo(Version.INITIAL) > 0);
    }

    @Test
    void watchWakesOnConcurrentWrite() throws Exception {
        final DisCasClient client = cluster.client(0);
        client.put(TestBytes.utf8("watch-write"), TestBytes.utf8("v0")).get(5, TimeUnit.SECONDS);
        final Version seen = settledCursor(client, "watch-write");

        // Nothing has changed past `seen` yet, so this watch parks.
        final CompletableFuture<WatchResult> watch =
                client.watch("watch-write", seen, Duration.ofSeconds(8));
        assertFalse(watch.isDone(), "Watch must not complete until the value changes");

        client.put(TestBytes.utf8("watch-write"), TestBytes.utf8("v1")).get(5, TimeUnit.SECONDS);

        final WatchResult r = watch.get(8, TimeUnit.SECONDS);
        assertTrue(r.changed());
        assertEquals("v1", TestBytes.string(r.value()));
        assertTrue(r.version().compareTo(seen) > 0, "The version must advance past the cursor");
    }

    @Test
    void watchTimesOutUnchanged() throws Exception {
        final DisCasClient client = cluster.client(0);
        client.put(TestBytes.utf8("watch-quiet"), TestBytes.utf8("v0")).get(5, TimeUnit.SECONDS);
        final Version seen = settledCursor(client, "watch-quiet");

        final long t0 = System.nanoTime();
        final WatchResult r = client.watch("watch-quiet", seen, Duration.ofSeconds(1))
                .get(5, TimeUnit.SECONDS);
        final long elapsed = (System.nanoTime() - t0) / 1_000_000L;

        assertFalse(r.changed(), "No write happened, so the watch must report unchanged");
        assertEquals(seen, r.version(), "Unchanged reply carries the same cursor");
        assertTrue(elapsed >= 900, "The watch must wait out (approximately) its budget");
    }

    @Test
    void watchWakesOnDelete() throws Exception {
        final DisCasClient client = cluster.client(0);
        client.put(TestBytes.utf8("watch-delete"), TestBytes.utf8("v0")).get(5, TimeUnit.SECONDS);
        final Version seen = settledCursor(client, "watch-delete");

        final CompletableFuture<WatchResult> watch =
                client.watch("watch-delete", seen, Duration.ofSeconds(8));

        client.delete(TestBytes.utf8("watch-delete")).get(5, TimeUnit.SECONDS);

        final WatchResult r = watch.get(8, TimeUnit.SECONDS);
        assertTrue(r.changed(), "A delete advances the version and wakes the watch");
        assertNull(r.value(), "A tombstoned key reports a null value");
        assertTrue(r.version().compareTo(seen) > 0);
    }

    /**
     * The key's cursor with the one benign spurious wake already spent.
     *
     * <p>A linearizable read that has to repair a lagging quorum re-accepts the current value at a
     * new ballot, so the cursor advances although nothing was written -- a benign spurious wake.
     * A test that captures a cursor straight from a write and then asserts what the <em>next</em>
     * change is
     * asserts something the design does not promise, and fails on the run where the repair lands
     * inside its watch instead of before it. Watching once here spends that wake: whatever it
     * returns, the cursor going back to the caller is the settled one.
     */
    private static Version settledCursor(final DisCasClient client, final String key)
            throws Exception {
        final Version read = client.get(key).get(5, TimeUnit.SECONDS).version();
        final WatchResult repaired = client.watch(key, read, Duration.ofMillis(500))
                .get(5, TimeUnit.SECONDS);
        return repaired.changed() ? repaired.version() : read;
    }
}
