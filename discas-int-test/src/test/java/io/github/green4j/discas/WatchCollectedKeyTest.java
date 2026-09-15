/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.WatchResult;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A watch whose key is collected while it waits.
 * <p>
 * A delete wakes a watch because its tombstone advances the version, and the tombstone is itself
 * collectable: once no replica can resurrect the value, the cluster purges the key and the register
 * reads as one that was never written. A watch polling across that never sees the advance it was
 * waiting for -- the version it finds is below its own cursor, not above it -- so the delete has to
 * reach the caller as the reset it now is, or not at all.
 * <p>
 * Its own cluster, with a sweeper that collects inside one poll gap: the race is the subject here,
 * and {@link WatchTest} configures the same race out to assert the tombstone the delete commits.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("Watch -- the key is collected while it waits")
class WatchCollectedKeyTest {

    private TestCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new TestCluster(3, 1,
                b -> b.tombstoneSweepInterval(Duration.ofMillis(50)),
                DisCasClientConfig.defaults());
        cluster.start();
        cluster.awaitReady();
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    @Test
    @DisplayName("A collected key wakes the watch, at the version of a key that holds nothing")
    void collectedTombstoneWakesTheWatch() throws Exception {
        final DisCasClient client = cluster.client(0);
        client.put(TestBytes.utf8("watch-collected"), TestBytes.utf8("v0")).get(5, TimeUnit.SECONDS);
        final Version seen = client.get("watch-collected").get(5, TimeUnit.SECONDS).version();

        final CompletableFuture<WatchResult> watch =
                client.watch("watch-collected", seen, Duration.ofSeconds(8));

        client.delete(TestBytes.utf8("watch-collected")).get(5, TimeUnit.SECONDS);

        final WatchResult r = watch.get(8, TimeUnit.SECONDS);
        assertTrue(r.changed(), "A key that is gone is a change, not a quiet key");
        assertNull(r.value(), "The value the collected tombstone suppressed must not come back");
        assertEquals(Version.INITIAL, r.version(),
                "A collected key reads as one never written, and the cursor says so");
    }
}
