/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestCluster;
import io.github.green4j.discas.common.client.ReadConsistency;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.github.green4j.discas.TestBytes.string;
import static io.github.green4j.discas.TestBytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code delete(key, Version)} is the fenced counterpart of {@code delete(key)}: a compare-and-set
 * to a tombstone rather than an unconditional one. It removes the value only while the key is
 * still where the caller left it, and reports the version that won when it is not.
 */
// One cluster for the file: each test owns its versioned-delete/* key.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Versioned delete -- fenced tombstone")
class VersionedDeleteTest {

    private static final long TIMEOUT_MS = 10_000L;

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
    @DisplayName("At the expected version it tombstones and advances the version")
    void deletesAtExpectedVersion() throws Exception {
        final DisCasClient client = cluster.client(0);
        final ByteBuffer key = utf8("versioned-delete/present");

        client.put(key.duplicate(), utf8("v1")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        final Version atV1 = versionOf(client, key);

        final CasResult deleted = client
                .delete(key.duplicate(), atV1)
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertTrue(deleted.swapped(), "The key was at the expected version, so the delete applies");
        assertNull(deleted.value(), "A tombstone has no value");
        assertNotEquals(atV1, deleted.version(),
                "The tombstone is a write and advances the version, as the unfenced delete does");

        assertNull(client.get(key.duplicate()).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value(),
                "The value is gone");
    }

    @Test
    @DisplayName("At a stale version it applies nothing and reports the version that won")
    void refusesAtStaleVersion() throws Exception {
        final DisCasClient client = cluster.client(0);
        final ByteBuffer key = utf8("versioned-delete/moved-on");

        client.put(key.duplicate(), utf8("v1")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        final Version stale = versionOf(client, key);

        // Somebody else writes; the caller's version is now behind.
        client.put(key.duplicate(), utf8("v2")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        final CasResult refused = client
                .delete(key.duplicate(), stale)
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertFalse(refused.swapped(), "The key had moved on, so the fence rejects the delete");
        assertEquals("v2", string(refused.value()),
                "The losing caller gets the value that is there instead, without a second read");
        assertNotEquals(stale, refused.version(), "And the version that won");

        assertEquals("v2", string(client.get(key.duplicate()).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value()),
                "V2 survives -- this is the write an unfenced delete would have destroyed");

        // The reported version is directly usable: retrying with it succeeds.
        assertTrue(client.delete(key.duplicate(), refused.version())
                        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS).swapped(),
                "version() feeds straight back into the retry");
    }

    @Test
    @DisplayName("Version.INITIAL means create-if-absent, so it loses against a written key")
    void initialVersionLosesAgainstAWrittenKey() throws Exception {
        final DisCasClient client = cluster.client(0);
        final ByteBuffer key = utf8("versioned-delete/never-absent");

        client.put(key.duplicate(), utf8("v1")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        final CasResult refused = client
                .delete(key.duplicate(), Version.INITIAL)
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertFalse(refused.swapped(),
                "INITIAL is a real version -- 'never written' -- and not a wildcard");
        assertEquals("v1", string(client.get(key.duplicate()).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value()));
    }

    private static Version versionOf(final DisCasClient client, final ByteBuffer key)
            throws Exception {
        return client.get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .version();
    }
}
