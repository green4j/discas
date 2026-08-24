/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestCas;
import io.github.green4j.discas.TestCluster;
import io.github.green4j.discas.client.CasResult;

import io.github.green4j.discas.client.ScanResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// One cluster for the file: each test below owns its keys, so nothing needs a fresh one.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("DisCasNode -- end-to-end CRUD + CAS + scan over an in-process 3-node cluster")
class DisCasNodeIntegrationTest {

    private TestCluster cluster;

    @BeforeAll
    void setUp() throws Exception {
        cluster = new TestCluster(3, 2);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterAll
    void tearDown() {
        cluster.close();
    }


    @Test
    void putOverwrite() throws Exception {
        cluster.client(0).put(TestBytes.utf8("overwritten"), TestBytes.utf8("v1")).get(5, TimeUnit.SECONDS);
        cluster.client(0).put(TestBytes.utf8("overwritten"), TestBytes.utf8("v2")).get(5, TimeUnit.SECONDS);

        final ByteBuffer result = cluster.client(1).get(TestBytes.utf8("overwritten")).get(5, TimeUnit.SECONDS).value();
        assertNotNull(result);
        assertEquals("v2", new String(toBytes(result)));
    }

    @Test
    void getOnFreshKeyReturnsNull() throws Exception {
        final ByteBuffer result = cluster.client(0).get(TestBytes.utf8("nonexistent")).get(5, TimeUnit.SECONDS).value();
        assertNull(result);
    }

    @Test
    void deleteRemovesKey() throws Exception {
        cluster.client(0).put(TestBytes.utf8("deleted"), TestBytes.utf8("val")).get(5, TimeUnit.SECONDS);
        cluster.client(0).delete(TestBytes.utf8("deleted")).get(5, TimeUnit.SECONDS);

        final ByteBuffer result = cluster.client(1).get(TestBytes.utf8("deleted")).get(5, TimeUnit.SECONDS).value();
        assertNull(result);
    }

    @Test
    void casFailsWhenExpectedMismatches() throws Exception {
        cluster.client(0).put(TestBytes.utf8("cas-mismatch"), TestBytes.utf8("actual")).get(5, TimeUnit.SECONDS);

        final CasResult result = TestCas.swapValue(cluster.client(0),
                TestBytes.utf8("cas-mismatch"), TestBytes.utf8("wrong"), TestBytes.utf8("new"), 5_000L);

        assertFalse(result.swapped());
    }

    @Test
    void scanExcludesTombstones() throws Exception {
        cluster.client(0).put(TestBytes.utf8("alive"), TestBytes.utf8("val")).get(5, TimeUnit.SECONDS);
        cluster.client(0).put(TestBytes.utf8("dead"), TestBytes.utf8("val")).get(5, TimeUnit.SECONDS);
        cluster.client(0).delete(TestBytes.utf8("dead")).get(5, TimeUnit.SECONDS);

        final List<ScanResult> results =
                cluster.client(1).scan().get(10, TimeUnit.SECONDS).results();

        final boolean hasDead = results.stream()
                .anyMatch(r -> "dead".equals(new String(toBytes(r.key()))));
        assertFalse(hasDead, "Tombstoned key should not appear in scan");

        final boolean hasAlive = results.stream()
                .anyMatch(r -> "alive".equals(new String(toBytes(r.key()))));
        assertTrue(hasAlive, "Live key should appear in scan");
    }

    private static byte[] toBytes(final ByteBuffer buf) {
        final byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }
}
