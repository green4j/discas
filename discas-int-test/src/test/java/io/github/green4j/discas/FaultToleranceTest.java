/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Fault tolerance -- partition / heal / quorum behaviour")
class FaultToleranceTest {

    private TestCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new TestCluster(3, 2);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterEach
    void tearDown() {
        // Heal all partitions before shutdown to avoid hangs
        for (final int nodeId : cluster.nodeIds()) {
            for (final int otherId : cluster.nodeIds()) {
                if (nodeId != otherId) {
                    cluster.transport(nodeId).heal(otherId);
                }
            }
        }
        cluster.close();
    }



    @Test
    @DisplayName("Write fails when quorum is unreachable (all peers isolated)")
    void writeFailsWithMajorityDown() {
        // Fully isolate all nodes from each other so no quorum is possible
        for (final int a : cluster.nodeIds()) {
            for (final int b : cluster.nodeIds()) {
                if (a != b) {
                    cluster.transport(a).isolate(b);
                }
            }
        }

        // Write should time out since quorum is unreachable on every peer
        // Client tries each peer (5s timeout each) x 3 peers = 15s+ before failing
        assertThrows(Exception.class, () ->
                cluster.client(0).put(TestBytes.utf8("key"), TestBytes.utf8("val")).get(25, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Network partition then heal -- all clients converge to the majority value")
    void networkPartitionAndHealConverges() throws Exception {
        cluster.client(0).put(TestBytes.utf8("key"), TestBytes.utf8("pre")).get(5, TimeUnit.SECONDS);

        // Partition: node 1 alone vs nodes 2+3
        cluster.transport(1).isolate(2);
        cluster.transport(1).isolate(3);
        cluster.transport(2).isolate(1);
        cluster.transport(3).isolate(1);

        // Client 1 is wired through all peers: it may hit node 1 first and fail, then retry on 2/3.
        cluster.client(1).put(TestBytes.utf8("key"), TestBytes.utf8("majority")).get(10, TimeUnit.SECONDS);

        // A quorum of two is a quorum: the write is not only accepted but readable while the
        // third member is still cut off.
        assertEquals("majority", TestBytes.string(
                cluster.client(1).get(TestBytes.utf8("key")).get(20, TimeUnit.SECONDS).value()));

        cluster.transport(1).heal(2);
        cluster.transport(1).heal(3);
        cluster.transport(2).heal(1);
        cluster.transport(3).heal(1);

        // Wait for the heal by asking, not by guessing. The value is the majority side's: the
        // minority never committed anything, so convergence means everyone reads what 2+3 chose.
        TestAwait.until("both clients to converge on the majority value", () -> {
            for (int client = 0; client < 2; client++) {
                final String seen = TestBytes.string(
                        cluster.client(client).get(TestBytes.utf8("key")).get(5, TimeUnit.SECONDS).value());
                if (!"majority".equals(seen)) {
                    throw new IllegalStateException("Client " + client + " read " + seen);
                }
            }
        });

        final String v0 = TestBytes.string(cluster.client(0).get(TestBytes.utf8("key")).get(5,
                TimeUnit.SECONDS).value());
        final String v1 = TestBytes.string(cluster.client(1).get(TestBytes.utf8("key")).get(5,
                TimeUnit.SECONDS).value());
        assertEquals("majority", v0);
        assertEquals("majority", v1);
    }

}
