/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.Version;

import io.github.green4j.discas.chaos.TestProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Linearizability and consistency across nodes")
class LinearizabilityTest {

    private TestCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new TestCluster(3, 3);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }



    @Test
    @DisplayName("Concurrent puts on the same key converge to one of the proposed values")
    void concurrentPutsToSameKeyConverge() throws Exception {
        final ByteBuffer key = TestBytes.utf8("shared");
        final List<CompletableFuture<Version>> futures = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            final ByteBuffer value = TestBytes.utf8("val-" + i);
            futures.add(cluster.client(i).put(key.duplicate(), value));
        }

        for (final CompletableFuture<Version> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }

        final String v0 = TestBytes.string(cluster.client(0).get(TestBytes.utf8("shared")).get(5,
                TimeUnit.SECONDS).value());
        final String v1 = TestBytes.string(cluster.client(1).get(TestBytes.utf8("shared")).get(5,
                TimeUnit.SECONDS).value());
        final String v2 = TestBytes.string(cluster.client(2).get(TestBytes.utf8("shared")).get(5,
                TimeUnit.SECONDS).value());

        assertNotNull(v0);
        assertEquals(v0, v1);
        assertEquals(v1, v2);
        assertTrue(Set.of("val-0", "val-1", "val-2").contains(v0),
                "Final value should be one of the put values");
    }

    @Test
    @DisplayName("Concurrent CAS increments are atomic -- final counter equals successful swaps")
    void concurrentCasIsAtomic() throws Exception {
        cluster.client(0).put(TestBytes.utf8("counter"), TestBytes.utf8("0")).get(5, TimeUnit.SECONDS);

        final int incrementsPerClient = TestProfile.current().scale(5);
        final List<CompletableFuture<Integer>> results = new ArrayList<>();

        for (int c = 0; c < 3; c++) {
            final DisCasClient client = cluster.client(c);
            results.add(CompletableFuture.supplyAsync(() -> {
                int successCount = 0;
                for (int i = 0; i < incrementsPerClient; i++) {
                    for (int retry = 0; retry < 20; retry++) {
                        try {
                            final ByteBuffer current = client.get(TestBytes.utf8("counter"))
                                    .get(5, TimeUnit.SECONDS).value();
                            final int currentVal = Integer.parseInt(TestBytes.string(current));
                            final CasResult casResult = TestCas.swapValue(
                                    client,
                                    TestBytes.utf8("counter"),
                                    TestBytes.utf8(String.valueOf(currentVal)),
                                    TestBytes.utf8(String.valueOf(currentVal + 1)),
                                    5_000L);
                            if (casResult.swapped()) {
                                successCount++;
                                break;
                            }
                        } catch (final Exception e) {
                            // Retry on failure
                        }
                    }
                }
                return successCount;
            }));
        }

        int totalSuccesses = 0;
        final long awaitSeconds = TestProfile.current().scale(30L);
        for (final CompletableFuture<Integer> f : results) {
            totalSuccesses += f.get(awaitSeconds, TimeUnit.SECONDS);
        }

        final String finalVal = TestBytes.string(cluster.client(0).get(TestBytes.utf8("counter")).get(5,
                TimeUnit.SECONDS).value());
        assertEquals(totalSuccesses, Integer.parseInt(finalVal),
                "Counter should equal total successful CAS increments");
    }

    @Test
    @DisplayName("Serial CAS chain A->B->C preserves order across all clients")
    void serialCasChainPreservesOrder() throws Exception {
        cluster.client(0).put(TestBytes.utf8("chain"), TestBytes.utf8("A")).get(5, TimeUnit.SECONDS);

        final CasResult r1 = TestCas.swapValue(cluster.client(0),
                TestBytes.utf8("chain"), TestBytes.utf8("A"), TestBytes.utf8("B"), 5_000L);
        assertTrue(r1.swapped());

        final CasResult r2 = TestCas.swapValue(cluster.client(1),
                TestBytes.utf8("chain"), TestBytes.utf8("B"), TestBytes.utf8("C"), 5_000L);
        assertTrue(r2.swapped());

        final String finalVal = TestBytes.string(cluster.client(2).get(TestBytes.utf8("chain")).get(5,
                TimeUnit.SECONDS).value());
        assertEquals("C", finalVal);
    }
}
