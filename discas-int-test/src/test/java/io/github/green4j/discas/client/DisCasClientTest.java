/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CAS client -- routing, retry, shutdown")
class DisCasClientTest {
    private TestCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new TestCluster(3, 2);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }



    @Test
    @DisplayName("Shutdown fails pending request futures")
    void shutdownFailsPendingFutures() throws Exception {
        // Isolate all nodes so operations hang
        for (final int nodeId : cluster.nodeIds()) {
            for (final int otherId : cluster.nodeIds()) {
                if (nodeId != otherId) {
                    cluster.transport(nodeId).isolate(otherId);
                }
            }
        }

        final DisCasClient client = cluster.client(0);

        // Start operations that will be pending
        final CompletableFuture<GetResult> getFuture = client.get(TestBytes.utf8("pending-key"));

        client.close();

        assertTrue(getFuture.isDone());
        assertTrue(getFuture.isCompletedExceptionally());
    }

    @Test
    @DisplayName("A closed client rejects new requests")
    void closedClientRejectsNewRequests() throws Exception {
        final DisCasClient client = cluster.client(0);
        client.close();

        final CompletableFuture<GetResult> future = client.get(TestBytes.utf8("after-close"));
        assertTrue(future.isCompletedExceptionally());

        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
    }

    private static String keyOf(final ScanResult r) {
        final ByteBuffer buf = r.key();
        final byte[] out = new byte[buf.remaining()];
        buf.duplicate().get(out);
        return new String(out, StandardCharsets.UTF_8);
    }

    private static List<String> keysOf(final List<ScanResult> results) {
        final List<String> out = new ArrayList<>();
        for (final ScanResult r : results) {
            out.add(keyOf(r));
        }
        return out;
    }

    @Test
    @DisplayName("Paging walks the whole key set exactly once, in key order")
    void pagingCoversEverythingWithoutGapsOrDuplicates() throws Exception {
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            final String k = String.format("page/%02d", i);
            cluster.client(0).put(TestBytes.utf8(k), TestBytes.utf8("v")).get(5, TimeUnit.SECONDS);
            expected.add(k);
        }

        // Page size 3 over 12 keys: several pages, each an independent quorum read whose merge
        // is truncated at the smallest last-key among nodes reporting more.
        final List<String> seen = new ArrayList<>();
        ByteBuffer cursor = null;
        int guard = 0;
        do {
            final ScanPage page = cluster.client(1)
                    .scan(TestBytes.utf8("page/"), cursor, 3)
                    .get(10, TimeUnit.SECONDS);
            seen.addAll(keysOf(page.results()));
            cursor = page.nextCursor();
            assertTrue(++guard < 50, "Pagination did not terminate");
        } while (cursor != null);

        assertEquals(expected, seen,
                "Every key exactly once, ascending -- a gap means the merge truncation is wrong, "
                        + "a duplicate means the cursor is not exclusive");
    }

    @Test
    @DisplayName("The prefix is pushed down: unrelated keys never come back")
    void prefixIsPushedDown() throws Exception {
        cluster.client(0).put(TestBytes.utf8("alpha/1"), TestBytes.utf8("v")).get(5, TimeUnit.SECONDS);
        cluster.client(0).put(TestBytes.utf8("beta/1"), TestBytes.utf8("v")).get(5, TimeUnit.SECONDS);

        final List<String> alpha = keysOf(
                cluster.client(1).scan("alpha/").get(10, TimeUnit.SECONDS).results());
        assertEquals(List.of("alpha/1"), alpha);
    }

    @Test
    @DisplayName("Auto-paging scan(prefix) returns the same keys as manual paging")
    void autoPagingMatchesManualPaging() throws Exception {
        for (int i = 0; i < 7; i++) {
            cluster.client(0).put(TestBytes.utf8("auto/" + i), TestBytes.utf8("v")).get(5, TimeUnit.SECONDS);
        }

        final List<String> auto = keysOf(
                cluster.client(1).scan("auto/").get(15, TimeUnit.SECONDS).results());

        final List<String> manual = new ArrayList<>();
        ByteBuffer cursor = null;
        do {
            final ScanPage page = cluster.client(1)
                    .scan(TestBytes.utf8("auto/"), cursor, 2).get(10, TimeUnit.SECONDS);
            manual.addAll(keysOf(page.results()));
            cursor = page.nextCursor();
        } while (cursor != null);

        assertEquals(manual, auto);
        assertEquals(7, auto.size());
    }
}
