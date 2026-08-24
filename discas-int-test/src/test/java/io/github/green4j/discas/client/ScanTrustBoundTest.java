/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The scan merge-truncation rule, exercised against <em>divergent</em> node pages.
 * <p>
 * A cluster test cannot cover this: every healthy node holds the same keys and so returns the
 * same page, which makes {@code min(last key)} and {@code max(last key)} identical -- an
 * incorrect bound would pass unnoticed. These cases construct pages that differ per node, which
 * is what happens whenever replicas are mid-convergence.
 *
 * @see DisCasClient#scanTrustBound(List)
 */
@DisplayName("Scan merge -- how far a merged page can be trusted")
class ScanTrustBoundTest {

    private static final Ballot B = new Ballot(1, NodeId.of("1"));

    /** A node's page: the keys it returned, and whether it has more beyond them. */
    private static ClientMessage.ClientScanResp page(final boolean hasMore, final String... keys) {
        final List<ClientMessage.ScanEntry> entries = new ArrayList<>();
        for (final String k : keys) {
            entries.add(new ClientMessage.ScanEntry(TestBytes.utf8(k), B, false));
        }
        return new ClientMessage.ClientScanResp("n", 1L, entries, hasMore);
    }

    @Test
    @DisplayName("All nodes exhausted -> no bound, the merge is complete")
    void allExhaustedMeansComplete() {
        assertNull(DisCasClient.scanTrustBound(List.of(
                page(false, "a", "b"),
                page(false, "a", "c"))));
    }

    @Test
    @DisplayName("Bound is the SMALLEST last-key among nodes reporting more")
    void boundIsTheSmallestLastKey() {
        // Node A stopped at "c" and has more; node B stopped at "m" and has more. Trusting "m"
        // would skip whatever A holds between "c" and "m" -- keys the merge never saw.
        assertEquals(TestBytes.utf8("c"), DisCasClient.scanTrustBound(List.of(
                page(true, "a", "c"),
                page(true, "a", "m"))));
    }

    @Test
    @DisplayName("An exhausted node imposes no bound even if its last key is smallest")
    void exhaustedNodeImposesNoBound() {
        // Node A is done at "b": nothing of A's is missing, so it cannot limit the merge.
        assertEquals(TestBytes.utf8("m"), DisCasClient.scanTrustBound(List.of(
                page(false, "a", "b"),
                page(true, "a", "m"))));
    }

    @Test
    @DisplayName("A node that returned nothing imposes no bound")
    void emptyPageImposesNoBound() {
        // An empty page has no last key; treating it as a bound would stall pagination forever.
        assertEquals(TestBytes.utf8("m"), DisCasClient.scanTrustBound(List.of(
                page(true),
                page(true, "a", "m"))));
    }

    @Test
    @DisplayName("No responses at all -> no bound")
    void noResponses() {
        assertNull(DisCasClient.scanTrustBound(List.of()));
    }
}
