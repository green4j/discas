/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * One page of a {@link DisCasClient#scan(ByteBuffer, ByteBuffer, int)}: the entries this page can
 * vouch for, plus the cursor to resume from.
 */
public final class ScanPage {
    private final List<ScanResult> results;
    private final ByteBuffer nextCursor;
    private final int respondedNodes;
    private final int clusterSize;

    ScanPage(final List<ScanResult> results, final ByteBuffer nextCursor,
             final int respondedNodes, final int clusterSize) {
        this.results = results;
        this.nextCursor = nextCursor;
        this.respondedNodes = respondedNodes;
        this.clusterSize = clusterSize;
    }

    /** How many nodes contributed to this page. */
    public int respondedNodes() {
        return respondedNodes;
    }

    /** The cluster's size {@code N} as reported by the nodes, or {@code 0} if never learned. */
    public int clusterSize() {
        return clusterSize;
    }

    /**
     * Whether a majority answered, and therefore whether this page carries the completeness
     * guarantee: every key committed before the scan started appears.
     * <p>
     * Always true under {@link ScanCoverage#QUORUM}, which fails rather than return a page
     * without it. Under {@link ScanCoverage#ANY_AVAILABLE} it may be false, and then the page
     * is a best-effort listing that can be missing committed keys.
     * <p>
     * Separate from {@link #complete()}, which is about pagination -- whether
     * more pages follow. A page can be the last one and still not be trustworthy, or be
     * trustworthy with more pages to come.
     */
    public boolean quorumReached() {
        return clusterSize > 0 && respondedNodes >= clusterSize / 2 + 1;
    }

    public List<ScanResult> results() {
        return results;
    }

    /**
     * Pass as {@code startAfter} to fetch the next page, or {@code null} when this was the
     * last page. Note the cursor is a key, not an opaque token: a scan can be resumed later,
     * though the keyspace may have changed in between (scan is not a snapshot).
     */
    public ByteBuffer nextCursor() {
        return nextCursor;
    }

    /** True when no further pages remain. */
    public boolean complete() {
        return nextCursor == null;
    }
}
