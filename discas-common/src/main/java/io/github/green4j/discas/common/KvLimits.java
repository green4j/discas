/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import java.nio.charset.StandardCharsets;

/**
 * The authoritative maxima for the sizes of keys, values and identities that flow through the
 * store: the one source the WAL, both transports and node-side ingress enforcement derive their
 * bounds from, so no two of them can disagree.
 *
 * <p>A maximum that also depends on a particular framing -- a WAL record, a peer message, a client
 * message -- is derived from these next to that framing.
 */
public final class KvLimits {

    /** Maximum key length in bytes. */
    public static final int MAX_KEY_BYTES = 1024 * 1024;

    /** Maximum value length in bytes. */
    public static final int MAX_VALUE_BYTES = 16 * 1024 * 1024;

    /** Maximum {@code node_id} length in UTF-8 bytes (bounds the {@link Ballot} on the wire/disk). */
    public static final int MAX_NODE_ID_BYTES = 256;

    /** Maximum {@code client_id} length in UTF-8 bytes (bounds the client message header). */
    public static final int MAX_CLIENT_ID_BYTES = 256;

    /** Maximum {@code cluster_id} length in UTF-8 bytes (bounds the PEER_HELLO payload). */
    public static final int MAX_CLUSTER_ID_BYTES = 256;

    /**
     * Maximum encoded {@link Ballot} size: a {@code long} counter plus a length-prefixed UTF-8
     * node id.
     */
    public static final int MAX_BALLOT_BYTES = Long.BYTES + Integer.BYTES + MAX_NODE_ID_BYTES;

    /**
     * Maximum entries a single scan page may return. A node clamps a client's requested limit
     * to this.
     */
    public static final int MAX_SCAN_LIMIT = 1024;

    /**
     * Soft byte budget for the entries in one scan page. A node stops filling a page once the
     * accumulated key bytes reach this and reports "more available" instead.
     * <p>
     * {@link #MAX_SCAN_LIMIT} alone does not bound a page: 1024 entries at the maximum key size
     * would still be a gigabyte. Both limits apply, so a scan response stays comfortably inside
     * ordinary transport budgets whatever the key sizes involved.
     */
    public static final int MAX_SCAN_PAGE_BYTES = 1024 * 1024;

    /**
     * Maximum key digests one anti-entropy {@code KeysResp} may carry. A node clamps a peer's
     * requested limit to this.
     */
    public static final int MAX_ANTI_ENTROPY_KEYS_PER_PAGE = 1024;

    /**
     * Soft byte budget for the keys in one anti-entropy page, applied alongside
     * {@link #MAX_ANTI_ENTROPY_KEYS_PER_PAGE} for the same reason the scan page has both: a count
     * cap does not bound a message whose keys may each be up to {@link #MAX_KEY_BYTES}.
     */
    public static final int MAX_ANTI_ENTROPY_PAGE_BYTES = 1024 * 1024;

    private KvLimits() {
    }

    /** UTF-8 byte length of {@code s}. */
    public static int utf8Length(final String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }
}
