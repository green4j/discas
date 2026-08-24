/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;


import io.github.green4j.discas.common.Hex;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** The header names and byte/text helpers every agent route handler shares. */
final class AgentSupport {

    /** Custom request header carrying a lock token as lowercase hex. */
    static final String HEADER_LOCK_TOKEN = "X-DisCas-Lock-Token";
    /**
     * Response header carrying the key's opaque per-key version: hex of the client
     * {@code Version} token, which is why it is named after that type and not after Consul's
     * {@code X-Consul-Index}. The HTTP surface mirrors Consul where the two agree on a concept
     * ({@code Key}/{@code Value}/{@code Flags}, {@code ?cas=0}), but a version a caller round-trips
     * through {@code ?version=}, {@code ?cas=} and {@code DisCasClient.Version} must not answer to
     * two different names depending on which side of the agent you stand on.
     */
    static final String HEADER_VERSION = "X-DisCas-Version";
    /**
     * Response header on a key listing: whether a majority of nodes answered, and therefore
     * whether the listing carries its completeness guarantee (every key committed before the scan
     * started appears). Always {@code true} unless {@code ?stale} was asked for, which is the only
     * way to get a listing back at all below a majority.
     * <p>
     * Derivable from {@link #HEADER_RESPONDED} and {@link #HEADER_CLUSTER_SIZE}, and published
     * anyway: a client should not have to re-implement the quorum arithmetic to learn whether the
     * answer can be trusted. The counts are the evidence, this is the conclusion.
     */
    static final String HEADER_COMPLETE = "X-DisCas-Complete";
    /**
     * Response header on a blocking query: whether the key moved past the version the caller sent,
     * or the wait simply elapsed. Derivable by comparing {@link #HEADER_VERSION} against the
     * version sent, and published for the same reason {@link #HEADER_COMPLETE} is -- the Java
     * client answers this with {@code WatchResult.changed()} rather than making the caller
     * compare, and the HTTP surface should not be stingier.
     */
    static final String HEADER_CHANGED = "X-DisCas-Changed";
    /** Response header on a key listing: how many nodes contributed to it. */
    static final String HEADER_RESPONDED = "X-DisCas-Responded";
    /** Response header on a key listing: the cluster size {@code N} the nodes reported. */
    static final String HEADER_CLUSTER_SIZE = "X-DisCas-Cluster-Size";

    private AgentSupport() {
    }

    static String base64(final byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    static String hex(final ByteBuffer buffer) {
        return Hex.encode(buffer);
    }

    /** Decode lowercase/uppercase hex to bytes, or {@code null} if {@code s} is null or malformed. */
    static byte[] fromHex(final String s) {
        return Hex.decode(s);
    }

    static byte[] utf8(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Copy the remaining bytes of {@code buffer} into a fresh array (does not consume the buffer). */
    static byte[] toBytes(final ByteBuffer buffer) {
        final ByteBuffer b = buffer.duplicate();
        final byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }
}
