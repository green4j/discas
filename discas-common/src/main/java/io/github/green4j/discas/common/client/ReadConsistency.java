/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

/**
 * Consistency level requested for a read ({@link ClientMessage.ClientGetReq}).
 * <p>
 * Each constant carries an explicit wire {@link #code() byte} (not the enum
 * {@code ordinal()}), so reordering constants never shifts the protocol.
 */
public enum ReadConsistency {

    /**
     * Linearizable read (the default): the node runs a full Paxos round so the value
     * returned reflects a state that a quorum agrees on at read time.
     */
    LINEARIZABLE((byte) 0),

    /**
     * Serializable read: the node returns its locally-committed value with no Paxos
     * round and no WAL write. Fast and cheap, but may be stale (a lagging replica) or,
     * under concurrent writers, reflect a value accepted locally that is not yet
     * cluster-confirmed. Use for read-heavy paths that tolerate bounded staleness; do
     * <b>not</b> use where a fresh read is required for correctness (e.g. lock helpers).
     */
    SERIALIZABLE((byte) 1);

    private final byte code;

    ReadConsistency(final byte code) {
        this.code = code;
    }

    /** The wire byte for this level (not the ordinal). */
    public byte code() {
        return code;
    }

    /**
     * Resolve a wire byte back to its level, or throw if unknown.
     * <p>
     * The byte is mandatory on the wire, so an unrecognised one is malformed input rather than
     * an older counterpart: both handshakes gate on {@code TransportProtocol.PROTOCOL_VERSION}.
     * A request that names no level defaults to {@link #LINEARIZABLE} at construction, never by
     * omitting the byte -- a read is not silently downgraded by a torn frame.
     */
    public static ReadConsistency fromCode(final byte code) {
        final ReadConsistency[] all = values();
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown read consistency code: " + code);
    }
}
