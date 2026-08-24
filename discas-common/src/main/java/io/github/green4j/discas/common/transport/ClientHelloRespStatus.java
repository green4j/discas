/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/**
 * Status of a {@link FrameCodec#TYPE_CLIENT_HELLO_RESP}. The node acting as a client server
 * writes one of these back to a connecting client right after its CLIENT_HELLO is processed.
 * <p>
 * Shaped like its peer-side counterpart: each constant carries an explicit wire
 * {@link #code() byte code}, <b>not</b> the enum {@code ordinal()}, so reordering or inserting
 * constants never shifts the protocol.
 */
public enum ClientHelloRespStatus {
    OK((byte) 0),
    SERVER_BUSY((byte) 1),
    PROTOCOL_MISMATCH((byte) 2),
    ACCESS_DENIED((byte) 3);

    private final byte code;

    ClientHelloRespStatus(final byte code) {
        this.code = code;
    }

    /** The wire byte for this status (not the ordinal). */
    public byte code() {
        return code;
    }

    /** Resolve a wire byte back to its status, or throw if unknown. */
    public static ClientHelloRespStatus fromCode(final byte code) {
        final ClientHelloRespStatus[] all = values();
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown CLIENT_HELLO_RESP status code: " + code);
    }
}
