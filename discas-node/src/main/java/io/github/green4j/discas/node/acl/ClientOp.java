/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

/**
 * The client operations an authorization grant can permit, one per {@link ClientOp}.
 * Each has a single-letter code used in the file grammar (e.g. {@code app/:GPCD}).
 *
 * <p>The letter is an explicit {@link #code()}, <b>not</b> derived from the {@code ordinal()}: an
 * ACL file outlives the enum, so reordering or inserting constants must never change what an
 * existing grant means.
 */
public enum ClientOp {
    GET('G'),
    PUT('P'),
    CAS('C'),
    DELETE('D'),
    SCAN('S');

    private final char code;

    ClientOp(final char code) {
        this.code = code;
    }

    public char code() {
        return code;
    }

    public static ClientOp fromCode(final char code) {
        final ClientOp[] all = values();
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown op code '" + code + "'");
    }
}
