/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

/** What an {@link AuditEvent} is about; each kind fills a different part of the record. */
public enum AuditEventKind {

    /** CLIENT_HELLO accepted; the record carries the identity presented. */
    SESSION_OPENED((byte) 1),

    /** CLIENT_HELLO refused; the identity is the one claimed, which is what was not verified. */
    SESSION_REFUSED((byte) 2),

    SESSION_CLOSED((byte) 3),

    /** An operation arrived: op, key, and the value a write carries. */
    REQUEST((byte) 4),

    /** How the operation with the same {@code correlationId} ended. */
    RESULT((byte) 5),

    /** Records the buffer had no room for: the count is all that is left of them. */
    RECORDS_LOST((byte) 6);

    private static final AuditEventKind[] VALUES = values();

    private final byte code;

    AuditEventKind(final byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static AuditEventKind fromCode(final byte code) {
        final AuditEventKind[] all = VALUES;
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown audit event kind " + code);
    }
}
