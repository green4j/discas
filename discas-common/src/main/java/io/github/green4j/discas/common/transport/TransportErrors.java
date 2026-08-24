/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/**
 * Shared transport error messages used by both the peer transport (node)
 * and the client transport. Kept in {@code common} so the client module can
 * reference them without depending on the node module.
 */
public final class TransportErrors {
    public static final String ERR_TRANSPORT_CLOSED = "Transport is closed";
    public static final String ERR_CONNECT_FAILED_PREFIX = "Connect failed to peer";

    private TransportErrors() {
    }
}
