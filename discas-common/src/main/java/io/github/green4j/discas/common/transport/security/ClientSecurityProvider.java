/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.security;

/**
 * Factory for per-connection {@link ClientChannelSecurity} instances, selected by
 * configuration and wired directly (no ServiceLoader) -- the client-side analogue of
 * {@link PeerSecurityProvider}. One provider serves a whole transport and mints a fresh
 * context per accepted (inbound, server role) or dialled (outbound, client role) connection.
 */
public interface ClientSecurityProvider {

    /** Security context for a connection the node's client server accepted (server role). */
    ClientChannelSecurity forInbound();

    /** Security context for a connection the client dialled (client role). */
    ClientChannelSecurity forOutbound();
}
