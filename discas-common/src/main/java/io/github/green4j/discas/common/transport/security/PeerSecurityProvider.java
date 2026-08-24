/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.security;

/**
 * Factory for per-connection {@link PeerChannelSecurity} instances, selected by
 * configuration and wired directly (no ServiceLoader). One provider serves a
 * whole transport; it mints a fresh security context per accepted (inbound) or
 * dialled (outbound) connection so each can carry its own handshake state.
 */
public interface PeerSecurityProvider {

    /** Security context for a connection this node accepted (server role). */
    PeerChannelSecurity forInbound();

    /** Security context for a connection this node dialled (client role). */
    PeerChannelSecurity forOutbound();
}
