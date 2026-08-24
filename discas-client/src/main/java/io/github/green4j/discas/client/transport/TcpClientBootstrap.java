/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;

import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.StderrClientObserver;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Everything {@link TcpClientTransport} needs to reach a cluster -- addresses, security, auth and
 * observability -- gathered into one value, so the transport's constructor stays one argument wide.
 * <p>
 * The addresses are every node the client may talk to, not a leader: any member can coordinate a
 * round, so the client dials one and fails over to the next. The map is copied on construction,
 * so later edits by the caller do not change an already-built bootstrap.
 */
public final class TcpClientBootstrap {
    /** Every reachable member, by id. The client dials one and fails over across the rest. */
    public final Map<NodeId, InetSocketAddress> nodeAddresses;
    /** Frame, buffer and connection limits for the client side of the wire. */
    public final ClientTransportConfig clientTransportConfig;
    /** Optional authentication token sent in CLIENT_HELLO; {@code null} for AllowAll/mTLS. */
    public final String token;
    /** Channel security; defaults to plaintext. Use a TLS provider for mTLS. */
    public final ClientSecurityProvider securityProvider;
    /**
     * Observability seam; defaults to {@link ClientObserver#NONE} (silent). Pass
     * {@link StderrClientObserver#INSTANCE} to print diagnostics, or an adapter onto a logging
     * or metrics backend.
     */
    public final ClientObserver observer;

    /** Plaintext, no token: the AllowAll case. */
    public TcpClientBootstrap(
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig clientTransportConfig) {
        this(nodeAddresses, clientTransportConfig, null, PlaintextClientSecurity.PROVIDER);
    }

    /** @param token presented in CLIENT_HELLO when the nodes run token authentication. */
    public TcpClientBootstrap(
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig clientTransportConfig,
            final String token) {
        this(nodeAddresses, clientTransportConfig, token, PlaintextClientSecurity.PROVIDER);
    }

    /** @param securityProvider a TLS provider for a TLS/mTLS client port; plaintext otherwise. */
    public TcpClientBootstrap(
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig clientTransportConfig,
            final String token,
            final ClientSecurityProvider securityProvider) {
        this(nodeAddresses, clientTransportConfig, token, securityProvider, ClientObserver.NONE);
    }

    /** @param observer where connection and request diagnostics go; may be {@code null}. */
    public TcpClientBootstrap(
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig clientTransportConfig,
            final String token,
            final ClientSecurityProvider securityProvider,
            final ClientObserver observer) {
        if (nodeAddresses == null || nodeAddresses.isEmpty()) {
            throw new IllegalArgumentException("nodeAddresses are required");
        }
        if (clientTransportConfig == null) {
            throw new IllegalArgumentException("clientTransportConfig is required");
        }
        if (securityProvider == null) {
            throw new IllegalArgumentException("securityProvider is required");
        }
        this.nodeAddresses = new HashMap<>(nodeAddresses);
        this.clientTransportConfig = clientTransportConfig;
        this.token = token;
        this.securityProvider = securityProvider;
        this.observer = observer == null ? ClientObserver.NONE : observer;
    }
}
