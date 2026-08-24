/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.auth.AllowAllClientAuthenticator;
import io.github.green4j.discas.common.client.auth.ClientAuthenticator;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;

import java.net.InetSocketAddress;

public final class TcpClientServerBootstrap {
    /** The listener this node accepts clients on, already bound. */
    public final ListenSocket listenSocket;
    public final ClientTransportConfig clientTransportConfig;
    /** Server-wide client authentication mode; defaults to AllowAll. */
    public final ClientAuthenticator authenticator;
    /** Channel security; defaults to plaintext. Use a TLS provider for mTLS. */
    public final ClientSecurityProvider securityProvider;

    public TcpClientServerBootstrap(
            final InetSocketAddress clientBindAddress,
            final ClientTransportConfig clientTransportConfig) {
        this(clientBindAddress, clientTransportConfig, AllowAllClientAuthenticator.INSTANCE,
                PlaintextClientSecurity.PROVIDER);
    }

    public TcpClientServerBootstrap(
            final InetSocketAddress clientBindAddress,
            final ClientTransportConfig clientTransportConfig,
            final ClientAuthenticator authenticator) {
        this(clientBindAddress, clientTransportConfig, authenticator,
                PlaintextClientSecurity.PROVIDER);
    }

    public TcpClientServerBootstrap(
            final InetSocketAddress clientBindAddress,
            final ClientTransportConfig clientTransportConfig,
            final ClientAuthenticator authenticator,
            final ClientSecurityProvider securityProvider) {
        this(bindOrFail(clientBindAddress), clientTransportConfig, authenticator, securityProvider);
    }

    private static ListenSocket bindOrFail(final InetSocketAddress clientBindAddress) {
        if (clientBindAddress == null) {
            throw new IllegalArgumentException("clientBindAddress required");
        }
        return ListenSocket.bind(clientBindAddress);
    }

    /** Uses a listener that is already bound; see {@link #listenSocket}. */
    public TcpClientServerBootstrap(
            final ListenSocket listenSocket,
            final ClientTransportConfig clientTransportConfig,
            final ClientAuthenticator authenticator,
            final ClientSecurityProvider securityProvider) {
        if (listenSocket == null) {
            throw new IllegalArgumentException("listenSocket required");
        }
        if (clientTransportConfig == null) {
            throw new IllegalArgumentException("clientTransportConfig is required");
        }
        if (authenticator == null) {
            throw new IllegalArgumentException("authenticator is required");
        }
        if (securityProvider == null) {
            throw new IllegalArgumentException("securityProvider is required");
        }
        this.listenSocket = listenSocket;
        this.clientTransportConfig = clientTransportConfig;
        this.authenticator = authenticator;
        this.securityProvider = securityProvider;
    }
}
