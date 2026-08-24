/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.transport.security.PlaintextPeerSecurity;
import io.github.green4j.discas.node.membership.Members;
import io.github.green4j.discas.node.membership.TcpMemberInfo;

import java.net.InetSocketAddress;

/**
 * The TCP-transport-specific wiring for a CAS node: where its peer server binds,
 * the single source of TCP counterparties ({@link Members} of {@link TcpMemberInfo}),
 * the {@link TcpTransportConfig}, and the peer channel security. The
 * transport-agnostic identity/size lives in {@code NodeConfig}, passed to
 * {@code DisCasNodeFactory} alongside this bootstrap.
 */
public final class TcpPeerBootstrap {
    /**
     * The listener this node accepts peers on, already bound. Held as a socket rather than an
     * address so a cluster can be built in two phases -- bind every node's listener, read the
     * addresses they actually got, then hand each node the resulting members map, which every
     * node validates at construction.
     */
    public final ListenSocket listenSocket;
    public final Members<TcpMemberInfo> members;
    public final TcpTransportConfig config;
    public final PeerSecurityProvider securityProvider;

    /** Plaintext peer transport (default security). */
    public TcpPeerBootstrap(
            final InetSocketAddress peerBindAddress,
            final Members<TcpMemberInfo> members,
            final TcpTransportConfig config) {
        this(peerBindAddress, members, config, PlaintextPeerSecurity.PROVIDER);
    }

    /** Plaintext peer transport on a listener that is already bound. */
    public TcpPeerBootstrap(
            final ListenSocket listenSocket,
            final Members<TcpMemberInfo> members,
            final TcpTransportConfig config) {
        this(listenSocket, members, config, PlaintextPeerSecurity.PROVIDER);
    }

    public TcpPeerBootstrap(
            final InetSocketAddress peerBindAddress,
            final Members<TcpMemberInfo> members,
            final TcpTransportConfig config,
            final PeerSecurityProvider securityProvider) {
        // Binds now, so this bootstrap always carries a listener whose address is settled.
        this(bindOrFail(peerBindAddress), members, config, securityProvider);
    }

    private static ListenSocket bindOrFail(final InetSocketAddress peerBindAddress) {
        if (peerBindAddress == null) {
            throw new IllegalArgumentException("peerBindAddress required");
        }
        return ListenSocket.bind(peerBindAddress);
    }

    /** Uses a listener that is already bound; see {@link #listenSocket}. */
    public TcpPeerBootstrap(
            final ListenSocket listenSocket,
            final Members<TcpMemberInfo> members,
            final TcpTransportConfig config,
            final PeerSecurityProvider securityProvider) {
        if (listenSocket == null) {
            throw new IllegalArgumentException("listenSocket required");
        }
        if (members == null) {
            throw new IllegalArgumentException("members required");
        }
        if (config == null) {
            throw new IllegalArgumentException("config is required");
        }
        if (securityProvider == null) {
            throw new IllegalArgumentException("securityProvider is required");
        }
        this.listenSocket = listenSocket;
        this.members = members;
        this.config = config;
        this.securityProvider = securityProvider;
    }
}
