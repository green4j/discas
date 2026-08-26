/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * A {@link MemberInfo} for the TCP transport: its {@link NodeId} plus the peer
 * server <em>address</em> it binds/accepts on.
 * <p>
 * The address is held <b>unresolved</b> (host + port) and resolved fresh at each
 * connection attempt via {@link #resolve()} -- so a peer whose underlying endpoint
 * changes (e.g. a rescheduled Kubernetes pod behind a stable DNS name) is dialed
 * at its current endpoint without a config reload. The host may be a DNS name or
 * a literal; we say "address" throughout, never "IP".
 */
public final class TcpMemberInfo extends MemberInfo {

    private final String host;
    private final int port;

    public TcpMemberInfo(final NodeId nodeId, final String host, final int port) {
        super(nodeId);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host required for member " + nodeId);
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("Bad port " + port + " for member " + nodeId);
        }
        this.host = host;
        this.port = port;
    }

    /**
     * Convenience: derive host+port from an {@link InetSocketAddress} without
     * pinning its resolved endpoint ({@link InetSocketAddress#getHostString()}
     * does not trigger a reverse lookup).
     */
    public TcpMemberInfo(final NodeId nodeId, final InetSocketAddress address) {
        this(nodeId, hostOf(address), portOf(address));
    }

    private static String hostOf(final InetSocketAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("address required");
        }
        return address.getHostString();
    }

    private static int portOf(final InetSocketAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("address required");
        }
        return address.getPort();
    }

    public String host() {
        return host;
    }

    @Override
    public String location() {
        return host + ":" + port;
    }

    public int port() {
        return port;
    }

    /** An unresolved address view, for logging/equality -- never triggers DNS. */
    public InetSocketAddress unresolvedAddress() {
        return InetSocketAddress.createUnresolved(host, port);
    }

    /**
     * Resolve the address to a concrete endpoint <b>now</b>. Call this immediately
     * before each connection attempt; never cache the result across attempts.
     */
    public InetSocketAddress resolve() {
        return new InetSocketAddress(host, port);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TcpMemberInfo other = (TcpMemberInfo) o;
        return port == other.port
                && nodeId().equals(other.nodeId())
                && host.equals(other.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId(), host, port);
    }

    @Override
    public String toString() {
        return "TcpMemberInfo(" + nodeId() + "," + host + ":" + port + ")";
    }
}
