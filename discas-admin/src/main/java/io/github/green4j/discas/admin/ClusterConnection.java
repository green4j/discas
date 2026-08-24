/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.StderrClientObserver;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;

import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * How every {@code discas-admin} command that talks to a cluster opens its client: an ordinary
 * {@link DisCasClient}, with connection diagnostics on stderr because a tool that cannot reach a
 * node should say which one and why.
 *
 * <p>Shared because connecting is one thing, not because the commands share a configuration --
 * each still declares and resolves its own options.
 */
final class ClusterConnection {

    private ClusterConnection() {
    }

    public static DisCasClient open(final Map<NodeId, InetSocketAddress> nodes,
                                    final ClientId clientId,
                                    final String token) {
        return DisCasClientFactory.create(clientId, new TcpClientBootstrap(nodes,
                ClientTransportConfig.defaults(), token,
                PlaintextClientSecurity.PROVIDER, StderrClientObserver.INSTANCE));
    }
}
