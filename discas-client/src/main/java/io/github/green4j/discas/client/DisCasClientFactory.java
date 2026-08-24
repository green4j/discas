/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;
import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.client.transport.TcpClientTransport;

import java.util.List;

/**
 * Builds a {@link DisCasClient} from a bootstrap.
 * <p>
 * The two bootstrap paths are symmetric: each takes its observer from the bootstrap, routes
 * event-loop failures into it, and honours the same {@link DisCasClientConfig}, so which
 * diagnostics a caller gets never depends on which transport it picked.
 */
public final class DisCasClientFactory {
    private DisCasClientFactory() {
    }

    public static DisCasClient create(final ClientId clientId, final TcpClientBootstrap bootstrap) {
        return create(clientId, bootstrap, DisCasClientConfig.defaults());
    }

    public static DisCasClient create(final ClientId clientId, final TcpClientBootstrap bootstrap,
                                      final DisCasClientConfig config) {
        // Route loop failures into the observer rather than stderr; ClientObserver.NONE stays
        // silent by choice, and StderrClientObserver prints them.
        final ClientObserver observer = bootstrap.observer;
        final EventLoop loop = new EventLoop(loopName(clientId), observer::eventLoopTaskFailed);
        final ClientTransport transport = new TcpClientTransport(
                loop,
                bootstrap.nodeAddresses,
                bootstrap.clientTransportConfig,
                clientId,
                bootstrap.token,
                bootstrap.securityProvider,
                observer);
        return new DisCasClient(clientId, transport, loop, true, observer, config);
    }

    public static DisCasClient create(final ClientId clientId, final InProcessClientBootstrap bootstrap) {
        return create(clientId, bootstrap, DisCasClientConfig.defaults());
    }

    public static DisCasClient create(final ClientId clientId, final InProcessClientBootstrap bootstrap,
                                      final DisCasClientConfig config) {
        final ClientObserver observer = bootstrap.observer;
        final EventLoop loop = new EventLoop(loopName(clientId), observer::eventLoopTaskFailed);
        final ClientTransport transport = new InProcessClientTransport(
                loop,
                bootstrap.peers,
                clientId);
        return new DisCasClient(clientId, transport, loop, true, observer, config);
    }

    /**
     * A client colocated with a node in the same process: it shares the node's {@code loop} and
     * talks in-process to {@code peers}.
     * <p>
     * The client does <b>not</b> own the loop -- {@code close()} drains pending state but leaves
     * the loop running, so the node that owns it can shut it down. Close the client before the
     * node.
     * <p>
     * Without this overload the one caller that needs a non-owning client had to hand-build the
     * loop, transport and client, which is the only reason a bypass of this factory existed.
     */
    public static DisCasClient createColocated(final ClientId clientId,
                                               final EventLoop loop,
                                               final List<NodeId> peers) {
        return createColocated(clientId, loop, peers, ClientObserver.NONE,
                DisCasClientConfig.defaults());
    }

    /** As {@link #createColocated(ClientId, EventLoop, List)}, with an observer and config. */
    public static DisCasClient createColocated(final ClientId clientId,
                                               final EventLoop loop,
                                               final List<NodeId> peers,
                                               final ClientObserver observer,
                                               final DisCasClientConfig config) {
        final ClientTransport transport = new InProcessClientTransport(loop, peers, clientId);
        return new DisCasClient(clientId, transport, loop, false, observer, config);
    }

    /**
     * One naming rule for the client loop, rather than the literal {@code "cas-client"} repeated
     * at each construction site -- a thread dump with three clients in it was three identical
     * names.
     */
    private static String loopName(final ClientId clientId) {
        return "cas-client-" + clientId.value();
    }
}
