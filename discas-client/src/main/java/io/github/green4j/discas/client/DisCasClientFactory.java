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
import io.github.green4j.discas.client.transport.ColocatedClientBootstrap;
import io.github.green4j.discas.client.transport.ColocatedClientTransport;
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;
import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.client.transport.TcpClientTransport;

import java.util.List;

/**
 * Builds a {@link DisCasClient} from a bootstrap.
 * <p>
 * The bootstrap paths are symmetric: each takes its observer from the bootstrap, routes
 * event-loop failures into it, and honours the same {@link DisCasClientConfig}, so which
 * diagnostics a caller gets never depends on which transport it picked.
 * <p>
 * Three shapes, differing only in where the members are:
 * <ul>
 *   <li>{@link #create(ClientId, TcpClientBootstrap)} -- a plain client, every member over TCP;</li>
 *   <li>{@link #createColocated(ClientId, ColocatedClientBootstrap)} -- a client inside one of the
 *       members: that one in process, the rest over TCP;</li>
 *   <li>{@link #createInProcess(ClientId, EventLoop, List)} and
 *       {@link #create(ClientId, InProcessClientBootstrap)} -- the whole cluster in this JVM.</li>
 * </ul>
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
     * A client inside a member of the cluster: the hop to <b>that</b> member is made in process,
     * every other member is reached over TCP.
     * <p>
     * This is the shape an embedded deployment wants. The client is an ordinary cluster client --
     * same coordinator choice, same failover, same {@code scan} -- that happens to reach one of
     * the coordinators without a socket or a codec. What it asks in return is the whole
     * membership and the same credentials any client would present, because all but one of its
     * hops are ordinary client connections. See {@link ColocatedClientTransport} for the routing
     * rule and for why there is no fallback from the local path to the loopback.
     * <p>
     * The client owns the loop it runs on. Use
     * {@link #createColocated(ClientId, ColocatedClientBootstrap, EventLoop, DisCasClientConfig)}
     * to put it on the node's loop instead.
     */
    public static DisCasClient createColocated(final ClientId clientId,
                                               final ColocatedClientBootstrap bootstrap) {
        return createColocated(clientId, bootstrap, DisCasClientConfig.defaults());
    }

    /** As {@link #createColocated(ClientId, ColocatedClientBootstrap)}, with a config. */
    public static DisCasClient createColocated(final ClientId clientId,
                                               final ColocatedClientBootstrap bootstrap,
                                               final DisCasClientConfig config) {
        final ClientObserver observer = bootstrap.cluster.observer;
        final EventLoop loop = new EventLoop(loopName(clientId), observer::eventLoopTaskFailed);
        return new DisCasClient(clientId, colocatedTransport(clientId, bootstrap, loop), loop,
                true, observer, config);
    }

    /**
     * As {@link #createColocated(ClientId, ColocatedClientBootstrap)}, on the node's own loop --
     * the whole embedded deployment on one thread.
     * <p>
     * The client does <b>not</b> own that loop: {@code close()} drains pending state and leaves it
     * running for the node. Close the client before the node. The rule that completions must not
     * block now has teeth, because the thread they would block is also carrying consensus, timers
     * and peer I/O.
     */
    public static DisCasClient createColocated(final ClientId clientId,
                                               final ColocatedClientBootstrap bootstrap,
                                               final EventLoop nodeLoop,
                                               final DisCasClientConfig config) {
        return new DisCasClient(clientId, colocatedTransport(clientId, bootstrap, nodeLoop),
                nodeLoop, false, bootstrap.cluster.observer, config);
    }

    private static ClientTransport colocatedTransport(final ClientId clientId,
                                                      final ColocatedClientBootstrap bootstrap,
                                                      final EventLoop loop) {
        final TcpClientBootstrap cluster = bootstrap.cluster;
        return new ColocatedClientTransport(
                loop,
                bootstrap.localNodeId,
                cluster.nodeAddresses,
                cluster.clientTransportConfig,
                clientId,
                cluster.token,
                cluster.securityProvider,
                cluster.observer);
    }

    /**
     * A client whose whole cluster is in this JVM, sharing a node's {@code loop} and talking
     * in-process to every one of {@code peers}.
     * <p>
     * That is the single-process shape: a test, a demo, anything where the "cluster" is not really
     * distributed. A distributed deployment that merely runs a client next to one of its members
     * wants {@link #createColocated(ClientId, ColocatedClientBootstrap)} instead -- this transport
     * resolves <em>every</em> peer through the in-process registry, so a member in another JVM is
     * not reachable through it at all.
     * <p>
     * The client does <b>not</b> own the loop -- {@code close()} drains pending state but leaves
     * the loop running, so the node that owns it can shut it down. Close the client before the
     * node.
     */
    public static DisCasClient createInProcess(final ClientId clientId,
                                               final EventLoop loop,
                                               final List<NodeId> peers) {
        return createInProcess(clientId, loop, peers, ClientObserver.NONE,
                DisCasClientConfig.defaults());
    }

    /** As {@link #createInProcess(ClientId, EventLoop, List)}, with an observer and config. */
    public static DisCasClient createInProcess(final ClientId clientId,
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
