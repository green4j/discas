/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.node.wal.Wal;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.node.transport.InProcessPeerBootstrap;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.transport.PeerTransport;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpPeerTransport;


/**
 * Builds a wired {@link DisCasNode} from a {@link NodeConfig} and a transport bootstrap.
 * <p>
 * Assembling a node by hand means creating an event loop, a peer transport and the node in the
 * right order and with the same identity threaded through all three. That is what these factories
 * do; the {@code TcpPeerBootstrap} overloads produce a networked node and the
 * {@code InProcessPeerBootstrap} ones a same-JVM cluster (tests, embedding, examples) with
 * identical semantics and no sockets.
 * <p>
 * The loop is created here rather than by the node so that its error handler can be pointed at the
 * caller's {@link NodeObserver} -- otherwise a failing loop task has nowhere to go but stderr.
 * <p>
 * The returned node is not started; call {@link DisCasNode#start()} once anything else that must
 * exist first (a client server, an ACL) has been registered.
 */
public final class DisCasNodeFactory {
    private DisCasNodeFactory() {
    }

    /** A TCP node with no observer. Timing comes from {@code cfg}. */
    public static DisCasNode create(
            final NodeConfig cfg,
            final TcpPeerBootstrap bootstrap,
            final Wal wal) {
        return create(cfg, bootstrap, wal, NodeObserver.NONE);
    }

    /**
     * @param cfg identity, quorum basis and every node timing; see {@link NodeConfig}
     * @param observer receives lifecycle, consensus and anti-entropy events, and the failures of
     *                 the event loop this factory creates. {@link NodeObserver#NONE} is silent.
     */
    public static DisCasNode create(
            final NodeConfig cfg,
            final TcpPeerBootstrap bootstrap,
            final Wal wal,
            final NodeObserver observer) {
        // Route loop failures into the observer rather than stderr; NodeObserver.NONE stays silent
        // by choice, and StderrNodeObserver prints them.
        final NodeObserver loopObserver = observer == null ? NodeObserver.NONE : observer;
        final EventLoop loop = new EventLoop("cas-node-" + cfg.nodeId,
                loopObserver::eventLoopTaskFailed);
        final PeerTransport peerTransport = new TcpPeerTransport(
                cfg.nodeId,
                cfg.clusterId,
                cfg.clusterSize,
                loop,
                bootstrap.listenSocket,
                bootstrap.members,
                bootstrap.securityProvider,
                bootstrap.config,
                observer,
                // From the WAL, not generated here: the incarnation identifies the durable state,
                // so a restart on the same directory must present the same one or every peer would
                // refuse an ordinary restart.
                wal.incarnation());
        return new DisCasNode(cfg, wal, loop, peerTransport, observer);
    }

    /**
     * Open this node's client-facing TCP endpoint from {@code bootstrap} and bind it to the
     * node: the server is closed with the node and its ingress is registered.
     * <p>
     * The peer bootstrap has always been consumed whole by {@link #create}; the client-server
     * one had no factory at all, so every caller unpacked it field by field and then repeated
     * the same three-line wiring ritual. Two of its four fields -- the authenticator and the
     * security provider -- had no reader anywhere as a result: the starter built its own and
     * passed those instead, silently discarding the bootstrap's.
     *
     * @return the transport, already registered; callers rarely need it
     */
    public static TcpClientServerTransport createClientServer(
            final DisCasNode node,
            final TcpClientServerBootstrap bootstrap) {
        final TcpClientServerTransport clientServer = new TcpClientServerTransport(
                node.loop(),
                bootstrap.listenSocket,
                bootstrap.clientTransportConfig,
                node.clusterSize(),
                bootstrap.authenticator,
                bootstrap.securityProvider);
        node.addLifecycleCloseable(clientServer);
        node.registerClientMessages(clientServer::registerIngress);
        return clientServer;
    }

    /** An in-process node with no observer. Timing comes from {@code cfg}. */
    public static DisCasNode create(
            final NodeConfig cfg,
            final InProcessPeerBootstrap bootstrap,
            final Wal wal) {
        return create(cfg, bootstrap, wal, NodeObserver.NONE);
    }

    /**
     * The in-process node additionally registers itself with {@code InProcessClientRegistry}, so a
     * co-located client can reach it by node id without a socket.
     *
     * @param cfg identity, quorum basis and every node timing -- including the repair interval,
     *            see {@link NodeConfig}.
     * @param observer receives lifecycle, consensus and anti-entropy events, and the failures of
     *                 the event loop this factory creates. {@link NodeObserver#NONE} is silent.
     */
    public static DisCasNode create(
            final NodeConfig cfg,
            final InProcessPeerBootstrap bootstrap,
            final Wal wal,
            final NodeObserver observer) {
        // Route loop failures into the observer rather than stderr; NodeObserver.NONE stays silent
        // by choice, and StderrNodeObserver prints them.
        final NodeObserver loopObserver = observer == null ? NodeObserver.NONE : observer;
        final EventLoop loop = new EventLoop("cas-node-" + cfg.nodeId,
                loopObserver::eventLoopTaskFailed);
        final PeerTransport peerTransport = new InProcessPeerTransport(
                cfg.nodeId,
                cfg.clusterSize,
                loop,
                bootstrap.members,
                loopObserver);
        final DisCasNode node = new DisCasNode(cfg, wal, loop, peerTransport, observer);
        node.registerClientMessages(registrar ->
                InProcessClientRegistry.register(cfg.nodeId, node.loop(), registrar, cfg.clusterSize));
        return node;
    }
}
