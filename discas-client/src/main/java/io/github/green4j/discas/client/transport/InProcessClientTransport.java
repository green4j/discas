/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.EventLoop;

import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.client.ResponseSink;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.TransportSetupException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * A {@link ClientTransport} that reaches nodes in the same JVM, with no sockets at all: a request
 * is paired with a per-client {@link ResponseSink} that routes the answer back to this client's
 * handler on its event loop, and node endpoints are resolved through the shared
 * {@link InProcessClientRegistry}.
 */
public final class InProcessClientTransport implements ClientTransport {

    private final EventLoop clientLoop;
    private final List<NodeId> peers;
    private final ClientId clientId;
    private Consumer<ClientMessage> responseHandler;
    private volatile boolean closed = false;

    public InProcessClientTransport(final EventLoop clientLoop, final List<NodeId> peers,
                                    final ClientId clientId) {
        this.clientLoop = clientLoop;
        this.peers = Collections.unmodifiableList(new ArrayList<>(peers));
        this.clientId = clientId;
    }

    @Override
    public void send(final NodeId targetNodeId, final ClientMessage message) {
        final InProcessClientRegistry.NodeEndpoint node =
                InProcessClientRegistry.lookup(targetNodeId);
        if (node == null || node.clientIngress == null) {
            throw new TransportSetupException(TransportSetupException.Fault.UNKNOWN_TARGET,
                    "No in-process client ingress for node " + targetNodeId);
        }
        final Consumer<ClientMessage> localHandler = responseHandler;
        // Reply sink always goes through clientLoop.execute -- even when the client
        // shares the node's loop. ClientHandler invokes replySink synchronously in
        // several code paths (e.g. handleScan happy path, exception fallbacks); the
        // execute() hop defers the response one loop iteration so it never runs on
        // the same stack as the request send. When client and node share the loop,
        // this is a same-loop enqueue and effectively free
        final ResponseSink replySink = response -> {
            if (localHandler != null) {
                clientLoop.execute(() -> localHandler.accept(response));
            }
        };
        if (node.loop == clientLoop && clientLoop.inLoop()) {
            node.clientIngress.accept(clientId, message, replySink);
        } else {
            node.loop.execute(() -> node.clientIngress.accept(clientId, message, replySink));
        }
    }

    @Override
    public void register(final Consumer<ClientMessage> handler) {
        this.responseHandler = handler;
    }

    @Override
    public List<NodeId> peers() {
        return peers;
    }

    @Override
    public int clusterSize() {
        // Every node in a given cluster registers the same frozen N, so the first
        // registered peer is as authoritative as any other.
        for (int i = 0; i < peers.size(); i++) {
            final InProcessClientRegistry.NodeEndpoint node =
                    InProcessClientRegistry.lookup(peers.get(i));
            if (node != null) {
                return node.clusterSize;
            }
        }
        return 0;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        responseHandler = null;
    }
}
