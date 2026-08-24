/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.observability;

import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.NodeState;
import io.github.green4j.discas.node.PeerStateObserver;

import java.util.List;

/**
 * {@code GET /ready} -- <b>readiness</b>: can this node serve linearizable traffic right now?
 * <p>
 * Answers 200 when the node has recovered <em>and</em> its handshaked peers plus itself reach a
 * quorum; 503 otherwise. That is the signal a load balancer or a Kubernetes {@code readinessProbe}
 * wants: a node that cannot reach a majority will fail every round it attempts, so sending clients
 * to it only converts an availability problem into a latency problem first.
 * <p>
 * Two honest limits, both worth knowing before trusting this:
 * <ul>
 *   <li>Readiness is derived from <em>completed handshakes</em>, not from liveness probes to peers.
 *       discas has no failure detector by design, so a peer that is handshaked but wedged still
 *       counts. This catches peers that are down, gone, partitioned away, or failing the handshake;
 *       it does not catch one that is merely stuck.</li>
 *   <li>A node reporting 503 here can still answer {@code ReadConsistency.SERIALIZABLE} reads from
 *       its local committed state. If stale-tolerant traffic is routed separately, this endpoint is
 *       the wrong gate for it -- the body carries the detail needed to decide.</li>
 * </ul>
 */
final class ReadyHandler implements ConnectionHandler {

    private final NodeId nodeId;
    private final HealthSource health;
    private final PeerStateObserver peerState;
    private String method;

    ReadyHandler(final NodeId nodeId, final HealthSource health, final PeerStateObserver peerState) {
        this.nodeId = nodeId;
        this.health = health;
        this.peerState = peerState;
    }

    @Override
    public void onRequestLine(final Connection connection, final HttpRequest request) {
        method = request.methodString();
    }

    @Override
    public void onRequestComplete(final Connection connection, final HttpRequest request) {
        if (!"GET".equals(method)) {
            connection.beginResponse().error(405, "method not allowed");
            return;
        }
        final NodeState state = health.state();
        final boolean serving = state == NodeState.SERVING;
        final boolean degraded = health.walDegraded();
        final boolean quorum = peerState.quorumAvailable();
        final boolean ready = serving && !degraded && quorum;

        final StringBuilder body = new StringBuilder(512);
        body.append("{\"status\":\"").append(ready ? "ready" : "not-ready").append('"');
        JsonText.field(body, "nodeId", nodeId.value());
        JsonText.field(body, "state", state.name());
        body.append(",\"walDegraded\":").append(degraded);
        body.append(",\"clusterSize\":").append(peerState.clusterSize());
        body.append(",\"quorumSize\":").append(peerState.quorumSize());
        body.append(",\"peersHandshaked\":").append(peerState.handshakedPeerCount());
        body.append(",\"quorumAvailable\":").append(quorum);
        // A node without a quorum can still answer stale reads; say so rather than leaving the
        // caller to infer it from a bare 503.
        body.append(",\"canServeStaleReads\":").append(serving && !degraded);
        appendPeers(body);
        body.append('}');

        connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                .setBodyUtf8(body)
                .commit(ready ? 200 : 503);
    }

    private void appendPeers(final StringBuilder body) {
        final List<PeerStateObserver.PeerSnapshot> peers = peerState.peerSnapshots();
        body.append(",\"peers\":[");
        for (int i = 0; i < peers.size(); i++) {
            final PeerStateObserver.PeerSnapshot peer = peers.get(i);
            if (i > 0) {
                body.append(',');
            }
            body.append("{\"id\":");
            JsonText.quoted(body, peer.peer.value());
            body.append(",\"handshaked\":").append(peer.handshaked);
            body.append(",\"changedAtMillis\":").append(peer.changedAtMillis);
            body.append(",\"ups\":").append(peer.ups);
            body.append(",\"downs\":").append(peer.downs);
            body.append('}');
        }
        body.append(']');
    }
}
