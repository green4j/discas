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

/**
 * {@code GET /health} -- <b>liveness</b>: is this process still a working node, or does it need
 * restarting?
 * <p>
 * Deliberately local-only. It answers 200 while the node has recovered and its WAL is intact, and
 * 503 when recovery has not finished or the WAL has degraded -- conditions a restart can plausibly
 * fix. It says nothing about peers, and that is the whole point of separating it from
 * {@link ReadyHandler}: wire a Kubernetes {@code livenessProbe} to a quorum-aware endpoint and a
 * network partition stops being a partition and becomes a cluster-wide crashloop, with every node
 * killed precisely when it could still have served stale reads and rejoined.
 * <p>
 * A node that cannot reach a quorum is unhealthy to route traffic to, not unhealthy to run. That
 * distinction lives between this endpoint and {@code /ready}.
 */
final class HealthHandler implements ConnectionHandler {

    private final NodeId nodeId;
    private final int clusterSize;
    private final HealthSource health;
    private String method;

    HealthHandler(final NodeId nodeId, final int clusterSize, final HealthSource health) {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.health = health;
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
        // Viability, not usefulness: a node still asking the cluster for a promise floor is
        // healthy and must not be restarted -- it is waiting on its peers, and a restart only
        // starts the wait again. /ready is what says whether it is a member of the quorum.
        final NodeState state = health.state();
        final boolean degraded = health.walDegraded();
        final boolean live = (state == NodeState.AWAITING_FLOOR || state == NodeState.SERVING)
                && !degraded;

        final StringBuilder body = new StringBuilder(160);
        body.append("{\"status\":\"").append(live ? "ok" : "unavailable").append('"');
        JsonText.field(body, "nodeId", nodeId.value());
        body.append(",\"clusterSize\":").append(clusterSize);
        JsonText.field(body, "state", state.name());
        body.append(",\"walDegraded\":").append(degraded);
        if (degraded) {
            final String reason = health.walDegradedReason();
            JsonText.field(body, "walDegradedReason", reason == null ? "unknown" : reason);
        }
        body.append('}');

        connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                .setBodyUtf8(body)
                .commit(live ? 200 : 503);
    }
}
