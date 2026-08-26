/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.observability;

import io.github.green4j.discas.common.http.server.PrefixRouter;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.io.ReloadReport;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.observability.MetricsHandler;
import io.github.green4j.discas.common.observability.ObservabilityServer;
import io.github.green4j.discas.node.PeerStateObserver;

import java.util.function.Supplier;

/**
 * The node's routes, for {@link ObservabilityServer} to serve.
 * <pre>
 * GET  /metrics   Prometheus/OpenMetrics exposition
 * GET  /health    liveness  -- 200 while recovered and the WAL is intact, else 503
 * GET  /ready     readiness -- 200 while additionally quorum-connected, else 503
 * POST /reload    re-read the node's files and apply them -- 200 applied, 400 refused
 * </pre>
 * The liveness/readiness split is the point: a {@code livenessProbe} pointed at a quorum-aware
 * endpoint turns a network partition into a cluster-wide crashloop, killing every node exactly when
 * it could still have served stale reads and rejoined. {@code /health} is therefore local-only.
 * <p>
 * The agent has no counterpart to the first three -- it holds a client, which has no view of quorum
 * -- so it serves {@code /metrics} and a reload of its own. That asymmetry is why the router is
 * built here rather than inside the shared server.
 */
public final class NodeEndpoints {

    private NodeEndpoints() {
    }

    /**
     * @param peerState the readiness source of truth; the same instance wired into the node's
     *                  observer chain, so what {@code /ready} reports is what the node actually saw
     * @param reload    what {@code POST /reload} calls, normally {@code node::reloadFiles}
     */
    public static PrefixRouter router(final NodeId nodeId,
                                      final HealthSource health,
                                      final PeerStateObserver peerState,
                                      final MetricRegistry registry,
                                      final Supplier<ReloadReport> reload) {
        return PrefixRouter.builder()
                .route("/metrics", connection -> new MetricsHandler(registry))
                .route("/health", connection ->
                        new HealthHandler(nodeId, peerState.clusterSize(), health))
                .route("/ready", connection -> new ReadyHandler(nodeId, health, peerState))
                .route("/reload", connection -> new ReloadHandler(reload))
                .build(); // default fallback: bodyless 404
    }
}
