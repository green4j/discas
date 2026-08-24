/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.observability;

import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.metrics.PrometheusTextFormat;

/**
 * {@code GET /metrics} -- the Prometheus/OpenMetrics text exposition of this process's registry.
 * <p>
 * Scraped directly by Prometheus or Grafana Alloy, and ingested by the OpenTelemetry Collector
 * through its {@code prometheus} receiver, so this one endpoint covers the pull-based OTel path
 * without discas taking on a dependency.
 * <p>
 * The body is rendered per request rather than cached: gauges read live state by definition, and a
 * scrape arrives every few seconds, so caching would trade correctness for an optimisation nothing
 * needs.
 */
public final class MetricsHandler implements ConnectionHandler {

    /**
     * The exposition format's own media type. Scrapers content-negotiate on it, and the version
     * parameter is part of the contract -- a bare {@code text/plain} is accepted but tells the
     * scraper nothing about which format version it is parsing.
     */
    private static final ContentType EXPOSITION =
            ContentType.of("text/plain; version=0.0.4; charset=utf-8");

    private final MetricRegistry registry;
    private String method;

    public MetricsHandler(final MetricRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onRequestLine(final Connection connection, final HttpRequest request) {
        // Cache the method while the request-line bytes are intact; a body read would compact them.
        method = request.methodString();
    }

    @Override
    public void onRequestComplete(final Connection connection, final HttpRequest request) {
        if (!"GET".equals(method)) {
            connection.beginResponse().error(405, "method not allowed");
            return;
        }
        connection.beginResponse(EXPOSITION, request.keepAlive())
                .setBodyUtf8(PrometheusTextFormat.render(registry))
                .ok();
    }
}
