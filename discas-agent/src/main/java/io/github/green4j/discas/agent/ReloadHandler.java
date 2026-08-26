/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import io.github.green4j.discas.common.io.ReloadReport;

/**
 * {@code POST /v1/agent/reload} -- re-read the files this agent is serving and apply them.
 * <p>
 * The agent reads its nodes file and its TLS stores when this is called and at no other time, which
 * is what makes them safe to edit in place. Writing a file and then calling this is the whole
 * procedure; the response says, source by source, what the agent made of what it found.
 * <p>
 * {@code 200} when every source was applied or had nothing to apply, {@code 400} when one was
 * refused -- and a refusal anywhere means <b>nothing</b> was applied, so the agent is running on
 * exactly what it was running on before. Retrying a refusal without editing the file changes
 * nothing, which is why it is a 4xx and not a 503.
 */
final class ReloadHandler implements ConnectionHandler {

    static final String PREFIX = "/v1/agent/reload";

    private final ReloadableClient clientHolder;
    private String method;

    ReloadHandler(final ReloadableClient clientHolder) {
        this.clientHolder = clientHolder;
    }

    @Override
    public void onRequestLine(final Connection connection, final HttpRequest request) {
        // Cache the method while the request-line bytes are intact (a body read would compact them).
        method = request.methodString();
    }

    @Override
    public void onRequestComplete(final Connection connection, final HttpRequest request) {
        if (!"POST".equals(method)) {
            // POST rather than GET because this changes what the agent is running on, and a GET
            // that does is a GET something will eventually retry, prefetch or cache.
            connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                    .error(405, Json.error("method not allowed"));
            return;
        }
        // Synchronous, on this worker: the work is reading a few local files and parsing them, and
        // the caller has nothing to do until it knows the answer.
        final ReloadReport report = clientHolder.current().reloadFiles();
        connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                .setBodyUtf8(body(report))
                .commit(report.applied() ? 200 : 400);
    }

    /** The report as {@code {"status":..., "sources":[{source, outcome, detail}, ...]}}. */
    private static String body(final ReloadReport report) {
        final StringBuilder sources = new StringBuilder().append('[');
        boolean first = true;
        for (final ReloadReport.Entry entry : report.entries()) {
            if (!first) {
                sources.append(',');
            }
            first = false;
            sources.append(Json.object()
                    .field("source", entry.source())
                    .field("outcome", entry.outcome().label())
                    .field("detail", entry.detail())
                    .end());
        }
        sources.append(']');
        return Json.object()
                .field("status", report.applied() ? "applied" : "rejected")
                .rawField("sources", sources.toString())
                .end();
    }
}
