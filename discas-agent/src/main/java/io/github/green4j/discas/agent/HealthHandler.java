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
import io.github.green4j.discas.common.identity.NodeId;

import java.util.ArrayList;
import java.util.List;

/**
 * Liveness endpoint {@code GET /v1/agent/health}. Reports that the agent process is up together with
 * its identity and the nodes it is currently configured to reach. The node set is read from the
 * {@link ReloadableClient} per request, so it reflects the live membership after a nodes-file reload.
 */
final class HealthHandler implements ConnectionHandler {

    private final String clientId;
    private final ReloadableClient clientHolder;
    private String method;

    HealthHandler(final String clientId, final ReloadableClient clientHolder) {
        this.clientId = clientId;
        this.clientHolder = clientHolder;
    }

    @Override
    public void onRequestLine(final Connection connection, final HttpRequest request) {
        // Cache the method while the request-line bytes are intact (a body read would compact them).
        method = request.methodString();
    }

    @Override
    public void onRequestComplete(final Connection connection, final HttpRequest request) {
        if (!"GET".equals(method)) {
            connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                    .error(405, Json.error("method not allowed"));
            return;
        }
        connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                .setBodyUtf8(body())
                .ok();
    }

    private String body() {
        final List<String> ids = new ArrayList<>();
        for (final NodeId id : clientHolder.currentNodeIds()) {
            ids.add(id.value());
        }
        return Json.object()
                .field("status", "ok")
                .field("clientId", clientId)
                .rawField("nodes", Json.arrayOfStrings(ids))
                .end();
    }
}
