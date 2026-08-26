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
import io.github.green4j.discas.common.io.ReloadReport;

import java.util.function.Supplier;

/**
 * {@code POST /reload} -- re-read the files this node is serving and apply them.
 * <p>
 * The node reads its members file, its client ACL, its client-token file or directory and its TLS
 * stores when this is called and at no other time, which is what makes them safe to edit in place:
 * an editor that saves on a timer cannot be caught half-way, because nothing is reading between one
 * call and the next. Writing the files and then calling this is the whole procedure.
 * <p>
 * {@code 200} when every source was applied or had nothing to apply, {@code 400} when one was
 * refused -- and a refusal anywhere means <b>nothing</b> was applied, so the node is enforcing
 * exactly what it was enforcing before. That is the other half of the point: a members list and the
 * ACL that names it, or a key and its certificate, move together or not at all.
 * <p>
 * The only endpoint here that changes anything, which is why it is a {@code POST}: a {@code GET}
 * that reloads is a {@code GET} something will eventually retry, prefetch or cache.
 */
final class ReloadHandler implements ConnectionHandler {

    private final Supplier<ReloadReport> reload;
    private String method;

    ReloadHandler(final Supplier<ReloadReport> reload) {
        this.reload = reload;
    }

    @Override
    public void onRequestLine(final Connection connection, final HttpRequest request) {
        method = request.methodString();
    }

    @Override
    public void onRequestComplete(final Connection connection, final HttpRequest request) {
        if (!"POST".equals(method)) {
            connection.beginResponse().error(405, "method not allowed");
            return;
        }
        // Synchronous, on this worker: reading a few local files and parsing them is the work, and
        // the caller has nothing to do until it knows the answer. Consumers inside the node marshal
        // the applied value onto the event loop themselves.
        final ReloadReport report = reload.get();
        connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                .setBodyUtf8(body(report))
                .commit(report.applied() ? 200 : 400);
    }

    private static String body(final ReloadReport report) {
        final StringBuilder out = new StringBuilder(256);
        out.append("{\"status\":\"").append(report.applied() ? "applied" : "rejected").append('"');
        out.append(",\"sources\":[");
        boolean first = true;
        for (final ReloadReport.Entry entry : report.entries()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append("{\"source\":");
            JsonText.quoted(out, entry.source());
            JsonText.field(out, "outcome", entry.outcome().label());
            JsonText.field(out, "detail", entry.detail());
            out.append('}');
        }
        out.append("]}");
        return out.toString();
    }
}
