/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.AcceptHandler;
import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionInitializer;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * End-to-end tests for {@link PrefixRouter}, driving a real {@link HttpServer} bound to an ephemeral
 * loopback port. Handlers are concrete recording classes (no mocks); each test builds its own route
 * table. A single-worker server keeps per-connection callback ordering deterministic.
 */
@DisplayName("PrefixRouter - broadcast prefix routing over a real loopback socket")
class PrefixRouterTest {

    private final List<String> events = new CopyOnWriteArrayList<>();

    private HttpServer server;
    private HttpClient client;
    private String base;

    private void start(final PrefixRouter router) throws IOException {
        server = new HttpServer(HttpServer.builder().port(0).workerCount(1).build(),
                new AcceptHandler() { }, router);
        server.start();
        base = "http://127.0.0.1:" + server.boundPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    // A recording delegate factory. It logs its lifecycle callbacks (tagged with 'name') and, when
    // 'status' is non-null, renders a response at onRequestComplete (with 'body' if given).
    private ConnectionInitializer recorder(final String name, final Integer status, final String body) {
        return connection -> new ConnectionHandler() {
            @Override
            public void onOpen(final Connection c) {
                events.add("onOpen:" + name);
            }

            @Override
            public void onRequestLine(final Connection c, final HttpRequest r) {
                events.add("onRequestLine:" + name);
            }

            @Override
            public void onRequestComplete(final Connection c, final HttpRequest r) {
                events.add("onRequestComplete:" + name);
                if (status != null) {
                    if (body != null) {
                        c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii(body).commit(status);
                    } else {
                        c.beginResponse(r.keepAlive()).commit(status);
                    }
                }
            }

            @Override
            public void onClose(final Connection c) {
                events.add("onClose:" + name);
            }
        };
    }

    private HttpResponse<byte[]> get(final String path) throws Exception {
        final java.net.http.HttpRequest req = java.net.http.HttpRequest
                .newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    @DisplayName("Every matching prefix fires, in outer->inner order; the innermost responds")
    void broadcastsToAllMatchingPrefixesInOrder() throws Exception {
        start(PrefixRouter.builder()
                .route("/", recorder("root", null, null))
                .route("/api", recorder("api", null, null))
                .route("/api/v1/users", recorder("users", 200, "users\n"))
                .build());

        final HttpResponse<byte[]> resp = get("/api/v1/users/42");
        assertEquals(200, resp.statusCode());
        assertEquals("users\n", new String(resp.body(), StandardCharsets.US_ASCII));

        // All three matched; onRequestLine reached them outer->inner (root, api, users).
        assertEquals(List.of("onRequestLine:root", "onRequestLine:api", "onRequestLine:users"),
                onlyLine(events));
        // onRequestComplete reached all three (none of the outer two responded, so no short-circuit).
        assertEquals(List.of("onRequestComplete:root", "onRequestComplete:api", "onRequestComplete:users"),
                onlyComplete(events));
    }

    @Test
    @DisplayName("onRequestComplete short-circuits once an outer handler responds")
    void shortCircuitsWhenOuterResponds() throws Exception {
        start(PrefixRouter.builder()
                .route("/", recorder("auth", 401, null))            // outer filter rejects
                .route("/api/users", recorder("users", 200, "ok\n")) // inner never gets to respond
                .build());

        final HttpResponse<byte[]> resp = get("/api/users");
        assertEquals(401, resp.statusCode());

        // Both saw the lifecycle (broadcast), but the inner handler's onRequestComplete was skipped.
        assertEquals(List.of("onRequestLine:auth", "onRequestLine:users"), onlyLine(events));
        assertEquals(List.of("onRequestComplete:auth"), onlyComplete(events));
        assertFalse(events.contains("onRequestComplete:users"), events.toString());
    }

    @Test
    @DisplayName("Prefix matching respects segment boundaries")
    void prefixMatchesWholeSegmentsOnly() throws Exception {
        start(PrefixRouter.builder()
                .route("/api", recorder("api", 200, "api\n"))
                .build());

        // "/apix" is a different segment than "/api": no match -> default 404 fallback, empty body.
        final HttpResponse<byte[]> unmatched = get("/apix");
        assertEquals(404, unmatched.statusCode());
        assertEquals(0, unmatched.body().length);
        // "/api" and any deeper path under it match.
        assertEquals("api\n", new String(get("/api").body(), StandardCharsets.US_ASCII));
        assertEquals("api\n", new String(get("/api/anything").body(), StandardCharsets.US_ASCII));
    }

    @Test
    @DisplayName("A custom fallback handler is used for unmatched paths")
    void customFallbackHonored() throws Exception {
        start(PrefixRouter.builder()
                .route("/api", recorder("api", 200, "api\n"))
                .fallback(recorder("fb", 200, "fallback\n"))
                .build());

        assertEquals("fallback\n", new String(get("/somewhere").body(), StandardCharsets.US_ASCII));
        assertEquals("api\n", new String(get("/api").body(), StandardCharsets.US_ASCII));
    }

    @Test
    @DisplayName("Keep-alive: distinct routes on one connection; onOpen once per route; onClose to all opened")
    void keepAliveAcrossRoutesOpensEachOnceAndClosesAll() throws Exception {
        start(PrefixRouter.builder()
                .route("/a", recorder("a", 200, "A\n"))
                .route("/b", recorder("b", 200, "B\n"))
                .build());

        final String requests =
                "GET /a HTTP/1.1\r\nHost: x\r\n\r\n"
                        + "GET /b HTTP/1.1\r\nHost: x\r\n\r\n"
                        + "GET /a HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        final String resp;
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(requests.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            resp = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
        }

        assertEquals(3, TestStrings.count(resp, "HTTP/1.1 200 OK"), resp);
        assertEquals(2, TestStrings.count(resp, "A\n"), resp); // /a served twice
        assertEquals(1, TestStrings.count(resp, "B\n"), resp); // /b served once

        // onOpen replayed exactly once per (connection, route), even though /a matched twice.
        assertEquals(1, TestStrings.count(events, "onOpen:a"));
        assertEquals(1, TestStrings.count(events, "onOpen:b"));

        // onClose reaches every opened delegate once after the connection is torn down.
        TestAwait.untilPresent(events, "onClose:a");
        TestAwait.untilPresent(events, "onClose:b");
        assertEquals(1, TestStrings.count(events, "onClose:a"));
        assertEquals(1, TestStrings.count(events, "onClose:b"));
    }

    @Test
    @DisplayName("Registering the same prefix twice is rejected, root included")
    void duplicateRegistrationRejected() {
        final PrefixRouter.Builder prefix = PrefixRouter.builder().route("/api", recorder("api", 200, "x"));
        assertThrows(IllegalArgumentException.class, () -> prefix.route("/api", recorder("api2", 200, "y")));

        // The root catch-all goes through a separate slot, so it needs its own guard.
        final PrefixRouter.Builder root = PrefixRouter.builder().route("/", recorder("r1", 200, "x"));
        assertThrows(IllegalArgumentException.class, () -> root.route("/", recorder("r2", 200, "y")));
    }

    private static List<String> onlyLine(final List<String> all) {
        return all.stream().filter(e -> e.startsWith("onRequestLine:")).collect(Collectors.toList());
    }

    private static List<String> onlyComplete(final List<String> all) {
        return all.stream().filter(e -> e.startsWith("onRequestComplete:")).collect(Collectors.toList());
    }

}
