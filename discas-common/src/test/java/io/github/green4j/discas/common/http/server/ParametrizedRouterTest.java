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
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import io.github.green4j.discas.common.http.server.ParametrizedRouter.Handler;
import io.github.green4j.discas.common.http.server.ParametrizedRouter.HandlerInitializer;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * End-to-end tests for {@link ParametrizedRouter}, driving a real {@link HttpServer} bound to an
 * ephemeral loopback port. Handlers are concrete recording classes (no mocks); each test builds its
 * own route table. A single-worker server keeps per-connection callback ordering deterministic.
 */
@DisplayName("ParametrizedRouter - single exact match with path-parameter capture over a real socket")
class ParametrizedRouterTest {

    private final List<String> events = new CopyOnWriteArrayList<>();

    private HttpServer server;
    private HttpClient client;
    private String base;

    private void start(final ParametrizedRouter router) throws IOException {
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

    // A recording matched-handler factory. It logs its lifecycle callbacks (tagged with 'name'),
    // stashes the captured parameters at onMatched, and -- reading them again at onRequestComplete --
    // renders "<paramList>\n" as the response body, proving the params stay valid across callbacks.
    private HandlerInitializer recorder(final String name) {
        return connection -> new Handler() {
            private PathParameters params;

            @Override
            public void onOpen(final Connection c) {
                events.add("onOpen:" + name);
            }

            @Override
            public void onMatched(final Connection c, final HttpRequest r, final PathParameters p) {
                events.add("onMatched:" + name);
                params = p; // stash: valid through the whole request
            }

            @Override
            public void onRequestComplete(final Connection c, final HttpRequest r) {
                events.add("onRequestComplete:" + name);
                final StringBuilder sb = new StringBuilder(name);
                for (int i = 0; i < params.size(); i++) {
                    // read via the reusable flyweight (gc-free) and only now materialize a String
                    sb.append(' ').append(params.name(i)).append('=').append(params.value(i).toString());
                }
                sb.append('\n');
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii(sb.toString()).commit(200);
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

    private static String body(final HttpResponse<byte[]> resp) {
        return new String(resp.body(), StandardCharsets.US_ASCII);
    }

    @Test
    @DisplayName("Parameters across segments are captured by name, readable in onRequestComplete")
    void singleMatchCapturesParameter() throws Exception {
        start(ParametrizedRouter.builder()
                .route("/api/{id}/users/{tab}", recorder("users"))
                .build());

        final HttpResponse<byte[]> resp = get("/api/42/users/two");
        assertEquals(200, resp.statusCode());
        assertEquals("users id=42 tab=two\n", body(resp));

        // onMatched fired in place of onRequestLine; onRequestComplete followed on the same delegate.
        assertEquals(List.of("onOpen:users", "onMatched:users", "onRequestComplete:users"),
                events.stream().filter(e -> !e.startsWith("onClose")).collect(Collectors.toList()));
    }

    @Test
    @DisplayName("A static segment is preferred over a parameter at the same position")
    void staticPreferredOverParameter() throws Exception {
        start(ParametrizedRouter.builder()
                .route("/api/health", recorder("health"))
                .route("/api/{id}", recorder("byId"))
                .build());

        assertEquals("health\n", body(get("/api/health"))); // exact static wins
        assertEquals("byId id=99\n", body(get("/api/99"))); // anything else captured as {id}
    }

    @Test
    @DisplayName("No matching expression falls through to the default 404")
    void noMatchDefault404() throws Exception {
        start(ParametrizedRouter.builder()
                .route("/api/{id}/users", recorder("users"))
                .build());

        assertEquals(404, get("/api/42/users/extra").statusCode()); // deeper: no leaf here (exact match)
        assertEquals(404, get("/api/42").statusCode());              // shorter: no leaf here
        assertEquals(404, get("/nope").statusCode());
    }

    @Test
    @DisplayName("A custom fallback handler is used for unmatched paths")
    void customFallbackHonored() throws Exception {
        start(ParametrizedRouter.builder()
                .route("/api/{id}", recorder("byId"))
                .fallback(connection -> new ConnectionHandler() {
                    @Override
                    public void onRequestComplete(final Connection c, final HttpRequest r) {
                        c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                                .setBodyAscii("fallback\n").commit(200);
                    }
                })
                .build());

        assertEquals("fallback\n", body(get("/other/path")));
        assertEquals("byId id=7\n", body(get("/api/7")));
    }

    @Test
    @DisplayName("Keep-alive across two parametrized routes: onOpen once per route, onClose to all opened")
    void keepAliveAcrossRoutes() throws Exception {
        start(ParametrizedRouter.builder()
                .route("/a/{x}", recorder("a"))
                .route("/b/{y}", recorder("b"))
                .build());

        final String requests =
                "GET /a/1 HTTP/1.1\r\nHost: x\r\n\r\n"
                        + "GET /b/2 HTTP/1.1\r\nHost: x\r\n\r\n"
                        + "GET /a/3 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        final String resp;
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(requests.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            resp = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
        }

        assertEquals(3, TestStrings.count(resp, "HTTP/1.1 200 OK"), resp);
        assertEquals(1, TestStrings.count(resp, "a x=1\n"), resp);
        assertEquals(1, TestStrings.count(resp, "b y=2\n"), resp);
        assertEquals(1, TestStrings.count(resp, "a x=3\n"), resp);

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
    @DisplayName("valueString returns cached Strings, agrees with value().toString(), null out of range")
    void valueStringCachedAndBounded() throws Exception {
        start(ParametrizedRouter.builder()
                .route("/api/{id}/users", connection -> new Handler() {
                    private PathParameters params;

                    @Override
                    public void onMatched(final Connection c, final HttpRequest r, final PathParameters p) {
                        params = p; // valid through the whole request
                    }

                    @Override
                    public void onRequestComplete(final Connection c, final HttpRequest r) {
                        final String byIndex = params.valueString(0);
                        final String byName = params.valueString("id");
                        final boolean agrees = byIndex.equals(params.value(0).toString());
                        final boolean cached = byIndex == params.valueString(0)  // same instance on re-read
                                && byName == params.valueString("id");
                        final boolean nulls = params.valueString(5) == null      // index out of range
                                && params.valueString("nope") == null;           // unknown name
                        c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                                .setBodyAscii(byIndex + '|' + byName + '|'
                                        + agrees + '|' + cached + '|' + nulls + '\n')
                                .commit(200);
                    }
                })
                .build());

        assertEquals("42|42|true|true|true\n", body(get("/api/42/users")));
    }

    @Test
    @DisplayName("A registration that cannot be routed unambiguously is rejected")
    void ambiguousRegistrationRejected() {
        // The same expression twice.
        final ParametrizedRouter.Builder duplicate =
                ParametrizedRouter.builder().route("/api/{id}", recorder("a"));
        assertThrows(IllegalArgumentException.class, () -> duplicate.route("/api/{id}", recorder("b")));

        // Two different parameter names at the same position -- the captured name would depend on
        // which route matched.
        final ParametrizedRouter.Builder conflicting =
                ParametrizedRouter.builder().route("/api/{id}/x", recorder("a"));
        assertThrows(IllegalArgumentException.class, () -> conflicting.route("/api/{name}/y", recorder("b")));
    }

}
