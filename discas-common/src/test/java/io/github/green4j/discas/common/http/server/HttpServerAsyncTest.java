/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.AcceptHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ChunkedResponse;
import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import io.github.green4j.discas.common.http.server.HttpServer.HttpServerConfig;
import io.github.green4j.discas.common.http.server.HttpServer.Cancelable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for the staged async response API: statusless begin, response-scoped async ops
 * ({@code resp.async(...)}, {@code c.schedule(resp, ...)}) that mark the response deferred,
 * fixed {@code commit(status)}/{@code ok()}/{@code error(status[, reason])}, incremental
 * chunked {@code addChunk}/{@code commit()}, and the worker-thread
 * {@link HttpServer.Future} composition. A real server on an ephemeral loopback port is driven by the
 * JDK HTTP client; a small executor stands in for an async data source completed off the worker thread.
 */
@DisplayName("HttpServer - staged async responses over a real loopback socket")
class HttpServerAsyncTest {

    private HttpServer server;
    private HttpClient client;
    private String base;
    private AsyncHandler handler;
    private final ExecutorService store = Executors.newFixedThreadPool(3);

    private CompletableFuture<byte[]> getAsync(final String key) {
        final CompletableFuture<byte[]> f = new CompletableFuture<>();
        store.execute(() -> {
            sleep(10);
            f.complete(key.getBytes(StandardCharsets.US_ASCII));
        });
        return f;
    }

    private CompletableFuture<byte[]> failAsync() {
        final CompletableFuture<byte[]> f = new CompletableFuture<>();
        store.execute(() -> {
            sleep(10);
            f.completeExceptionally(new IllegalStateException("Boom"));
        });
        return f;
    }

    private final class AsyncHandler implements ConnectionHandler {
        final AtomicReference<Connection> lastConnection = new AtomicReference<>();
        final AtomicBoolean mixThrew = new AtomicBoolean();

        @Override
        public void onOpen(final Connection c) {
            lastConnection.set(c);
        }

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            if (r.pathEquals("/async")) {
                // Fixed, deferred: begin in the callback, build + commit in the continuation.
                final HttpServer.HttpResponse resp = c.beginResponse(ContentType.OCTET_STREAM, r.keepAlive());
                resp.async(getAsync("async-body"))
                        .onSuccess(data -> resp.setBody(data).commit(data.length > 0 ? 200 : 404))
                        .onFailure(err -> resp.error(500));
            } else if (r.pathEquals("/report")) {
                // Chunked multi-read: first addChunk sends 200 OK, then more chunks, commit at the end.
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                resp.async(getAsync("A"))
                        .map(a -> {
                            resp.addChunk(a);
                            return "B";
                        })
                        .compose(k -> resp.async(getAsync(k)))
                        .map(b -> {
                            resp.addChunk(b);
                            return "C";
                        })
                        .compose(k -> resp.async(getAsync(k)))
                        .map(cc -> {
                            resp.addChunk(cc);
                            return null;
                        })
                        .onSuccess(x -> resp.commit())
                        .onFailure(err -> resp.close());
            } else if (r.pathEquals("/addbody")) {
                // Fixed multi-read: addBody parts, buffered, commit(200) sums the Content-Length.
                final HttpServer.HttpResponse resp = c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                resp.async(getAsync("A"))
                        .map(a -> {
                            resp.addBody(a);
                            return "B";
                        })
                        .compose(k -> resp.async(getAsync(k)))
                        .map(b -> {
                            resp.addBody(b);
                            return null;
                        })
                        .onSuccess(x -> resp.commit(200))
                        .onFailure(err -> resp.error(500));
            } else if (r.pathEquals("/all")) {
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                HttpServer.Future.all(
                                resp.async(getAsync("1")),
                                resp.async(getAsync("2")),
                                resp.async(getAsync("3")))
                        .onSuccess(parts -> {
                            resp.addChunk((byte[]) parts[0]);
                            resp.addChunk((byte[]) parts[1]);
                            resp.addChunk((byte[]) parts[2]);
                            resp.commit();
                        })
                        .onFailure(err -> resp.close());
            } else if (r.pathEquals("/fail")) {
                // Fixed build whose read fails -> error(503, reason) (buffered, nothing sent yet).
                final HttpServer.HttpResponse resp = c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                resp.async(failAsync())
                        .onSuccess(v -> resp.setBody(v).commit(200))
                        .onFailure(err -> resp.error(503, "read failed"));
            } else if (r.pathEquals("/ticks")) {
                // Chunked incremental: 10 numbers, one per scheduled tick, each flushed.
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                final int[] i = {0};
                final Runnable[] tick = new Runnable[1];
                tick[0] = () -> {
                    if (!c.isOpen()) {
                        return;
                    }
                    resp.addChunk(String.valueOf(i[0]).getBytes(StandardCharsets.US_ASCII));
                    if (++i[0] < 10) {
                        c.schedule(resp, tick[0], 15);
                    } else {
                        resp.commit();
                    }
                };
                c.schedule(resp, tick[0], 15);
            } else if (r.pathEquals("/sched-cancel")) {
                final HttpServer.HttpResponse resp = c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                final boolean[] fired = {false};
                final Cancelable h = c.schedule(resp, () -> fired[0] = true, 20);
                h.cancel();  // disarm before it can fire
                c.schedule(resp, () -> resp.setBodyAscii(fired[0] ? "FIRED" : "CANCELLED").commit(200), 80);
            } else if (r.pathEquals("/mix")) {
                // Commit synchronously, then attempt async work on the finished response -> must throw.
                final HttpServer.HttpResponse resp = c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                resp.setBodyAscii("mixed").commit(200);
                try {
                    c.execute(resp, () -> { });
                    mixThrew.set(false);
                } catch (final IllegalStateException expected) {
                    mixThrew.set(true);
                }
            } else if (r.pathEquals("/never")) {
                // Deferred but never committed (for the reaper test on a short-timeout server).
                final HttpServer.HttpResponse resp = c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                c.execute(resp, () -> { });   // marks deferred; response never commits
            } else {
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii("ok").ok();
            }
        }

        @Override
        public void onException(final Connection c, final Throwable cause) {
            // swallow: tests assert on observable HTTP behavior
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        handler = new AsyncHandler();
        server = new HttpServer(HttpServer.builder().port(0).workerCount(2).build(), new AcceptHandler() { },
                connection -> handler);
        server.start();
        base = "http://127.0.0.1:" + server.boundPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() {
        server.close();
        store.shutdownNow();
    }

    private HttpResponse<byte[]> get(final String path) throws IOException, InterruptedException {
        return client.send(java.net.http.HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    @DisplayName("Fixed deferred: begin in the callback, setBody/commit in the continuation")
    void deferredFixedResponse() throws Exception {
        final HttpResponse<byte[]> resp = get("/async");
        assertEquals(200, resp.statusCode());
        assertEquals("async-body", new String(resp.body()));
        assertEquals(200, get("/async").statusCode());   // keep-alive
    }

    @Test
    @DisplayName("Chunked multi-read: addChunk assembles parts in order")
    void chunkedMultiRead() throws Exception {
        final HttpResponse<byte[]> resp = get("/report");
        assertEquals(200, resp.statusCode());
        assertEquals("ABC", new String(resp.body()));
        assertEquals("chunked", resp.headers().firstValue("transfer-encoding").orElse(null));
        assertFalse(resp.headers().firstValue("content-length").isPresent());
    }

    @Test
    @DisplayName("Fixed multi-read: addBody parts, Content-Length is their sum, buffered")
    void fixedMultiReadAddBody() throws Exception {
        final HttpResponse<byte[]> resp = get("/addbody");
        assertEquals(200, resp.statusCode());
        assertEquals("AB", new String(resp.body()));
        assertEquals("2", resp.headers().firstValue("content-length").orElse(null));
        assertFalse(resp.headers().firstValue("transfer-encoding").isPresent());
    }

    @Test
    @DisplayName("Parallel fan-out: Future.all gathers results in order")
    void parallelFanOut() throws Exception {
        final HttpResponse<byte[]> resp = get("/all");
        assertEquals(200, resp.statusCode());
        assertEquals("123", new String(resp.body()));
    }

    @Test
    @DisplayName("Failed read -> error(503, reason) with the reason as the body")
    void fixedErrorFallback() throws Exception {
        final HttpResponse<byte[]> resp = get("/fail");
        assertEquals(503, resp.statusCode());
        assertEquals("read failed", new String(resp.body()));
        assertEquals(200, get("/ok").statusCode());   // server survived
    }

    @Test
    @DisplayName("Chunked incremental over ticks: numbers flushed one per tick")
    void chunkedIncrementalOverTicks() throws Exception {
        final HttpResponse<byte[]> resp = get("/ticks");
        assertEquals(200, resp.statusCode());
        assertEquals("0123456789", new String(resp.body()));
        assertEquals("chunked", resp.headers().firstValue("transfer-encoding").orElse(null));
    }

    @Test
    @DisplayName("Scheduler: a cancelled task never fires")
    void schedulerCancel() throws Exception {
        final HttpResponse<byte[]> resp = get("/sched-cancel");
        assertEquals(200, resp.statusCode());
        assertEquals("CANCELLED", new String(resp.body()));
    }

    @Test
    @DisplayName("Async work after a committed response throws (sync/async mix guard)")
    void mixGuard() throws Exception {
        final HttpResponse<byte[]> resp = get("/mix");
        assertEquals(200, resp.statusCode());
        assertEquals("mixed", new String(resp.body()));
        assertTrue(handler.mixThrew.get(), "Starting async work on a committed response must throw");
    }

    @Test
    @DisplayName("Connection I/O off the worker thread is rejected")
    void offThreadGuard() throws Exception {
        get("/ok");
        final Connection c = handler.lastConnection.get();
        assertNotNull(c);
        assertThrows(IllegalStateException.class, () -> c.beginResponse(ContentType.TEXT_PLAIN, true));
    }

    @Test
    @DisplayName("A deferred response that never arrives is reaped by the write timeout")
    void deferredNeverCompletesIsReaped() throws Exception {
        final HttpServerConfig cfg = HttpServer.builder()
                .port(0).workerCount(1)
                .headerTimeoutMs(10_000L).bodyTimeoutMs(30_000L).requestTimeoutMs(60_000L)
                .writeTimeoutMs(500L).keepAliveTimeoutMs(15_000L)
                .maxConnectionsPerWorker(10_000)
                .build();
        final HttpServer shortServer = new HttpServer(cfg, new AcceptHandler() { }, connection -> handler);
        shortServer.start();
        final String b = "http://127.0.0.1:" + shortServer.boundPort();
        final HttpClient c = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        try {
            final long t0 = System.nanoTime();
            assertThrows(IOException.class, () -> c.send(
                    java.net.http.HttpRequest.newBuilder(URI.create(b + "/never"))
                            .timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray()));
            assertTrue((System.nanoTime() - t0) / 1_000_000L < 4_000,
                    "Should be reaped near the write timeout");
            final HttpResponse<byte[]> ok = c.send(
                    java.net.http.HttpRequest.newBuilder(URI.create(b + "/ok"))
                            .timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, ok.statusCode());
        } finally {
            shortServer.close();
        }
    }

    private static void sleep(final long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
