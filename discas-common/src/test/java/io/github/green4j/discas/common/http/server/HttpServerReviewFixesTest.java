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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end regression tests for the HttpServer review fixes (B1, B2, B4, B5, B6, B7, B9), each
 * driving a real server on an ephemeral loopback port. Raw {@link Socket}s are used where exact
 * request bytes matter (malformed framing, Expect/100-continue); the JDK client is used where a
 * de-chunked response body is convenient. No mocks.
 */
@DisplayName("HttpServer - review fixes (real loopback sockets)")
class HttpServerReviewFixesTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    private int start(final HttpServerConfig config, final ConnectionHandler handler) throws IOException {
        server = new HttpServer(config, new AcceptHandler() { }, connection -> handler);
        server.start();
        return server.boundPort();
    }

    private static HttpServerConfig config(final int maxConnPerWorker, final long maxBodyBytes) {
        // Long-ish read/write timeouts (short enough to keep the suite snappy) unless a test overrides.
        return HttpServer.builder()
                .port(0).workerCount(1)
                .headerTimeoutMs(5_000L).bodyTimeoutMs(5_000L).requestTimeoutMs(10_000L)
                .writeTimeoutMs(5_000L).keepAliveTimeoutMs(5_000L)
                .maxConnectionsPerWorker(maxConnPerWorker)
                .maxRequestBodyBytes(maxBodyBytes)
                .build();
    }

    /** Connect, send {@code request}, read every byte until the peer closes, return it as US-ASCII. */
    private static String exchangeUntilClose(final int port, final String request) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            final byte[] all = socket.getInputStream().readAllBytes();
            return new String(all, StandardCharsets.US_ASCII);
        }
    }

    // A minimal handler: 200 + tiny body for a complete request; renders the server's default error
    // otherwise. Its onHeadersComplete rejects an Expect:100-continue request routed to /reject.
    private static final class SimpleHandler implements ConnectionHandler {
        @Override
        public void onHeadersComplete(final Connection c, final HttpRequest r) {
            if (r.pathEquals("/reject")) {
                c.beginResponse().error(413);           // reject before the body / before any 100-continue
            }
        }

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii("ok\n").commit(200);
        }

        @Override
        public void onError(final Connection c, final int status, final String reason) {
            c.beginResponse().error(status);
        }
    }

    @Test
    @DisplayName("B1: Expect:100-continue rejected in onHeadersComplete -> 413 and no 100 Continue")
    void expectContinueRejectedEmitsNo100() throws Exception {
        final int port = start(config(1000, 0), new SimpleHandler());
        // Full headers (Expect + a body announced) but withhold the body; the handler rejects in
        // onHeadersComplete, so the server must answer 413 and never emit an interim 100 Continue.
        final String resp = exchangeUntilClose(port,
                "POST /reject HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n");
        assertTrue(resp.startsWith("HTTP/1.1 413"), resp);
        assertFalse(resp.contains("100 Continue"), "Must not send 100 Continue after rejecting: " + resp);
    }

    @Test
    @DisplayName("B2: a malformed chunk-size line is rejected with 400 (not treated as the last chunk)")
    void malformedChunkSizeRejected() throws Exception {
        final int port = start(config(1000, 0), new SimpleHandler());
        // "xyz" is not a hex chunk size: must be a 400, not a premature message-complete.
        final String resp = exchangeUntilClose(port,
                "POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nxyz\r\n");
        assertTrue(resp.startsWith("HTTP/1.1 400"), resp);
    }

    @Test
    @DisplayName("B7: Transfer-Encoding: chunked together with Content-Length is rejected with 400")
    void chunkedPlusContentLengthRejected() throws Exception {
        final int port = start(config(1000, 0), new SimpleHandler());
        final String resp = exchangeUntilClose(port,
                "POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n");
        assertTrue(resp.startsWith("HTTP/1.1 400"), resp);
    }

    @Test
    @DisplayName("B9: an unparseable request-version with no Connection header closes the connection")
    void malformedVersionDefaultsToClose() throws Exception {
        final int port = start(config(1000, 0), new SimpleHandler());
        // "XYZ" is not an HTTP/1.x token: the request still completes, but keep-alive must default off.
        final String resp = exchangeUntilClose(port, "GET / XYZ\r\nHost: x\r\n\r\n");
        assertTrue(resp.startsWith("HTTP/1.1 200"), resp);
        assertTrue(resp.toLowerCase().contains("connection: close"), resp);
        // exchangeUntilClose returned (read hit EOF), so the server closed the connection.
    }

    static Stream<Arguments> cappedBodies() {
        return Stream.of(
                // A fixed body over the cap, refused on its Content-Length alone.
                Arguments.of("fixed body over the cap",
                        "POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\nConnection: close\r\n\r\n",
                        "HTTP/1.1 413"),
                Arguments.of("fixed body under the cap",
                        "POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                        "HTTP/1.1 200"),
                // Chunked has no up-front length, so it is counted as it arrives: one 0x20-byte chunk is over.
                Arguments.of("chunked data over the cap",
                        "POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n20\r\n",
                        "HTTP/1.1 413"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cappedBodies")
    @DisplayName("B6: a request body over maxRequestBodyBytes is rejected with 413, however it is framed")
    void bodyCapEnforced(final String name, final String request, final String expected) throws Exception {
        final int port = start(config(1000, 10), new SimpleHandler());   // cap = 10 bytes
        final String resp = exchangeUntilClose(port, request);
        assertTrue(resp.startsWith(expected), resp);
    }

    @Test
    @DisplayName("inputBufferBytes is honored: an oversized header block overflows a small buffer (431)")
    void inputBufferBytesIsConfigurable() throws Exception {
        // A request whose (single) header line dwarfs a tiny input buffer: the headers can never fit,
        // so the server must give up with 431 rather than wedge until the header timeout.
        final String oversized = "GET /" + "a".repeat(2000)
                + " HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";

        // 256-byte input buffer: the request line alone overflows it before CRLFCRLF -> 431. The
        // server closes while the client is still uploading, so the fallback 431 may be lost to the
        // RST; assert the overflow via the onError hook (the wire 431 when it survives).
        final RecordingErrHandler small = new RecordingErrHandler();
        final int port = start(HttpServer.builder().port(0).workerCount(1)
                .inputBufferBytes(256).headerTimeoutMs(5_000L).build(), small);
        final String smallResp = exchangeTolerant(port, oversized);
        TestAwait.until("the server to report 431", () -> small.errorStatus == 431);
        assertTrue(smallResp.isEmpty() || smallResp.startsWith("HTTP/1.1 431"), smallResp);
        // The default input buffer is 16 KiB and would have served this request, so the overflow can
        // only have come from the 256 bytes configured above: the setting is wired through.
    }

    // As exchangeUntilClose, but tolerant of the connection reset that can accompany an early
    // server-side close while the client is still uploading: returns whatever bytes did arrive.
    private static String exchangeTolerant(final int port, final String request) throws IOException {
        try {
            return exchangeUntilClose(port, request);
        } catch (final SocketException reset) {
            return "";
        }
    }


    // Records the last protocol-error status the server surfaced, and (like SimpleHandler) renders it.
    private static final class RecordingErrHandler implements ConnectionHandler {
        private volatile int errorStatus;

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii("ok\n").commit(200);
        }

        @Override
        public void onError(final Connection c, final int status, final String reason) {
            errorStatus = status;
            c.beginResponse().error(status);
        }
    }

    @Test
    @DisplayName("B4: a chunked stream making steady flush progress survives past the write timeout")
    void streamingSurvivesWriteTimeoutWithProgress() throws Exception {
        final int chunks = 6;
        final byte[] payload = "abcdefgh".getBytes(StandardCharsets.US_ASCII);   // 8 bytes/chunk
        final long writeMs = 1_000L;
        // writeTimeout 1s, but the whole stream runs well past that (chunks spaced ~one wheel tick).
        final HttpServerConfig cfg = HttpServer.builder()
                .port(0).workerCount(1)
                .headerTimeoutMs(5_000L).bodyTimeoutMs(5_000L).requestTimeoutMs(30_000L)
                .writeTimeoutMs(writeMs).keepAliveTimeoutMs(5_000L)
                .maxConnectionsPerWorker(1000)
                .build();
        final int port = start(cfg, new SchedulingStreamHandler(chunks, payload, 300L));

        final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        final HttpResponse<byte[]> resp = client.send(
                java.net.http.HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/stream"))
                        .timeout(Duration.ofSeconds(30)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, resp.statusCode());
        assertEquals(chunks * payload.length, resp.body().length,
                "Full stream must arrive despite outliving the write timeout");
    }

    // Emits N chunks spaced delayMs apart via Connection.schedule (self-rescheduling), then commits.
    private static final class SchedulingStreamHandler implements ConnectionHandler {
        private final int chunks;
        private final byte[] payload;
        private final long delayMs;

        SchedulingStreamHandler(final int chunks, final byte[] payload, final long delayMs) {
            this.chunks = chunks;
            this.payload = payload;
            this.delayMs = delayMs;
        }

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            final ChunkedResponse resp = c.beginChunkedResponse(ContentType.OCTET_STREAM, false);
            final int[] i = {0};
            final Runnable[] task = new Runnable[1];
            task[0] = () -> {
                if (i[0] < chunks) {
                    resp.addChunk(payload);
                    i[0]++;
                    c.schedule(resp, task[0], delayMs);
                } else {
                    resp.commit();
                }
            };
            c.schedule(resp, task[0], delayMs);
        }

        @Override
        public void onError(final Connection c, final int status, final String reason) {
            c.beginResponse().error(status);
        }
    }

    @Test
    @DisplayName("B5: whenDrained() chains addChunk so every chunk arrives, in order")
    void whenDrainedChainsChunks() throws Exception {
        final int chunks = 50;
        final int port = start(config(1000, 0), new WhenDrainedStreamHandler(chunks));

        final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        final HttpResponse<byte[]> resp = client.send(
                java.net.http.HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/stream"))
                        .timeout(Duration.ofSeconds(30)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, resp.statusCode());

        final StringBuilder expected = new StringBuilder();
        for (int i = 0; i < chunks; i++) {
            expected.append('[').append(i).append(']');
        }
        assertEquals(expected.toString(), new String(resp.body(), StandardCharsets.US_ASCII));
    }

    // Streams N chunks, each added only once the previous has drained (via whenDrained()).
    private static final class WhenDrainedStreamHandler implements ConnectionHandler {
        private final int chunks;

        WhenDrainedStreamHandler(final int chunks) {
            this.chunks = chunks;
        }

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            final ChunkedResponse resp = c.beginChunkedResponse(ContentType.OCTET_STREAM, false);
            streamNext(resp, 0);
        }

        private void streamNext(final ChunkedResponse resp, final int i) {
            if (i == chunks) {
                resp.commit();
                return;
            }
            resp.addChunk(("[" + i + "]").getBytes(StandardCharsets.US_ASCII));
            resp.whenDrained().onSuccess(v -> streamNext(resp, i + 1));
        }

        @Override
        public void onError(final Connection c, final int status, final String reason) {
            c.beginResponse().error(status);
        }
    }
}
