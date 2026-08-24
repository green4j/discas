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
import io.github.green4j.discas.common.http.server.HttpServer.HttpServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for {@link HttpServer}'s layered timeout system (Slowloris / slow-consumer
 * defense) and per-worker admission control. Each test starts a real server on an ephemeral
 * loopback port with a deliberately short-timeout {@link HttpServerConfig} and drives it with raw
 * {@link Socket}s so byte timing is fully controlled. No mocks.
 */
@DisplayName("HttpServer - layered timeouts + admission control (real loopback sockets)")
class HttpServerTimeoutTest {

    // Short timeouts so the suite runs fast; detection granularity is one wheel tick (~250ms).
    private static final long HEADER_MS = 400;
    private static final long BODY_MS = 400;
    private static final long REQUEST_MS = 1000;
    private static final long WRITE_MS = 400;
    private static final long KEEPALIVE_MS = 400;

    // Generous ceiling for "must have closed by now": timeout + a few ticks of slack.
    private static final long CLOSE_GRACE_MS = 3000;

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    /** Small routing handler: /big streams a large body; everything else returns a tiny body. */
    private static final class Handler implements ConnectionHandler {
        private final byte[] hello = "hello\n".getBytes(StandardCharsets.US_ASCII);
        private final byte[] big;

        Handler(final int bigSize) {
            this.big = new byte[bigSize];
            Arrays.fill(big, (byte) 'B');
        }

        @Override
        public void onOpen(final Connection c) {
        }

        @Override
        public void onRequestLine(final Connection c, final HttpRequest r) {
        }

        @Override
        public void onHeader(final Connection c, final HttpRequest r, final ByteBuffer buf,
                             final int nameOff, final int nameLen, final int valOff, final int valLen) {
        }

        @Override
        public void onHeadersComplete(final Connection c, final HttpRequest r) {
        }

        @Override
        public void onBodyChunk(final Connection c, final HttpRequest r,
                                final ByteBuffer buf, final int off, final int len) {
        }

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            if (r.pathEquals("/big")) {
                c.beginResponse(ContentType.OCTET_STREAM, r.keepAlive())
                        .setBody(big)
                        .commit(200);
            } else {
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBody(hello)
                        .commit(200);
            }
        }

        @Override
        public void onError(final Connection c, final int status, final String reason) {
            c.beginResponse().error(status);
        }

        @Override
        public void onException(final Connection c, final Throwable cause) {
        }

        @Override
        public void onClose(final Connection c) {
        }
    }

    private int start(final HttpServerConfig config, final int bigSize) throws IOException {
        server = new HttpServer(config, new AcceptHandler() { }, connection -> new Handler(bigSize));
        server.start();
        return server.boundPort();
    }

    private static HttpServerConfig shortConfig(final int maxConnectionsPerWorker) {
        return HttpServer.builder()
                .port(0).workerCount(1)
                .headerTimeoutMs(HEADER_MS).bodyTimeoutMs(BODY_MS).requestTimeoutMs(REQUEST_MS)
                .writeTimeoutMs(WRITE_MS).keepAliveTimeoutMs(KEEPALIVE_MS)
                .maxConnectionsPerWorker(maxConnectionsPerWorker)
                .build();
    }

    private static Socket connect(final int port) throws IOException {
        final Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        return socket;
    }

    /**
     * Read until the peer closes (EOF or reset), returning the elapsed millis, or {@code -1} if the
     * connection was still open after {@code timeoutMs}. Consumes and discards any bytes the server
     * sends first (e.g. a 408).
     */
    private static long millisUntilClosed(final Socket socket, final long timeoutMs) throws IOException {
        return millisUntilClosed(socket, timeoutMs, null);
    }

    /** As above, but appending whatever the server sent before closing to {@code sink}. */
    private static long millisUntilClosed(final Socket socket, final long timeoutMs,
                                          final StringBuilder sink) throws IOException {
        socket.setSoTimeout((int) timeoutMs);
        final long startNanos = System.nanoTime();
        final InputStream in = socket.getInputStream();
        final byte[] buf = new byte[8192];
        try {
            for (int n = in.read(buf); n >= 0; n = in.read(buf)) {
                if (sink != null) {
                    sink.append(new String(buf, 0, n, StandardCharsets.US_ASCII));
                }
            }
        } catch (final SocketTimeoutException e) {
            return -1; // still open
        } catch (final SocketException e) {
            // connection reset -- treat as closed
        }
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    static Stream<Arguments> stalledConnections() {
        return Stream.of(
                // [1] Slowloris: request line only, no blank line -> headers never complete.
                Arguments.of("stalled headers", "GET / HTTP/1.1\r\n", false),
                // [2] Complete headers announcing a large body, then none of it.
                Arguments.of("stalled body",
                        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\n", false),
                // [5] A served keep-alive request, then silence -> reaped by the idle timeout.
                Arguments.of("idle after a served keep-alive request",
                        "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n", true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stalledConnections")
    @DisplayName("A connection that stops making progress is reaped, whichever timer is armed")
    void stalledConnectionIsReaped(final String name, final String request,
                                   final boolean expectServed) throws Exception {
        final int port = start(shortConfig(1000), 0);
        try (Socket socket = connect(port)) {
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            final StringBuilder received = new StringBuilder();
            final long closedAfter = millisUntilClosed(socket, CLOSE_GRACE_MS, received);
            assertTrue(closedAfter >= 0, "Server must close a connection that stopped progressing");
            assertTrue(closedAfter < CLOSE_GRACE_MS, "Closed after " + closedAfter + "ms");
            if (expectServed) {
                // The idle row doubles as the proof that a normal request still completes under
                // timeouts this short.
                final String resp = received.toString();
                assertTrue(resp.startsWith("HTTP/1.1 200 OK"), resp);
                assertTrue(resp.contains("hello\n"), resp);
            }
        }
    }

    @Test
    @DisplayName("[4] Slow consumer: a response to a non-reading client is truncated at the write timeout")
    void writeTimeoutReapsSlowConsumer() throws Exception {
        final int big = 4 * 1024 * 1024; // >> socket buffers, so 'out' stays full while unread
        final int port = start(shortConfig(1000), big);
        try (Socket socket = connect(port)) {
            socket.getOutputStream().write(
                    "GET /big HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
                            .getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();

            // Do not read until well past the write timeout, so the server's out buffer stays full.
            Thread.sleep(WRITE_MS + CLOSE_GRACE_MS);

            socket.setSoTimeout((int) CLOSE_GRACE_MS);
            final byte[] received = socket.getInputStream().readAllBytes(); // drains buffered bytes, then EOF
            assertTrue(received.length < big, "Response should be truncated, got " + received.length + " bytes");
        }
    }

    @Test
    @DisplayName("Admission control: connections beyond the per-worker cap are shed at accept")
    void admissionControlShedsOverflow() throws Exception {
        // Long header timeout so admitted-but-idle connections stay open for the whole test window,
        // isolating the shed behaviour of the overflow connection.
        final HttpServerConfig config = HttpServer.builder()
                .port(0).workerCount(1)
                .headerTimeoutMs(10_000).bodyTimeoutMs(BODY_MS).requestTimeoutMs(REQUEST_MS)
                .writeTimeoutMs(WRITE_MS).keepAliveTimeoutMs(KEEPALIVE_MS)
                .maxConnectionsPerWorker(3)
                .build();
        final int port = start(config, 0);

        final List<Socket> admitted = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                admitted.add(connect(port));
                Thread.sleep(50L); // keep accept order deterministic
            }
            // The 4th connection is over the cap and must be shed promptly.
            try (Socket overflow = connect(port)) {
                final long closedAfter = millisUntilClosed(overflow, CLOSE_GRACE_MS);
                assertTrue(closedAfter >= 0, "Overflow connection must be shed");
                assertTrue(closedAfter < CLOSE_GRACE_MS, "shed after " + closedAfter + "ms");
            }
            // The admitted connections are still open (idle, under the long header timeout).
            assertEquals(-1, millisUntilClosed(admitted.get(0), 500),
                    "An admitted connection must stay open");
        } finally {
            for (final Socket socket : admitted) {
                try {
                    socket.close();
                } catch (final IOException ignore) {
                    // best-effort
                }
            }
        }
    }
}
