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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for the {@link ConnectionHandler} life-cycle and error hooks -- {@code onOpen},
 * {@code onError} (with the server's guaranteed fallback response), {@code onException}, and the
 * always-fired {@code onClose} -- plus the connection-policy primitives ({@code remoteAddress()},
 * early rejection). Drives a real server on an ephemeral loopback port with raw sockets so the exact
 * callback order and wire bytes can be asserted (no mocks).
 */
@DisplayName("HttpServer - connection life-cycle & error callbacks")
class HttpServerLifecycleTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    private int start(final ConnectionHandler handler) throws IOException {
        server = new HttpServer(HttpServer.builder().port(0).workerCount(1).build(), new AcceptHandler() { },
                connection -> handler);
        server.start();
        return server.boundPort();
    }

    @Test
    @DisplayName("onError fires for a malformed request and the server sends the fallback 400")
    void onErrorFallbackFor400() throws Exception {
        final LifeHandler h = new LifeHandler();          // onError records only (no response)
        final int port = start(h);
        final String resp = exchange(port, "GET\r\n\r\n"); // bare CR in the request line -> 400
        assertEquals(400, h.errorStatus);
        assertTrue(resp.startsWith("HTTP/1.1 400"), resp); // fallback response reached the wire
        assertTrue(h.events.contains("error"));
    }

    @Test
    @DisplayName("onError fires with 431 when request headers overflow the input buffer")
    void onErrorFor431() throws Exception {
        final LifeHandler h = new LifeHandler();
        final int port = start(h);
        final StringBuilder req = new StringBuilder("GET / HTTP/1.1\r\nX-Big: ");
        for (int i = 0; i < 20_000; i++) {                 // exceeds INPUT_CAP (16 KiB), no CRLF
            req.append('a');
        }
        final String resp = exchange(port, req.toString());
        assertEquals(431, h.errorStatus);                  // the onError hook observed the overflow
        // The fallback 431 may or may not survive the abrupt close of the still-uploading client
        // (RST); when it does arrive it must be the 431. Clean-FIN wire delivery is covered by the
        // 400 fallback test above.
        assertTrue(resp.isEmpty() || resp.startsWith("HTTP/1.1 431"), resp);
    }

    @Test
    @DisplayName("A handler that responds in onError overrides the fallback (single response)")
    void onErrorOverrideNoDoubleResponse() throws Exception {
        final LifeHandler h = new LifeHandler();
        h.respondInError = true;                           // onError sends the error itself
        final int port = start(h);
        final String resp = exchange(port, "GET\r\n\r\n");
        assertEquals(1, TestStrings.count(resp, "HTTP/1.1 400"), resp); // no double-send
    }

    @Test
    @DisplayName("onException fires when a callback throws, then onClose; the socket is closed")
    void onExceptionThenClose() throws Exception {
        final LifeHandler h = new LifeHandler();
        h.throwOnComplete = true;
        final int port = start(h);
        final String resp = exchange(port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        assertEquals("", resp, "No response expected after an unhandled exception");
        TestAwait.until("the connection-close event", () -> h.events.contains("close"));
        assertNotNull(h.exception);
        assertTrue(h.exception instanceof RuntimeException);
        // Ordering: the failure notification precedes the terminal close.
        assertTrue(h.events.indexOf("exception") < h.events.indexOf("close"), h.events.toString());
        assertEquals(1, TestStrings.count(String.join(",", h.events), "close"));
    }

    @Test
    @DisplayName("onClose is guaranteed for a live connection when the server shuts down")
    void onCloseGuaranteedAtShutdown() throws Exception {
        final LifeHandler h = new LifeHandler();
        final int port = start(h);
        try (Socket socket = new Socket("127.0.0.1", port)) {
            // connection is active (idle keep-alive)
            TestAwait.until("the connection-open event", () -> h.events.contains("open"));
            assertNotNull(h.remote, "remoteAddress() must be available in onOpen");
            server.close();                                 // shutdown must release live connections
            TestAwait.until("the connection-close event", () -> h.events.contains("close"));
            assertTrue(socket.isConnected());
        }
    }

    @Test
    @DisplayName("A per-IP style reject in onOpen drops the connection before any request callback")
    void rejectInOnOpen() throws Exception {
        final LifeHandler h = new LifeHandler();
        h.rejectInOpen = true;                              // hardClose() straight away in onOpen
        final int port = start(h);
        final String resp = exchange(port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        assertEquals("", resp, "Rejected connection sends nothing");
        TestAwait.until("the connection-close event", () -> h.events.contains("close"));
        assertEquals(List.of("open", "close"), h.events);   // no request callbacks in between
    }

    @Test
    @DisplayName("Rejecting in onHeadersComplete suppresses the request body (no onBodyChunk)")
    void earlyRejectSuppressesBody() throws Exception {
        final LifeHandler h = new LifeHandler();
        h.rejectInHeaders = true;                           // error(403) at onHeadersComplete
        final int port = start(h);
        final String req = "POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello";
        final String resp = exchange(port, req);
        assertTrue(resp.startsWith("HTTP/1.1 403"), resp);
        assertFalse(h.events.contains("bodyChunk"), h.events.toString());
        assertFalse(h.events.contains("requestComplete"), h.events.toString());
    }

    @Test
    @DisplayName("AcceptHandler sees onOpen -> onAccept -> onClose, all on the acceptor thread")
    void acceptHandlerLifecycle() throws Exception {
        final RecordingAcceptHandler accept = new RecordingAcceptHandler();
        server = new HttpServer(
                HttpServer.builder().port(0).workerCount(1).build(),
                accept,
                connection -> new LifeHandler(),
                runnable -> daemon(runnable, "test-acceptor"),
                runnable -> daemon(runnable, "test-worker"));
        server.start();
        final int port = server.boundPort();

        // onOpen fires when the loop starts
        TestAwait.until("the accept-loop open event", () -> accept.events.contains("open"));
        try (Socket socket = new Socket("127.0.0.1", port)) {
            // onAccept per accepted socket
            TestAwait.until("an accept event", () -> accept.events.contains("accept"));
            assertTrue(socket.isConnected());
        }
        server.close();                                       // drives the terminal onClose
        TestAwait.until("the accept-loop close event", () -> accept.events.contains("close"));

        assertEquals(List.of("open", "accept", "close"), accept.events);
        assertEquals(Set.of("test-acceptor"), Set.copyOf(accept.threadNames),
                "All AcceptHandler callbacks must run on the single acceptor thread");
    }

    @Test
    @DisplayName("AcceptHandler.onAccept returning false rejects: socket closed, connection never opened")
    void acceptHandlerRejects() throws Exception {
        final LifeHandler life = new LifeHandler();
        final List<String> acceptEvents = Collections.synchronizedList(new ArrayList<>());
        final AcceptHandler reject = new AcceptHandler() {
            @Override
            public boolean onAccept(final ServerSocketChannel serverChannel, final SocketChannel channel) {
                acceptEvents.add("accept");
                return false; // reject every connection at accept, before it reaches a worker
            }
        };
        server = new HttpServer(
                HttpServer.builder().port(0).workerCount(1).build(),
                reject,
                connection -> life,
                runnable -> daemon(runnable, "test-acceptor"),
                runnable -> daemon(runnable, "test-worker"));
        server.start();
        final int port = server.boundPort();

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000);
            boolean closed;
            try {
                closed = socket.getInputStream().read() == -1; // EOF: server closed the channel
            } catch (final SocketException reset) {
                closed = true; // abrupt close surfaced as a reset -- also a valid "rejected" signal
            }
            assertTrue(closed, "Rejected connection must be closed by the server");
        }
        TestAwait.until("the accept gate to run", () -> acceptEvents.contains("accept"));   // the accept gate ran
        assertFalse(life.events.contains("open"),       // ...but the connection never reached a worker
                "rejected connection must not fire onOpen: " + life.events);
    }

    @Test
    @DisplayName("onOpen runs before bind so a handler can set a server-socket option (pre-bind)")
    void onOpenTunesServerSocketBeforeBind() throws Exception {
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final AcceptHandler tuner = new AcceptHandler() {
            @Override
            public void onOpen(final ServerSocketChannel serverChannel) throws IOException {
                // setOption on a listening channel is only legal before bind(); this throws if the
                // channel were already bound, so a clean start proves onOpen ran pre-bind.
                serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                events.add("open");
            }
        };
        server = new HttpServer(HttpServer.builder().port(0).workerCount(1).build(), tuner,
                connection -> new LifeHandler());
        server.start();                                    // blocks until the acceptor binds (latch)
        assertTrue(server.boundPort() > 0, "boundPort must be valid once start() returns");
        assertTrue(events.contains("open"), "onOpen must have fired during start()");
        final String resp = exchange(server.boundPort(), "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        assertTrue(resp.startsWith("HTTP/1.1 200"), resp); // the tuned, bound server serves
    }

    @Test
    @DisplayName("A bind failure on the acceptor thread propagates as IOException out of start()")
    void bindFailurePropagatesFromStart() throws Exception {
        server = new HttpServer(HttpServer.builder().port(0).workerCount(1).build(), new AcceptHandler() { },
                connection -> new LifeHandler());
        server.start();
        final int port = server.boundPort();               // an actively-listening port

        // same port, no SO_REUSEADDR: bind fails
        final HttpServer second = new HttpServer(
                HttpServer.builder().port(port).workerCount(1).build(),
                new AcceptHandler() { },
                connection -> new LifeHandler());
        try {
            assertThrows(IOException.class, second::start, "Binding an in-use port must fail start()");
        } finally {
            second.close();                                // must be a clean no-op after a failed start
        }
    }

    @Test
    @DisplayName("start() is idempotent: a second call is a no-op and the server keeps serving")
    void startTwiceIsNoOp() throws Exception {
        final int port = start(new LifeHandler());
        server.start();                                    // second start(): must be a no-op
        assertEquals(port, server.boundPort(), "Port must not change on a repeated start()");
        final String resp = exchange(port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        assertTrue(resp.startsWith("HTTP/1.1 200"), resp); // still serving after the repeat
    }

    @Test
    @DisplayName("start() after close() throws IllegalStateException")
    void startAfterCloseThrows() throws Exception {
        start(new LifeHandler());
        server.close();
        assertThrows(IllegalStateException.class, () -> server.start());
    }

    @Test
    @DisplayName("close() before start() is a clean no-op and leaves the server closed")
    void closeBeforeStart() throws Exception {
        server = new HttpServer(HttpServer.builder().port(0).workerCount(1).build(), new AcceptHandler() { },
                connection -> new LifeHandler());
        server.close();                                    // never started: must not throw
        assertThrows(IllegalStateException.class, () -> server.start(), "Closed server rejects start()");
    }

    @Test
    @DisplayName("close() is idempotent: a second call returns immediately")
    void closeTwiceIsNoOp() throws Exception {
        start(new LifeHandler());
        server.close();
        server.close();                                    // must be a harmless no-op
    }

    @Test
    @DisplayName("Concurrent start() from many threads binds exactly one server")
    void concurrentStartBindsOnce() throws Exception {
        server = new HttpServer(HttpServer.builder().port(0).workerCount(1).build(), new AcceptHandler() { },
                connection -> new LifeHandler());
        final int threads = 8;
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        final List<Thread> starters = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(() -> {
                try {
                    barrier.await();                       // line all callers up on the first start()
                    server.start();
                } catch (final Throwable e) {
                    failures.add(e);
                }
            });
            starters.add(t);
            t.start();
        }
        for (final Thread t : starters) {
            t.join(5000);
        }
        assertTrue(failures.isEmpty(), "No start() should fail: " + failures);
        final int port = server.boundPort();
        final String resp = exchange(port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        assertTrue(resp.startsWith("HTTP/1.1 200"), resp); // the single bound server serves
    }

    private static Thread daemon(final Runnable runnable, final String name) {
        final Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        return thread;
    }

    /**
     * Send one raw request and read the response until the server closes the socket. Tolerant of a
     * TCP reset: an abrupt reject (server closes while inbound bytes are still unread) surfaces as a
     * {@code Connection reset} -- we return whatever bytes arrived before it.
     */
    private static String exchange(final int port, final String request) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            final InputStream in = socket.getInputStream();
            final byte[] chunk = new byte[4096];
            try {
                int n;
                while ((n = in.read(chunk)) != -1) {
                    buf.write(chunk, 0, n);
                }
            } catch (final SocketException reset) {
                // peer reset after an abrupt close: keep the bytes received so far
            }
            return buf.toString(StandardCharsets.US_ASCII);
        }
    }


    /**
     * A recording handler with toggles to exercise each hook. Shared across a single test's one
     * connection; events are collected in order.
     */
    private static final class LifeHandler implements ConnectionHandler {
        private static final byte[] OK = "OK".getBytes(StandardCharsets.US_ASCII);

        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        volatile int errorStatus = -1;
        volatile Throwable exception;
        volatile InetSocketAddress remote;

        boolean respondInError;   // onError renders its own response (overrides the fallback)
        boolean throwOnComplete;  // onRequestComplete throws -> onException path
        boolean rejectInOpen;     // onOpen hardCloses -> per-IP style admission reject
        boolean rejectInHeaders;  // onHeadersComplete error(403) -> early rejection

        @Override
        public void onOpen(final Connection c) {
            remote = c.remoteAddress();
            events.add("open");
            if (rejectInOpen) {
                c.close();
            }
        }

        @Override
        public void onRequestLine(final Connection c, final HttpRequest r) {
            events.add("requestLine");
        }

        @Override
        public void onHeader(final Connection c, final HttpRequest r, final ByteBuffer buf,
                             final int nameOff, final int nameLen, final int valOff, final int valLen) {
            events.add("header");
        }

        @Override
        public void onHeadersComplete(final Connection c, final HttpRequest r) {
            events.add("headersComplete");
            if (rejectInHeaders) {
                c.beginResponse().error(403);
            }
        }

        @Override
        public void onBodyChunk(final Connection c, final HttpRequest r,
                                final ByteBuffer buf, final int off, final int len) {
            events.add("bodyChunk");
        }

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            events.add("requestComplete");
            if (throwOnComplete) {
                throw new RuntimeException("Boom");
            }
            c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                    .setBody(OK)
                    .commit(200);
        }

        @Override
        public void onError(final Connection c, final int status, final String reason) {
            errorStatus = status;
            events.add("error");
            if (respondInError) {
                c.beginResponse().error(status);
            }
        }

        @Override
        public void onException(final Connection c, final Throwable cause) {
            exception = cause;
            events.add("exception");
        }

        @Override
        public void onClose(final Connection c) {
            events.add("close");
        }
    }

    /**
     * Records the accept-loop life-cycle events and the thread each fired on, so a test can assert
     * both the ordering and that every callback ran on the single acceptor thread.
     */
    private static final class RecordingAcceptHandler implements AcceptHandler {
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final List<String> threadNames = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onOpen(final ServerSocketChannel serverChannel) {
            threadNames.add(Thread.currentThread().getName());
            events.add("open");
        }

        @Override
        public boolean onAccept(final ServerSocketChannel serverChannel, final SocketChannel channel) {
            threadNames.add(Thread.currentThread().getName());
            events.add("accept");
            return true;
        }

        @Override
        public void onClose(final ServerSocketChannel serverChannel) {
            threadNames.add(Thread.currentThread().getName());
            events.add("close");
        }
    }
}
