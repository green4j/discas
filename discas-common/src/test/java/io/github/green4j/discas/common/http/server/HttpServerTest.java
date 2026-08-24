/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.AcceptHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ByteContent;
import io.github.green4j.discas.common.http.server.HttpServer.ChunkedResponse;
import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpHeader;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HttpServer}, driving a real server bound to an ephemeral loopback port
 * with the JDK HTTP client. No mocking framework is used: the handler is a concrete routing
 * class and the client makes real requests. Exercising the full read/parse/write path is the
 * only way to cover the internal {@code ResponseBodyDriver} (fixed + chunked) and
 * {@code Worker} without
 * fabricating socket state.
 */
@DisplayName("HttpServer - end-to-end request/response over a real loopback socket (no mocks)")
class HttpServerTest {

    private static final byte[] HELLO = "hello\n".getBytes();
    private static final int BIG = 1 << 20;          // 1 MiB fixed-length body
    private static final int CHUNK_TOTAL = 256 * 1024; // chunked body
    // mixed 1/2/3-byte UTF-8 (e-acute, euro sign), for setBodyUtf8
    private static final String TEXT = "h\u00e9llo-\u20ac\n";
    private static final String ASCII = "plain ascii body\n"; // for setBodyAscii (1 byte/char)
    private static final int SELF_LEN = 5000;         // custom BodyContent that reports its own length

    private HttpServer server;
    private HttpClient client;
    private String base;
    private DemoHandler handler;

    /** Concrete routing handler -- a real object, not a mock. */
    private static final class DemoHandler implements ConnectionHandler {
        private final byte[] big = filled(BIG, (byte) 'B');
        private final byte[] chunked = filled(CHUNK_TOTAL, (byte) 'C');
        private final byte[] small = "hi".getBytes();
        private final byte[] firstBody = "FIRST".getBytes(StandardCharsets.US_ASCII);
        private final byte[] secondBody = "SECOND".getBytes(StandardCharsets.US_ASCII);

        // Request-body backpressure coordination for the /echo-body route.
        private final AtomicReference<Connection> paused = new AtomicReference<>();
        private final AtomicInteger bodyBytes = new AtomicInteger();
        private volatile boolean pausedOnce;

        // Release-callback coordination for the /release route.
        private final AtomicInteger releases = new AtomicInteger();
        private final AtomicReference<Object> lastReleased = new AtomicReference<>();

        // /replace: records each released body object, in order; /guards: "T"/"F" per guarded call.
        private final List<Object> releasedInOrder = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<String> guardResults = new AtomicReference<>();
        // /header-only-guard: "T"/"F" per body-add attempt on a header-only (no content type) response.
        private final AtomicReference<String> headerOnlyGuardResults = new AtomicReference<>();

        // Header-API coordination. 'noStore' is a single constant value shared across all requests.
        private final HttpServer.ByteArrayContent noStore =
                new HttpServer.ByteArrayContent("no-store".getBytes(StandardCharsets.US_ASCII));
        final AtomicInteger headerReleases = new AtomicInteger();
        final AtomicReference<Throwable> lastException = new AtomicReference<>();

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
            if (r.pathEquals("/err-early")) {
                // Respond before the request body is read: even with the keep-alive flag the server
                // must close, since the undrained body would desync the next request.
                c.beginResponse(ContentType.APPLICATION_JSON, r.keepAlive())
                        .error(400, "{\"error\":\"early\"}");
                return;
            }
            // Capture routing while the path slice is valid: a large body spans multiple reads
            // (and buffer compactions), after which r.pathEquals(...) is no longer reliable.
            c.setUserState(r.pathEquals("/echo-body") ? Boolean.TRUE : null);
        }

        @Override
        public void onBodyChunk(final Connection c, final HttpRequest r,
                                final ByteBuffer buf, final int off, final int len) {
            if (Boolean.TRUE.equals(c.userState())) {
                bodyBytes.addAndGet(len);
                if (!pausedOnce) {          // pause once after the first chunk to apply backpressure
                    pausedOnce = true;
                    c.pauseInput();
                    paused.set(c);
                }
            }
        }

        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            if (Boolean.TRUE.equals(c.userState())) {
                // Echo the total number of request-body bytes actually consumed.
                final byte[] body = Integer.toString(bodyBytes.get()).getBytes(StandardCharsets.US_ASCII);
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBody(body)
                        .commit(200);
                return;
            }
            if (r.pathEquals("/big")) {
                c.beginResponse(ContentType.OCTET_STREAM, r.keepAlive())
                        .setBody(big)
                        .commit(200);
            } else if (r.pathEquals("/chunked")) {
                c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .addChunk(chunked)
                        .commit();
            } else if (r.pathEquals("/chunked-small")) {
                // Small, single chunk: exact wire framing.
                c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .addChunk(small)
                        .commit();
            } else if (r.pathEquals("/text")) {
                // Content-Length derived from a CharSequence, UTF-8 encoded straight to the wire.
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBodyUtf8(TEXT)
                        .commit(200);
            } else if (r.pathEquals("/ascii")) {
                // Content-Length == char count; one byte per char.
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBodyAscii(ASCII)
                        .commit(200);
            } else if (r.pathEquals("/ok-and-close")) {
                // Begun with the request's keep-alive (true here), then okAndClose() overrides it.
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBodyAscii("bye")
                        .okAndClose();
            } else if (r.pathEquals("/replace")) {
                // A second setBody replaces the first, releasing it (last set wins).
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBody(firstBody, () -> releasedInOrder.add(firstBody))
                        .setBody(secondBody, () -> releasedInOrder.add(secondBody))
                        .commit(200);
            } else if (r.pathEquals("/guards")) {
                // Exercise each illegal call; record whether it threw, then respond normally. The one
                // cast reaches the fixed HttpResponse view of a chunked response to call setBody*
                // (hidden by ChunkedResponse) -- it must throw at runtime via requireFixed().
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                final StringBuilder sb = new StringBuilder();
                sb.append(threw(() -> ((HttpServer.HttpResponse) resp).setBodyAscii("x"))); // setBody on chunked
                sb.append(threw(resp::commit));                   // commit before any chunk
                resp.addChunkAscii("ok");                          // first chunk (200 OK)
                resp.commit();
                sb.append(threw(() -> resp.addChunkAscii("z")));  // add after commit
                guardResults.set(sb.toString());
            } else if (r.pathEquals("/release")) {
                // Body handed back to a release listener once fully consumed into the output buffer.
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBody(small, () -> {
                            releases.incrementAndGet();
                            lastReleased.set(small);
                        })
                        .commit(200);
            } else if (r.pathEquals("/underrun")) {
                // A body that promises 100 bytes via length() but can only produce one: when the
                // driver (counting to length()) asks for the rest, the content must throw -- a framing
                // under-run the server turns into a dropped connection.
                c.beginResponse(ContentType.OCTET_STREAM, r.keepAlive())
                        .setBody(new ByteContent() {
                            @Override
                            public long length() {
                                return 100;
                            }

                            @Override
                            public int write(final int sourceOffset, final ByteBuffer target) {
                                if (sourceOffset >= 1) {
                                    throw new IllegalStateException("Only 1 byte available");
                                }
                                target.put((byte) 'x');
                                return 1;
                            }
                        })
                        .commit(200);
            } else if (r.pathEquals("/self-len")) {
                // Custom BodyContent that reports its own length: the Content-Length header is derived
                // from length().
                c.beginResponse(ContentType.OCTET_STREAM, r.keepAlive())
                        .setBody(new ByteContent() {
                            @Override
                            public long length() {
                                return SELF_LEN;
                            }

                            @Override
                            public int write(final int sourceOffset, final ByteBuffer target) {
                                final int n = Math.min(target.remaining(), SELF_LEN - sourceOffset);
                                for (int i = 0; i < n; i++) {
                                    target.put((byte) 'Z');
                                }
                                return n;
                            }
                        })
                        .commit(200);
            } else if (r.pathEquals("/headers")) {
                // A constant shared value, a dynamic CharSequence value, and a custom-content value
                // with a release callback -- none of which the caller frames with CRLF.
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .header(HttpHeader.CACHE_CONTROL, noStore)
                        .header(HttpHeader.of("X-Path"), r.pathString())
                        .header(HttpHeader.ETAG,
                                new HttpServer.ByteArrayContent("\"v1\"".getBytes(StandardCharsets.US_ASCII)),
                                headerReleases::incrementAndGet)
                        .setBody(small)
                        .commit(200);
            } else if (r.pathEquals("/hdr-crlf")) {
                final HttpServer.HttpResponse resp =
                        c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                String result = "F";
                try {
                    resp.header(HttpHeader.ETAG, "bad\r\ninjected");    // must be rejected
                } catch (final IllegalArgumentException expected) {
                    result = "T";
                }
                resp.setBodyAscii(result).commit(200);
            } else if (r.pathEquals("/hdr-overflow")) {
                // A header value larger than the whole output buffer: overflow -> onException + close.
                final byte[] huge = new byte[64 * 1024];
                Arrays.fill(huge, (byte) 'H');
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .header(HttpHeader.of("X-Huge"), new HttpServer.ByteArrayContent(huge))
                        .setBody(small)
                        .commit(200);
            } else if (r.pathEquals("/redirect")) {
                // Header-only response begun without a content type: add a Location header and commit
                // bodyless with a 301 status, keeping the connection alive.
                c.beginResponse(true)
                        .header(HttpHeader.LOCATION, "/new")
                        .commit(301);
            } else if (r.pathEquals("/header-only-guard")) {
                // A response begun without a content type is header-only: every setBody*/addBody* must
                // throw, yet headers + a bodyless commit still work.
                final HttpServer.HttpResponse resp = c.beginResponse(r.keepAlive());
                final StringBuilder sb = new StringBuilder();
                sb.append(threw(() -> resp.setBodyUtf8("x")));
                sb.append(threw(() -> resp.addBodyAscii("y")));
                sb.append(threw(() -> resp.addBody("z".getBytes(StandardCharsets.US_ASCII))));
                headerOnlyGuardResults.set(sb.toString());
                resp.header(HttpHeader.of("X-Header-Only"), "yes").commit(204);
            } else if (r.pathEquals("/err")) {
                c.beginResponse().error(404);
            } else if (r.pathEquals("/err-json")) {
                // HTTP-level error that keeps the connection alive (flag follows the request) and
                // carries a JSON body via the begun content type.
                c.beginResponse(ContentType.APPLICATION_JSON, r.keepAlive())
                        .error(400, "{\"error\":\"bad\"}");
            } else if (r.pathEquals("/err-close")) {
                // Same, but errorAndClose forces the connection shut regardless of the keep-alive flag.
                c.beginResponse(ContentType.APPLICATION_JSON, r.keepAlive())
                        .errorAndClose(500, "{\"error\":\"boom\"}");
            } else {
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
                        .setBody(HELLO)
                        .commit(200);
            }
        }

        @Override
        public void onError(final Connection c, final int status, final String reason) {
            c.beginResponse().error(status);
        }

        @Override
        public void onException(final Connection c, final Throwable cause) {
            lastException.set(cause);
        }

        @Override
        public void onClose(final Connection c) {
        }

        private static byte[] filled(final int n, final byte v) {
            final byte[] b = new byte[n];
            Arrays.fill(b, v);
            return b;
        }

        /** Run {@code r}; return "T" if it threw IllegalStateException, else "F". */
        private static String threw(final Runnable r) {
            try {
                r.run();
                return "F";
            } catch (final IllegalStateException expected) {
                return "T";
            }
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        handler = new DemoHandler();
        server = new HttpServer(HttpServer.builder().port(0).workerCount(2).build(), new AcceptHandler() { },
                connection -> handler);
        server.start();
        base = "http://127.0.0.1:" + server.boundPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    private HttpResponse<byte[]> get(final String path) throws IOException, InterruptedException {
        final java.net.http.HttpRequest req = java.net.http.HttpRequest
                .newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    @DisplayName("Small fixed-length response returns exact body and status")
    void smallResponse() throws Exception {
        final HttpResponse<byte[]> resp = get("/hello");
        assertEquals(200, resp.statusCode());
        assertEquals("hello\n", new String(resp.body()));
        assertEquals("6", resp.headers().firstValue("content-length").orElse(null));
    }

    @Test
    @DisplayName("Custom response headers are emitted; Date and Server are not; constants survive reuse")
    void customHeadersAndNoDefaults() throws Exception {
        final HttpResponse<byte[]> resp = get("/headers");
        assertEquals(200, resp.statusCode());
        assertEquals("no-store", resp.headers().firstValue("cache-control").orElse(null));
        assertEquals("/headers", resp.headers().firstValue("x-path").orElse(null));
        assertEquals("\"v1\"", resp.headers().firstValue("etag").orElse(null));
        assertTrue(resp.headers().firstValue("server").isEmpty(), "Server must not be auto-emitted");
        assertTrue(resp.headers().firstValue("date").isEmpty(), "Date must not be auto-emitted");

        // The same constant ByteContent header value renders again on later requests, and is
        // released exactly once per request -- a cursor left on it would corrupt the second render.
        for (int i = 0; i < 2; i++) {
            assertEquals("no-store", get("/headers").headers().firstValue("cache-control").orElse(null));
        }
        assertEquals(3, handler.headerReleases.get(), "Custom-content onReleased fires once per request");
    }

    @Test
    @DisplayName("A CR/LF in a header value is rejected")
    void headerValueCrlfRejected() throws Exception {
        assertEquals("T", new String(get("/hdr-crlf").body(), StandardCharsets.US_ASCII));
    }

    @Test
    @DisplayName("A header block larger than the output buffer fails the response and closes")
    void headerOverflowClosesConnection() throws Exception {
        try {
            get("/hdr-overflow");
        } catch (final IOException expected) {
            // connection dropped mid-response before any bytes were flushed
        }
        Throwable t = null;
        for (int i = 0; i < 200 && t == null; i++) {
            t = handler.lastException.get();
            if (t == null) {
                Thread.sleep(10L);
            }
        }
        assertNotNull(t, "onException should fire on header overflow");
        assertTrue(t instanceof IllegalStateException, String.valueOf(t));
        // The server survived: a fresh request still succeeds.
        assertEquals(200, get("/hello").statusCode());
    }

    @Test
    @DisplayName("Large fixed-length body (> output buffer) is streamed intact")
    void largeFixedBody() throws Exception {
        final HttpResponse<byte[]> resp = get("/big");
        assertEquals(200, resp.statusCode());
        assertEquals(BIG, resp.body().length);
        assertEquals(String.valueOf(BIG), resp.headers().firstValue("content-length").orElse(null));
        assertTrue(allEqual(resp.body(), (byte) 'B'));
    }

    @Test
    @DisplayName("Chunked body is framed and reassembled by the client to the full payload")
    void chunkedBody() throws Exception {
        final HttpResponse<byte[]> resp = get("/chunked");
        assertEquals(200, resp.statusCode());
        assertEquals(CHUNK_TOTAL, resp.body().length);
        assertTrue(allEqual(resp.body(), (byte) 'C'));
        // Chunked responses carry no Content-Length.
        assertTrue(resp.headers().firstValue("content-length").isEmpty());
    }

    @Test
    @DisplayName("okAndClose() answers 200, announces Connection: close and closes the socket")
    void okAndCloseOverridesKeepAlive() throws Exception {
        // The request asks to keep the connection, so only okAndClose() can turn it into a close --
        // and the header has to say so, or the client is told one thing and given another.
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.getOutputStream().write(
                    ("GET /ok-and-close HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
            // readAllBytes returns only on EOF, so returning at all proves the server closed.
            final String resp = new String(socket.getInputStream().readAllBytes(),
                    StandardCharsets.US_ASCII);
            assertTrue(resp.startsWith("HTTP/1.1 200"), resp);
            assertTrue(resp.toLowerCase().contains("connection: close"), resp);
            assertFalse(resp.toLowerCase().contains("connection: keep-alive"), resp);
            assertTrue(resp.endsWith("bye"), resp);
        }
    }

    @Test
    @DisplayName("Chunked framing uses a fixed-width zero-padded hex size and 0-terminator")
    void chunkedFixedWidthFraming() throws Exception {
        // Read the raw bytes (the JDK client hides chunk framing) to assert the exact wire form.
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.getOutputStream().write(
                    ("GET /chunked-small HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
            final byte[] raw = socket.getInputStream().readAllBytes();
            final String resp = new String(raw, StandardCharsets.US_ASCII);
            final int split = resp.indexOf("\r\n\r\n");
            assertTrue(split > 0, resp);
            final String body = resp.substring(split + 4);
            // 2-byte chunk "hi" framed with a 4-digit zero-padded lowercase hex size, then the
            // zero-length terminator.
            assertEquals("0002\r\nhi\r\n0\r\n\r\n", body);
        }
    }

    @Test
    @DisplayName("Chunked request body + trailers are fully consumed; next keep-alive request still parses")
    void chunkedRequestBodyThenPipelinedRequest() throws Exception {
        // First request carries a chunked body ending with a trailer; second (pipelined) is a
        // plain GET. If the terminator/trailers were not consumed, the second request would 400.
        final String reqs =
                "POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                        + "5\r\nhello\r\n0\r\nX-Trailer: v\r\n\r\n"
                        + "GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(reqs.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            final String resp = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
            assertEquals(2, TestStrings.count(resp, "HTTP/1.1 200 OK"), resp);
        }
    }

    @Test
    @DisplayName("HEAD response carries headers (incl. Content-Length) but no body")
    void headSuppressesBody() throws Exception {
        final java.net.http.HttpRequest req = java.net.http.HttpRequest
                .newBuilder(URI.create(base + "/hello"))
                .timeout(Duration.ofSeconds(10))
                .method("HEAD", java.net.http.HttpRequest.BodyPublishers.noBody())
                .build();
        final HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, resp.statusCode());
        assertEquals(0, resp.body().length, "HEAD must have no body");
        assertEquals("6", resp.headers().firstValue("content-length").orElse(null));
    }

    @Test
    @DisplayName("Expect: 100-continue yields an interim response before the final one")
    void expectContinueEmitsInterim() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5000);
            final var out = socket.getOutputStream();
            // Send headers only; withhold the body until we see the interim response.
            out.write(("POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n"
                    + "Expect: 100-continue\r\nConnection: close\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();

            final String interim = "HTTP/1.1 100 Continue\r\n\r\n";
            final String got = readExactly(socket.getInputStream(), interim.length());
            assertEquals(interim, got);

            // Now send the body; the final response should follow.
            out.write("hello".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            final String rest = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
            assertTrue(rest.startsWith("HTTP/1.1 200 OK"), rest);
        }
    }

    @Test
    @DisplayName("Request-body backpressure: handler pauses input, resumes, and the full body arrives")
    void requestBodyBackpressurePauseResume() throws Exception {
        final int total = 200 * 1024;   // > input buffer, so delivery spans reads and forces a pause
        final byte[] body = new byte[total];
        Arrays.fill(body, (byte) 'D');
        final java.net.http.HttpRequest req = java.net.http.HttpRequest
                .newBuilder(URI.create(base + "/echo-body"))
                .timeout(Duration.ofSeconds(15))
                .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        final CompletableFuture<HttpResponse<String>> future =
                client.sendAsync(req, HttpResponse.BodyHandlers.ofString());

        // The handler pauses after the first chunk; reads are suspended until we resume.
        final Connection paused = awaitPaused();

        paused.resumeInput();

        final HttpResponse<String> resp = future.get(15, TimeUnit.SECONDS);
        assertEquals(200, resp.statusCode());
        assertEquals(String.valueOf(total), resp.body(), "The entire body must be consumed after resume");
    }

    private Connection awaitPaused() throws InterruptedException {
        TestAwait.until("handler pauses input after the first body chunk",
                () -> handler.paused.get() != null);
        return handler.paused.get();
    }

    @Test
    @DisplayName("setBodyUtf8 encodes the body and derives Content-Length")
    void textBodyDerivesUtf8Length() throws Exception {
        final byte[] expected = TEXT.getBytes(StandardCharsets.UTF_8);
        final HttpResponse<byte[]> resp = get("/text");
        assertEquals(200, resp.statusCode());
        assertArrayEquals(expected, resp.body());
        assertEquals(String.valueOf(expected.length),
                resp.headers().firstValue("content-length").orElse(null));
    }

    @Test
    @DisplayName("setBodyAscii writes one byte per char and derives Content-Length == length")
    void asciiBodyDerivesCharCountLength() throws Exception {
        final HttpResponse<byte[]> resp = get("/ascii");
        assertEquals(200, resp.statusCode());
        assertEquals(ASCII, new String(resp.body(), StandardCharsets.US_ASCII));
        assertEquals(String.valueOf(ASCII.length()),
                resp.headers().firstValue("content-length").orElse(null));
    }

    @Test
    @DisplayName("Custom BodyContent reporting byteLength() derives Content-Length without declaring it")
    void selfDescribingBodyDerivesLength() throws Exception {
        final HttpResponse<byte[]> resp = get("/self-len");
        assertEquals(200, resp.statusCode());
        assertEquals(String.valueOf(SELF_LEN),
                resp.headers().firstValue("content-length").orElse(null));
        final byte[] expected = new byte[SELF_LEN];
        Arrays.fill(expected, (byte) 'Z');
        assertArrayEquals(expected, resp.body());
    }

    @Test
    @DisplayName("A second setBody replaces the first and releases it (last set wins)")
    void secondSetBodyReleasesFirst() throws Exception {
        final HttpResponse<byte[]> resp = get("/replace");
        assertEquals(200, resp.statusCode());
        assertEquals("SECOND", new String(resp.body(), StandardCharsets.US_ASCII), "Second body is sent");
        // Both bodies are released exactly once: the first when replaced, the second when consumed.
        assertEquals(List.of(handler.firstBody, handler.secondBody), handler.releasedInOrder);
    }

    @Test
    @DisplayName("Lifecycle guards reject mixing / post-endChunks / post-commit calls")
    void lifecycleGuardsRejectMisuse() throws Exception {
        final HttpResponse<byte[]> resp = get("/guards");
        assertEquals(200, resp.statusCode());
        assertEquals("ok", new String(resp.body(), StandardCharsets.US_ASCII));
        // setBody-on-chunked, addChunk-after-endChunks, add-after-commit each threw IllegalStateException.
        assertEquals("TTT", handler.guardResults.get());
    }

    @Test
    @DisplayName("Header-only response (no content type): bodyless commit with Location, no Content-Type")
    void headerOnlyRedirect() throws Exception {
        final HttpResponse<byte[]> resp = get("/redirect");
        assertEquals(301, resp.statusCode());
        assertEquals("/new", resp.headers().firstValue("location").orElse(null));
        assertEquals("0", resp.headers().firstValue("content-length").orElse(null));
        assertTrue(resp.headers().firstValue("content-type").isEmpty(),
                "A header-only response must not emit a Content-Type line");
        assertEquals(0, resp.body().length);
        // beginResponse(true) kept the connection alive: a second request over the reused client succeeds.
        assertEquals(200, get("/hello").statusCode());
    }

    @Test
    @DisplayName("Header-only response (no content type): setBody*/addBody* throw, headers+commit still work")
    void headerOnlyForbidsBody() throws Exception {
        final HttpResponse<byte[]> resp = get("/header-only-guard");
        assertEquals(204, resp.statusCode());
        assertEquals("yes", resp.headers().firstValue("x-header-only").orElse(null));
        assertTrue(resp.headers().firstValue("content-type").isEmpty(),
                "A header-only response must not emit a Content-Type line");
        // setBodyUtf8, addBodyAscii and addBody(byte[]) each threw IllegalStateException.
        assertEquals("TTT", handler.headerOnlyGuardResults.get());
    }

    @Test
    @DisplayName("Release callback fires exactly once with the original body object")
    void releaseCallbackFiresOnce() throws Exception {
        final HttpResponse<byte[]> resp = get("/release");
        assertEquals(200, resp.statusCode());
        assertEquals("hi", new String(resp.body(), StandardCharsets.US_ASCII));
        // By the time the client has the full body, the body was fully consumed -> callback fired.
        assertEquals(1, handler.releases.get(), "Release listener must fire exactly once");
        assertSame(handler.small, handler.lastReleased.get(), "Listener receives the original object");
    }

    @Test
    @DisplayName("Body under-run (content shorter than declared Content-Length) drops the connection")
    void underrunClosesConnection() {
        // The server sends the headers then closes mid-body; the client sees a truncated
        // fixed-length response and fails.
        assertThrows(IOException.class, () -> get("/underrun"));
    }

    static Stream<Arguments> errorFramings() {
        return Stream.of(
                // beginResponse() with no keep-alive argument defaults to closing, whatever the request asked.
                Arguments.of("404 through the no-argument beginResponse closes",
                        "GET /err HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 404 ", 0, true),
                // error() reuses the content type the handler had already begun (JSON here).
                Arguments.of("400 reuses the begun JSON content type",
                        "GET /err-json HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 400 ", 1, false),
                // error() follows the request's own keep-alive flag.
                Arguments.of("400 on a Connection: close request closes",
                        "GET /err-json HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
                        "HTTP/1.1 400 ", 0, true),
                // Errored at onHeadersComplete with the body still pending: it was never drained, so
                // keeping the connection would desync the stream.
                Arguments.of("400 before the body is read closes despite keep-alive",
                        "POST /err-early HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello",
                        "HTTP/1.1 400 ", 0, true),
                // errorAndClose closes whatever the request asked for.
                Arguments.of("errorAndClose closes on a keep-alive request",
                        "GET /err-close HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 500 ", 0, true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("errorFramings")
    @DisplayName("An error response frames and closes according to its cause and the request")
    void errorFraming(final String name, final String request, final String statusLine,
                      final int expectedOkCount, final boolean expectClose) throws Exception {
        // Every case pipelines the same trailing GET: whether it is answered is what says the
        // connection survived the error.
        final String reqs = request + "GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(reqs.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            final String resp = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
            assertTrue(resp.contains(statusLine), resp);
            assertEquals(expectedOkCount, TestStrings.count(resp, "HTTP/1.1 200 OK"), resp);
            // Only the error response's own headers say whether it closed -- the trailing GET always
            // carries Connection: close, so the whole stream cannot be searched for it.
            final int nextStatus = resp.indexOf("HTTP/1.1", 1);
            final String errorResponse = nextStatus < 0 ? resp : resp.substring(0, nextStatus);
            assertEquals(expectClose, errorResponse.toLowerCase().contains("connection: close"), resp);
            if (request.contains("/err-json")) {
                assertTrue(resp.toLowerCase().contains("content-type: application/json"), resp);
                assertTrue(resp.contains("{\"error\":\"bad\"}"), resp);
            }
        }
    }

    private static boolean allEqual(final byte[] b, final byte v) {
        for (final byte x : b) {
            if (x != v) {
                return false;
            }
        }
        return true;
    }

    /** Blocking read of exactly {@code n} bytes (or fewer if the peer closes / times out). */
    private static String readExactly(final InputStream in, final int n) throws IOException {
        final byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            final int r = in.read(buf, off, n - off);
            if (r < 0) {
                break;
            }
            off += r;
        }
        return new String(buf, 0, off, StandardCharsets.US_ASCII);
    }
}
