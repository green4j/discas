/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.AcceptHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ChunkPolicy;
import io.github.green4j.discas.common.http.server.HttpServer.ChunkedResponse;
import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for gzip response support (the {@code gzip} flag on
 * {@link Connection#beginResponse(ContentType, boolean, boolean)}). Uses a raw {@link Socket} for
 * exact framing control: the JDK {@code HttpClient} neither sends {@code Accept-Encoding} nor
 * decompresses, so we set the header ourselves and inflate the body with {@link GZIPInputStream}.
 */
@DisplayName("HttpServer - gzip response encoding over a real loopback socket")
class HttpServerGzipTest {

    // A moderately compressible ~1 MiB body: numbered lines so it compresses well below its size,
    // yet the compressed stream still exceeds the 32 KiB output buffer (drains across flushes).
    private static final byte[] PAYLOAD = buildPayload();
    private static final byte[] MULTI_A = "the-first-part-".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MULTI_B = "the-second-part\n".getBytes(StandardCharsets.US_ASCII);

    private HttpServer server;
    private String host;
    private int port;

    private static final class GzipHandler implements ConnectionHandler {
        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            if (r.pathEquals("/gzip")) {
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true)
                        .setBody(PAYLOAD)
                        .ok();
            } else if (r.pathEquals("/gzip-multi")) {
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true)
                        .addBody(MULTI_A)
                        .addBody(MULTI_B)
                        .ok();
            } else if (r.pathEquals("/gzip-empty")) {
                // gzip flag set but no body -> gzip must not activate (Content-Length: 0).
                c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true)
                        .ok();
            } else if (r.pathEquals("/chunked-gzip")) {
                // Chunked + gzip, default PER_CHUNK: the whole payload as one chunk.
                c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true)
                        .addChunk(PAYLOAD)
                        .commit();
            } else if (r.pathEquals("/chunked-gzip-multi")) {
                // Chunked + gzip, several adds -> one gzip member across SYNC_FLUSH boundaries.
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true);
                resp.addChunk(MULTI_A);
                resp.addChunk(MULTI_B);
                resp.commit();
            } else if (r.pathEquals("/chunked-plain")) {
                // Chunked identity, default PER_CHUNK: two adds -> two chunks.
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
                resp.addChunk(MULTI_A);
                resp.addChunk(MULTI_B);
                resp.commit();
            } else if (r.pathEquals("/bysize-gzip")) {
                // Chunked + gzip, bySize: many tiny adds coalesced into far fewer, larger chunks.
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive(),
                        true, ChunkPolicy.bySize(4096));
                addInPieces(resp, PAYLOAD, 256);
                resp.commit();
            } else if (r.pathEquals("/bysize-plain")) {
                // Chunked identity, bySize: many tiny adds coalesced into ~4 KiB chunks.
                final ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive(),
                        false, ChunkPolicy.bySize(4096));
                addInPieces(resp, PAYLOAD, 256);
                resp.commit();
            } else {
                c.beginResponse(r.keepAlive()).error(404);
            }
        }

        private static void addInPieces(final ChunkedResponse resp, final byte[] data, final int piece) {
            for (int off = 0; off < data.length; off += piece) {
                resp.addChunk(data, off, Math.min(piece, data.length - off));
            }
        }
    }

    // Number of addChunk pieces /bysize-* split PAYLOAD into (PAYLOAD length / 256, rounded up).
    private static final int BYSIZE_PIECES = (PAYLOAD.length + 255) / 256;

    @BeforeEach
    void setUp() throws IOException {
        server = new HttpServer(HttpServer.builder().port(0).workerCount(2).build(), new AcceptHandler() { },
                connection -> new GzipHandler());
        server.start();
        host = "127.0.0.1";
        port = server.boundPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    /** The fixed-length and chunked framings of the same route pair, driven by one boolean. */
    private static String route(final boolean chunked, final String fixed) {
        return chunked ? "/chunked-" + fixed.substring(1) : fixed;
    }

    /** Framing is the axis these tests are parameterized on; the encoding assertions are shared. */
    private static void assertFraming(final RawResponse resp, final boolean chunked, final int bodyLength) {
        if (chunked) {
            assertEquals("chunked", resp.header("transfer-encoding"));
            assertNull(resp.header("content-length"), "Chunked responses carry no Content-Length");
        } else {
            assertEquals(bodyLength, Integer.parseInt(resp.header("content-length")));
        }
    }

    @ParameterizedTest(name = "chunked={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("Accept-Encoding: gzip -> compressed body, correct framing, inflates to the original")
    void negotiatedGzip(final boolean chunked) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            final RawResponse resp = request(socket, "GET", route(chunked, "/gzip"), true, false);
            assertEquals(200, resp.status);
            assertEquals("gzip", resp.header("content-encoding"));
            assertEquals("Accept-Encoding", resp.header("vary"));
            assertFraming(resp, chunked, resp.body.length);
            assertTrue(resp.body.length < PAYLOAD.length, "Expected compression to shrink the body");
            assertTrue(resp.body.length > 32 * 1024, "Compressed stream should exceed the output buffer");
            assertArrayEquals(PAYLOAD, gunzip(resp.body));
        }
    }

    @ParameterizedTest(name = "chunked={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("No Accept-Encoding -> flag no-ops: identity body, no Content-Encoding")
    void noAcceptEncoding(final boolean chunked) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            final RawResponse resp = request(socket, "GET", route(chunked, "/gzip"), false, false);
            assertEquals(200, resp.status);
            assertNull(resp.header("content-encoding"));
            assertNull(resp.header("vary"));
            assertFraming(resp, chunked, PAYLOAD.length);
            assertArrayEquals(PAYLOAD, resp.body);
        }
    }

    @ParameterizedTest(name = "chunked={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("Multi-part body compresses all parts into one gzip stream")
    void multiPartBody(final boolean chunked) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            final RawResponse resp = request(socket, "GET", route(chunked, "/gzip-multi"), true, false);
            assertEquals(200, resp.status);
            assertEquals("gzip", resp.header("content-encoding"));
            assertArrayEquals(concat(MULTI_A, MULTI_B), gunzip(resp.body));
        }
    }

    @ParameterizedTest(name = "chunked={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("HEAD -> the GET's gzip headers and length, but no body")
    void headMatchesGetLength(final boolean chunked) throws IOException {
        final String path = route(chunked, "/gzip");
        final int compressedLen;
        try (Socket socket = new Socket(host, port)) {
            compressedLen = request(socket, "GET", path, true, false).body.length;
        }
        try (Socket socket = new Socket(host, port)) {
            final RawResponse head = request(socket, "HEAD", path, true, false);
            assertEquals(200, head.status);
            assertEquals("gzip", head.header("content-encoding"));
            assertFraming(head, chunked, compressedLen);
            assertEquals(0, head.body.length);
        }
    }

    @Test
    @DisplayName("Empty body -> gzip inactive, Content-Length: 0, no Content-Encoding")
    void emptyBodyGzipInactive() throws IOException {
        try (Socket socket = new Socket(host, port)) {
            final RawResponse resp = request(socket, "GET", "/gzip-empty", true, false);
            assertEquals(200, resp.status);
            assertNull(resp.header("content-encoding"));
            assertEquals(0, Integer.parseInt(resp.header("content-length")));
            assertEquals(0, resp.body.length);
        }
    }

    @ParameterizedTest(name = "chunked={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("Two pipelined gzip requests on one keep-alive connection both inflate correctly")
    void keepAliveReusesEncoder(final boolean chunked) throws IOException {
        // A deflater that is not reset between responses produces a second body that will not inflate.
        final String path = route(chunked, "/gzip-multi");
        final byte[] expected = concat(MULTI_A, MULTI_B);
        try (Socket socket = new Socket(host, port)) {
            final RawResponse first = request(socket, "GET", path, true, true);
            assertEquals("gzip", first.header("content-encoding"));
            assertArrayEquals(expected, gunzip(first.body));

            final RawResponse second = request(socket, "GET", path, true, true);
            assertEquals("gzip", second.header("content-encoding"));
            assertArrayEquals(expected, gunzip(second.body));
        }
    }

    @ParameterizedTest(name = "gzip={0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("bySize: many small adds coalesce into far fewer chunks and still round-trip")
    void bySizeCoalesces(final boolean gzip) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            final RawResponse resp = request(socket, "GET",
                    gzip ? "/bysize-gzip" : "/bysize-plain", gzip, false);
            assertEquals(200, resp.status);
            assertEquals("chunked", resp.header("transfer-encoding"));
            assertNull(resp.header("content-length"));
            if (gzip) {
                assertEquals("gzip", resp.header("content-encoding"));
                assertArrayEquals(PAYLOAD, gunzip(resp.body));
            } else {
                assertNull(resp.header("content-encoding"));
                assertArrayEquals(PAYLOAD, resp.body);
            }
            assertTrue(resp.chunkCount > 0);
            assertTrue(resp.chunkCount < BYSIZE_PIECES,
                    "Expected coalescing: " + resp.chunkCount + " chunks vs " + BYSIZE_PIECES + " adds");
        }
    }

    private static RawResponse request(final Socket socket, final String method, final String path,
                                       final boolean acceptGzip, final boolean keepAlive) throws IOException {
        final OutputStream os = socket.getOutputStream();
        final StringBuilder req = new StringBuilder();
        req.append(method).append(' ').append(path).append(" HTTP/1.1\r\n");
        req.append("Host: ").append("localhost").append("\r\n");
        if (acceptGzip) {
            req.append("Accept-Encoding: gzip\r\n");
        }
        req.append("Connection: ").append(keepAlive ? "keep-alive" : "close").append("\r\n");
        req.append("\r\n");
        os.write(req.toString().getBytes(StandardCharsets.US_ASCII));
        os.flush();
        return readResponse(socket.getInputStream(), "HEAD".equals(method));
    }

    private static RawResponse readResponse(final InputStream in, final boolean headRequest) throws IOException {
        // Read up to and including the blank line that ends the header block.
        final ByteArrayOutputStream head = new ByteArrayOutputStream();
        int state = 0;   // matches \r\n\r\n
        while (state < 4) {
            final int b = in.read();
            if (b < 0) {
                throw new IOException("EOF before end of headers");
            }
            head.write(b);
            final boolean cr = (state == 0 || state == 2);
            if (cr) {
                state = (b == '\r') ? state + 1 : 0;
            } else {
                state = (b == '\n') ? state + 1 : 0;
            }
        }
        final String[] lines = head.toString(StandardCharsets.US_ASCII.name()).split("\r\n");
        final RawResponse resp = new RawResponse();
        resp.status = Integer.parseInt(lines[0].split(" ")[1]);
        for (int i = 1; i < lines.length; i++) {
            final String line = lines[i];
            if (line.isEmpty()) {
                continue;
            }
            final int colon = line.indexOf(':');
            resp.headers.put(line.substring(0, colon).trim().toLowerCase(),
                    line.substring(colon + 1).trim());
        }
        if (headRequest) {
            resp.body = new byte[0];   // HEAD: framing is in the headers, but no body follows
            return resp;
        }
        if ("chunked".equalsIgnoreCase(resp.headers.get("transfer-encoding"))) {
            resp.body = readChunkedBody(in, resp);
            return resp;
        }
        final int contentLength = Integer.parseInt(resp.headers.getOrDefault("content-length", "0"));
        final byte[] body = new byte[contentLength];
        int off = 0;
        while (off < contentLength) {
            final int n = in.read(body, off, contentLength - off);
            if (n < 0) {
                throw new IOException("EOF at " + off + "/" + contentLength);
            }
            off += n;
        }
        resp.body = body;
        return resp;
    }

    // De-chunk a Transfer-Encoding: chunked body: "<hex-size>\r\n<data>\r\n" repeated, ended by a
    // zero-size chunk "0\r\n\r\n". Records the number of non-empty data chunks in resp.chunkCount.
    private static byte[] readChunkedBody(final InputStream in, final RawResponse resp) throws IOException {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        int chunks = 0;
        while (true) {
            final int size = Integer.parseInt(readLine(in).trim(), 16);
            if (size == 0) {
                readLine(in);            // consume the (empty) trailer line ending the message
                break;
            }
            chunks++;
            final byte[] data = new byte[size];
            int off = 0;
            while (off < size) {
                final int n = in.read(data, off, size - off);
                if (n < 0) {
                    throw new IOException("EOF at " + off + "/" + size + " in chunk");
                }
                off += n;
            }
            body.write(data, 0, size);
            readLine(in);                // consume the CRLF after the chunk data
        }
        resp.chunkCount = chunks;
        return body.toByteArray();
    }

    private static String readLine(final InputStream in) throws IOException {
        final StringBuilder sb = new StringBuilder();
        int prev = -1;
        int b;
        while ((b = in.read()) >= 0) {
            if (prev == '\r' && b == '\n') {
                sb.setLength(sb.length() - 1);   // drop the trailing '\r'
                return sb.toString();
            }
            sb.append((char) b);
            prev = b;
        }
        throw new IOException("EOF before end of line");
    }

    private static final class RawResponse {
        private int status;
        private final Map<String, String> headers = new HashMap<>();
        private byte[] body;
        private int chunkCount;   // number of non-empty data chunks (chunked responses only)

        private String header(final String name) {
            return headers.get(name.toLowerCase());
        }
    }

    private static byte[] gunzip(final byte[] compressed) throws IOException {
        try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final byte[] buf = new byte[8192];
            int n;
            while ((n = gz.read(buf)) >= 0) {
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        }
    }

    private static byte[] concat(final byte[] a, final byte[] b) {
        final byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static byte[] buildPayload() {
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < (1 << 20)) {   // ~1 MiB
            sb.append("line ").append(String.format("%08d", i++))
                    .append(": the quick brown fox jumps over the lazy dog 0123456789\n");
        }
        return sb.toString().getBytes(StandardCharsets.US_ASCII);
    }
}
