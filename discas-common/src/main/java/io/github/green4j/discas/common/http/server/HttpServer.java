/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * Minimalistic NIO HTTP/1.1 server, inspired by nginx and Netty. This javadoc is the manual: every
 * feature below is shown with the code that uses it, and the referenced types carry only their
 * contracts.
 *
 * <h2>Architecture</h2>
 * <ul>
 *   <li>N worker threads, each with its own Selector and event loop.</li>
 *   <li>A connection is pinned to ONE worker for life, so no connection state is shared and no
 *       lock is taken on it.</li>
 *   <li>Each connection owns one input and one output {@link java.nio.ByteBuffer}, used as byte
 *       queues. Nothing per message is allocated -- no buffer per request, no event objects.</li>
 *   <li>Request headers MUST fit the input buffer (else 431). A body may span many reads, fixed
 *       length or chunked.</li>
 *   <li>Zero-allocation parser and response writer; a resumable {@link ByteContent} state machine
 *       drives bodies larger than the output buffer, integrating with OP_WRITE and backpressure.</li>
 *   <li>Keep-alive, per-state timeouts, and a connection droppable at any point.</li>
 * </ul>
 *
 * <p>Everything is nested in this one file: the public API ({@link ConnectionHandler},
 * {@link ConnectionInitializer}, {@link AcceptHandler}, {@link HttpRequest}, {@link HttpResponse},
 * {@link ChunkedResponse}, {@link HttpHeader}, {@link ContentType}, {@link Connection},
 * {@link ByteContent}, {@link Future}) and the internals that serve it.
 *
 * <h2>1. Getting started</h2>
 * Implement a {@link ConnectionHandler}, hand the server a {@link ConnectionInitializer} that
 * produces one per connection, {@link #start()} and {@link #close()}. Port {@code 0} binds an
 * ephemeral port, readable from {@link #boundPort()} once {@code start()} returns. The handler here
 * is shared by every connection because it holds no per-connection state -- see section 4 for the
 * variant that does.
 * <pre>{@code
 * ConnectionHandler handler = new ConnectionHandler() {
 *     public void onRequestComplete(Connection c, HttpRequest r) {
 *         c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
 *          .setBodyAscii("Hello from NIO HTTP/1.1\n")
 *          .ok();                       // 200 OK, Content-Length filled in for you
 *     }
 * };
 *
 * HttpServerConfig cfg = HttpServer.builder().port(8080).workerCount(1).build();
 * try (HttpServer server = new HttpServer(cfg, new AcceptHandler() { }, c -> handler)) {
 *     server.start();                         // binds; boundPort() valid on return
 *     System.out.println("listening on :" + server.boundPort());
 *     Thread.currentThread().join();          // serve until interrupted; close() runs on exit
 * }
 * }</pre>
 *
 * <h2>2. Configuration</h2>
 * Every tunable has a default declared inline on its {@link HttpServerConfigBuilder} field, so a
 * fresh config is usable as-is and {@code builder()} alone is a readable source of every default.
 * A built config is an immutable snapshot: keep the builder and it cannot reach through to a
 * running server.
 * <pre>{@code
 * HttpServerConfig cfg = HttpServer.builder()
 *         .port(8080)
 *         .workerCount(Runtime.getRuntime().availableProcessors())
 *         .outputBufferBytes(64 * 1024)
 *         .acceptBacklog(2048)
 *         .build();
 * }</pre>
 *
 * <h2>3. Reading the request</h2>
 * {@link HttpRequest} compares method and path without allocating, and reports what the client
 * will accept. Route on it directly.
 * <pre>{@code
 * public void onRequestComplete(Connection c, HttpRequest r) {
 *     if (r.methodEquals("GET") && r.pathEquals("/ping")) {
 *         c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true)   // gzip if the client takes it
 *          .setBodyAscii("pong\n")
 *          .ok();
 *     } else {
 *         c.beginResponse().error(404);        // header-only reject
 *     }
 * }
 * }</pre>
 *
 * <h3>3.1 Request-body backpressure</h3>
 * A handler that cannot absorb a body as fast as it arrives stops the reads rather than buffering:
 * {@link Connection#pauseInput()} clears OP_READ, {@link Connection#resumeInput()} restores it and
 * is safe to call from any thread.
 * <pre>{@code
 * public void onBodyChunk(Connection c, HttpRequest r, ByteBuffer buf, int off, int len) {
 *     c.pauseInput();                                  // stop reading until the sink drains
 *     sink.writeAsync(buf, off, len).whenComplete((v, e) -> c.resumeInput()); // any thread
 * }
 * }</pre>
 *
 * <h2>4. Per-connection state</h2>
 * A {@link ConnectionInitializer} runs once per accepted connection, so a handler may hold state
 * for that connection's life and needs no synchronization -- the connection never leaves its
 * worker thread. Header accumulation is the usual case.
 * <pre>{@code
 * ConnectionInitializer init = connection -> new ConnectionHandler() {
 *     private final HttpRequestHeader headers = new HttpRequestHeader();
 *
 *     public void onRequestLine(Connection c, HttpRequest r) { headers.reset(); }
 *
 *     public void onHeader(Connection c, HttpRequest r, ByteBuffer buf,
 *                          int nameOff, int nameLen, int valOff, int valLen) {
 *         headers.add(buf, nameOff, nameLen, valOff, valLen);
 *     }
 *
 *     public void onRequestComplete(Connection c, HttpRequest r) {
 *         if (!headers.contains("Authorization")) {
 *             c.beginResponse().error(401, "missing Authorization");
 *         } else {
 *             c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii("authorized\n").ok();
 *         }
 *     }
 * };
 * }</pre>
 *
 * <p>When the handler is shared rather than per-connection, {@link Connection#userState()} and
 * {@link Connection#setUserState} are a free-form slot on the connection for the same purpose:
 * state kept across the requests of one connection, reachable from every callback, and still never
 * touched by two threads.
 *
 * <h2>5. Responses</h2>
 * Three shapes, all begun from the {@link Connection}: a fixed-length body, a header-only reply,
 * and a chunked stream. One response is in flight at a time; a pipelined request waits.
 *
 * <h3>5.1 Fixed length</h3>
 * Buffered until {@code commit}, so {@code Content-Length} is the summed body size. Parts may be
 * appended before committing, and {@link HttpResponse#error} discards whatever was collected.
 * <pre>{@code
 * // Whole body at once:
 * c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
 *  .header(HttpHeader.CACHE_CONTROL, "no-store")
 *  .setBodyAscii("hello\n")
 *  .ok();                                 // == commit(200)
 *
 * // Assembled from parts, explicit status:
 * HttpResponse resp = c.beginResponse(ContentType.APPLICATION_JSON, r.keepAlive());
 * resp.addBodyUtf8("{\"items\":[");
 * resp.addBodyUtf8("1,2,3");
 * resp.addBodyUtf8("]}");
 * resp.commit(200);
 *
 * // Throw the collected body away and answer an error instead (errorAndClose forces a close):
 * resp.error(500, "{\"error\":\"boom\"}");
 * }</pre>
 *
 * <h3>5.2 Header-only replies and errors</h3>
 * Begun without a content type, a response carries no body: {@code setBody*} and {@code addBody*}
 * throw, and a bodyless {@link HttpResponse#commit(int)} emits {@code Content-Length: 0} with no
 * {@code Content-Type}.
 * <p>
 * {@link HttpResponse#error(int)} is the bodyless reject. Its two-argument form is <em>not</em>
 * bodyless: the reason is sent verbatim as the body, under the content type the response was begun
 * with, defaulting to {@code text/plain} -- so the caller must match the two (a JSON reason under a
 * JSON-begun response). Keep-alive follows the flag the response was begun with, and
 * {@link Connection#beginResponse()} begins with it off, so the reject below closes the connection;
 * {@link HttpResponse#errorAndClose} forces a close whatever the flag.
 * <pre>{@code
 * c.beginResponse(r.keepAlive())               // e.g. a 204 or a redirect
 *  .header(HttpHeader.LOCATION, "/next")
 *  .commit(302);
 *
 * c.beginResponse().error(404);                // bodyless reject, closes the connection
 * c.beginResponse().error(404, "not found");   // same, plus "not found" as a text/plain body
 * }</pre>
 *
 * <p>To answer successfully and hang up, use {@link HttpResponse#okAndClose()} -- the pair of
 * {@code errorAndClose}. It drops the begun keep-alive <em>before</em> committing, so the reply
 * carries {@code Connection: close} and the socket closes behind it:
 * <pre>{@code
 * c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii("bye\n").okAndClose();
 * }</pre>
 *
 * <p>{@link Connection#closeAfterFlush()} is the blunter tool: it closes once whatever is queued
 * has drained, but it cannot change a header already decided, so a reply committed with
 * {@code ok()} still announces {@code keep-alive} and then closes anyway. Reach for it when the
 * decision to hang up comes <em>after</em> the reply was committed; otherwise prefer
 * {@code okAndClose()}.
 *
 * <h3>5.3 Chunked</h3>
 * The first {@code addChunk} writes the {@code 200 OK} headers; {@code commit()} writes the
 * terminating chunk. Bailing out is only possible <em>before</em> that first chunk --
 * {@link ChunkedResponse#error(int)} still works then. After it the status is on the wire and the
 * only way out is {@link ChunkedResponse#close()}, which drops the connection with the body
 * unfinished, so the client sees a failed response. To end a stream cleanly and still hang up,
 * commit it and mark the connection with {@link Connection#closeAfterFlush()} -- there is no
 * chunked pair of {@link HttpResponse#okAndClose()}, because the header block went out with the
 * first chunk and can no longer announce the close.
 * <pre>{@code
 * c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive())
 *  .addChunkAscii("part-1\n")
 *  .addChunkAscii("part-2\n")
 *  .commit();
 * }</pre>
 *
 * <h3>5.4 gzip, and how chunks are cut</h3>
 * Pass {@code gzip = true} to opt in. Checking {@link HttpRequest#gzipAccepted()} first is not
 * required -- the server ANDs the flag with what the request advertised, so on a client that does
 * not accept gzip the flag transparently no-ops and the body goes out identity-encoded.
 * <p>
 * {@link ChunkPolicy} decides how added bytes become HTTP chunks: {@code PER_CHUNK} (the default)
 * makes every add its own chunk, with a gzip {@code SYNC_FLUSH} at each boundary, which is what a
 * client watching a live stream needs; {@link ChunkPolicy#bySize} coalesces small adds, which
 * compresses far better.
 * <pre>{@code
 * // Fixed body, compressed at commit:
 * c.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true)
 *  .setBody(page)
 *  .ok();
 *
 * // Low latency: one HTTP chunk per add, SYNC_FLUSHed so the client sees it now.
 * c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive(), true);
 *
 * // Throughput: coalesce into ~8 KiB compressed chunks, remainder flushed at commit().
 * ChunkedResponse resp = c.beginChunkedResponse(
 *         ContentType.TEXT_PLAIN, r.keepAlive(), true, ChunkPolicy.bySize(8 * 1024));
 * for (int off = 0; off < bulk.length; off += 512) {
 *     resp.addChunk(bulk, off, Math.min(512, bulk.length - off));
 * }
 * resp.commit();
 * }</pre>
 *
 * <h2>6. Producing a body over time</h2>
 *
 * <h3>6.1 On a timer</h3>
 * {@link Connection#schedule} runs a task on this connection's worker thread and marks the
 * response deferred, so the connection waits for the eventual commit instead of treating the
 * handler's return as the end of the reply. It works for either shape: chunked flushes each tick,
 * a fixed body accumulates and goes out whole at commit.
 * <pre>{@code
 * ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
 * int[] i = {0};
 * Runnable[] tick = new Runnable[1];
 * tick[0] = () -> {
 *     if (!c.isOpen()) return;                 // peer gone: stop
 *     resp.addChunkAscii(i[0] + "\n");         // first add sends the 200 OK headers
 *     if (++i[0] < 10) c.schedule(resp, tick[0], 1000);
 *     else resp.commit();
 * };
 * c.schedule(resp, tick[0], 1000);             // marks resp deferred until commit()
 * }</pre>
 *
 * <p>The timer runs on a wheel whose tick is ~250&nbsp;ms, so a task is armed at least one tick out
 * and a very small {@code delayMs} is rounded up to one. For <em>as soon as possible on the loop</em>
 * rather than after a delay -- resuming a deferred response from a continuation, say --
 * {@link Connection#execute(Response, Runnable)} is the one to use; it defers the response the same
 * way and costs no wheel entry.
 *
 * <h3>6.2 Driven by the consumer</h3>
 * For an unbounded stream, pace the producer off the consumer rather than off a clock:
 * {@link ChunkedResponse#whenDrained()} completes once the recorded chunks have reached the
 * socket, so a slow client pauses production instead of letting chunks pile up. It completes on
 * the next loop turn rather than inline, so this iterates rather than recurses, and it fails on
 * teardown, which is what ends the stream.
 * <pre>{@code
 * ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
 * long[] n = {0};
 * Runnable[] produce = new Runnable[1];
 * produce[0] = () -> {
 *     if (!c.isOpen()) return;
 *     resp.addChunkAscii(n[0]++ + "\n");
 *     resp.whenDrained()
 *         .onSuccess(v -> produce[0].run())    // next only after this one drained
 *         .onFailure(err -> { });              // torn down: stop quietly
 * };
 * produce[0].run();
 * }</pre>
 *
 * <h3>6.3 Bounding the wait</h3>
 * {@link Connection#schedule} hands back a {@link Cancelable}, which is how a deferred response
 * gets a deadline.
 * <pre>{@code
 * Cancelable timeout = c.schedule(resp, () -> resp.error(504, "timed out"), 5_000);
 * // ... later, once the real result arrives on the worker thread:
 * timeout.cancel();                 // disarm; a no-op if it already fired
 * resp.setBody(data).commit(200);
 * }</pre>
 *
 * <h2>7. Work off the worker thread</h2>
 * {@link Response#async} adopts any {@link java.util.concurrent.CompletionStage} and hands back a
 * {@link Future} whose callbacks run <em>on this connection's worker thread</em> -- so a
 * continuation may touch the response directly, with no hop and no lock. The response is deferred
 * until it commits.
 * <pre>{@code
 * HttpResponse resp = c.beginResponse(ContentType.OCTET_STREAM, r.keepAlive());
 * resp.async(store.getAsync(key))
 *     .onSuccess(data -> resp.setBody(data).commit(data.length > 0 ? 200 : 404))
 *     .onFailure(err -> resp.error(500));
 * }</pre>
 *
 * <p>{@link Future#map} and {@link Future#compose} sequence dependent reads, flushing each section
 * as it lands:
 * <pre>{@code
 * ChunkedResponse resp = c.beginChunkedResponse(ContentType.TEXT_PLAIN, r.keepAlive());
 * resp.async(getAsync("A"))
 *     .map(a -> { resp.addChunk(a); return "B"; })
 *     .compose(k -> resp.async(getAsync(k)))
 *     .map(b -> { resp.addChunk(b); return null; })
 *     .onSuccess(x -> resp.commit())
 *     .onFailure(err -> resp.close());
 * }</pre>
 *
 * <p>{@link Future#all} waits for independent reads to finish together:
 * <pre>{@code
 * Future.all(resp.async(getAsync("1")), resp.async(getAsync("2")), resp.async(getAsync("3")))
 *     .onSuccess(parts -> {
 *         resp.addChunk((byte[]) parts[0]);
 *         resp.addChunk((byte[]) parts[1]);
 *         resp.addChunk((byte[]) parts[2]);
 *         resp.commit();
 *     })
 *     .onFailure(err -> resp.close());
 * }</pre>
 *
 * <h2>8. Saying it without allocating</h2>
 * Field names, media types and constant values are encoded once and shared across every connection
 * and worker. {@link HttpHeader#of} and {@link ContentType#of} validate up front, so a malformed
 * name fails at startup rather than on the wire.
 * <pre>{@code
 * static final HttpHeader X_REQUEST_ID = HttpHeader.of("X-Request-Id");
 * static final ContentType CSV = ContentType.of("text/csv; charset=utf-8");
 * static final ByteArrayContent NO_STORE = ByteArrayContent.ascii("no-store");
 * static final ByteArrayContent EMPTY_JSON = ByteArrayContent.utf8("{}");
 *
 * c.beginResponse(ContentType.APPLICATION_JSON, r.keepAlive())
 *  .header(HttpHeader.CACHE_CONTROL, NO_STORE)         // constant value, no allocation
 *  .header(X_REQUEST_ID, requestId)                    // dynamic value
 *  .setBody(EMPTY_JSON)
 *  .ok();
 * }</pre>
 *
 * <p>A body that is generated rather than stored implements {@link ByteContent} and is pulled in
 * output-buffer-sized pieces, so its size is bounded by nothing:
 * <pre>{@code
 * final class Filler implements ByteContent {
 *     private final byte b;
 *     private final long n;
 *     Filler(byte b, long n) { this.b = b; this.n = n; }
 *
 *     public long length() { return n; }
 *
 *     public int write(int sourceOffset, ByteBuffer target) {
 *         int m = (int) Math.min(target.remaining(), n - sourceOffset);
 *         for (int i = 0; i < m; i++) target.put(b);   // relative puts advance target.position()
 *         return m;                                    // exactly the bytes written this call
 *     }
 * }
 *
 * c.beginResponse(ContentType.OCTET_STREAM, r.keepAlive())
 *  .setBody(new Filler((byte) 'x', 1L << 30))
 *  .ok();
 * }</pre>
 *
 * <h2>9. The accept loop</h2>
 * {@link AcceptHandler} is where the listening socket is tuned and where a connection can be
 * refused before it costs anything. The server sets no socket options itself.
 * <pre>{@code
 * AcceptHandler accept = new AcceptHandler() {
 *     public void onOpen(ServerSocketChannel serverChannel) throws IOException {
 *         serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);   // must precede bind()
 *     }
 *
 *     public boolean onAccept(ServerSocketChannel serverChannel, SocketChannel channel) throws IOException {
 *         channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
 *         InetSocketAddress peer = (InetSocketAddress) channel.getRemoteAddress();
 *         return limiter.admit(peer.getAddress());   // false rejects before a buffer is allocated
 *     }
 * };
 * }</pre>
 *
 * <p>Refusing here is the cheap place to do it: the channel is closed before a {@link Connection}
 * exists, so neither buffer is allocated. Past that point the peer is still available from
 * {@link Connection#remoteAddress()}, stable for the connection's life and readable in any
 * callback.
 *
 * <h2>10. Failures</h2>
 * A malformed request reaches {@link ConnectionHandler#onError} with the status the parser chose;
 * a handler that throws reaches {@link ConnectionHandler#onException}. Both default to no-ops, and
 * the server covers each: if {@code onError} returns without answering, it sends that status
 * itself, and a handler that threw always has its connection closed -- so {@code onException} is a
 * place to record the failure, not to rescue the connection.
 * <pre>{@code
 * public void onError(Connection c, int status, String reason) {
 *     c.beginResponse().error(status);        // render your own 400/408/431
 * }
 *
 * public void onException(Connection c, Throwable cause) {
 *     log.error("in-connection failure", cause);   // the connection is closed for you
 * }
 * }</pre>
 */
public final class HttpServer implements AutoCloseable {
    private final Worker[] workers;
    private final HttpServerConfig config;
    private final AcceptHandler acceptHandler;
    private final ThreadFactory acceptorThreadFactory;
    private final AtomicLong roundRobin = new AtomicLong();

    private Selector acceptSelector;
    private Set<SelectionKey> acceptSelectedKeys;
    private Thread acceptThread;

    private boolean started; // guarded by this
    private boolean closed;  // guarded by this
    // Written once by the acceptor thread just before it counts the bind latch down; published to
    // start() via the latch and to any other reader via the 'this' monitor (a boundPort() caller
    // blocks on the monitor until start() releases it, so the write is transitively visible).
    private int boundPort = -1;
    // Set by the acceptor thread when the bind attempt fails, before the latch; read (and rethrown)
    // by start() after the latch. Published via the latch.
    private IOException startException;

    private volatile boolean running;

    public HttpServer(final HttpServerConfig config,
                      final AcceptHandler acceptHandler,
                      final ConnectionInitializer initializer) {
        this(
                config,
                acceptHandler,
                initializer,
                defaultAcceptorThreadFactory(),
                defaultWorkerThreadFactory()
        );
    }

    /**
     * Full-control constructor: supply an {@link AcceptHandler} for accept-loop life-cycle events
     * and explicit {@link ThreadFactory}s for the single acceptor thread and the worker threads.
     * A supplied factory owns its threads' naming and daemon policy.
     */
    public HttpServer(final HttpServerConfig config,
                      final AcceptHandler acceptHandler,
                      final ConnectionInitializer initializer,
                      final ThreadFactory acceptorThreadFactory,
                      final ThreadFactory workerThreadFactory) {
        this.config = config;
        this.acceptHandler = acceptHandler;
        this.acceptorThreadFactory = acceptorThreadFactory;
        this.workers = new Worker[config.workerCount()];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(i, initializer, acceptHandler, config, workerThreadFactory);
        }
    }

    /**
     * A fresh {@link HttpServerConfigBuilder} with every tunable (bind address, port, worker count,
     * timeouts, buffer/accept-queue sizes, gzip settings) at its default. Override only what you need
     * through the fluent setters, then call {@link HttpServerConfigBuilder#build()} and pass the
     * resulting {@link HttpServerConfig} to a constructor.
     */
    public static HttpServerConfigBuilder builder() {
        return new HttpServerConfigBuilder();
    }

    /**
     * Start the server. Thread-safe and idempotent: safe to call from any thread at any time.
     * A second call once started is a no-op; a call after {@link #close()} throws
     * {@link IllegalStateException}.
     */
    public void start() throws IOException {
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("Closed");
            }
            if (started) {
                return; // idempotent: already started
            }
            started = true;

            for (int i = 0; i < workers.length; i++) {
                workers[i].start();
            }

            acceptSelector = Selector.open();
            acceptSelectedKeys = acceptSelector.selectedKeys();

            // The listening socket is opened, tuned (via AcceptHandler.onOpen) and bound on the
            // acceptor thread so onOpen can set pre-bind options; wait here until that resolves so
            // start() stays synchronous (boundPort() valid on return, bind IOException propagated).
            final CountDownLatch bindLatch = new CountDownLatch(1);
            acceptThread = acceptorThreadFactory.newThread(() -> acceptLoop(bindLatch));
            acceptThread.start();
            try {
                bindLatch.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                final InterruptedIOException iioe = new InterruptedIOException("Interrupted while binding");
                iioe.initCause(e);
                throw iioe;
            }
            if (startException != null) {
                throw startException; // bind failed on the acceptor thread; surface it to the caller
            }
        }
    }

    /**
     * The local port the server is bound to. Useful when the server was created with port 0
     * (ephemeral). Only valid after {@link #start()}; returns -1 if the
     * server has not been started yet.
     */
    public int boundPort() {
        synchronized (this) {
            return boundPort;
        }
    }

    /**
     * Stop accepting and drive the acceptor thread to release the listening socket and all workers
     * (closing their connections); the acceptor thread fires {@link AcceptHandler#onClose(ServerSocketChannel)} as its
     * terminal step. Thread-safe, best-effort and idempotent: safe to call from any thread at any
     * time, before or after {@link #start()}, and repeatedly.
     */
    @Override
    public void close() {
        final boolean wasStarted;
        final Worker[] workersLocal;
        final Selector acceptSelectorLocal;
        final Thread acceptThreadLocal;

        synchronized (this) {
            if (closed) {
                return; // idempotent
            }
            closed = true;
            running = false;

            wasStarted = started;
            workersLocal = workers;
            acceptSelectorLocal = acceptSelector; // non-final: published by start()
            acceptThreadLocal = acceptThread;     // non-final: published by start()
        }
        if (!wasStarted) {
            // start() was never called: release the worker selectors opened in the constructor.
            for (int i = 0; i < workersLocal.length; i++) {
                workersLocal[i].stop();
            }
            return;
        }
        // Both are set by start() before the acceptor thread starts; still guard the non-final
        // references defensively rather than rely on that invariant at every call site.
        if (acceptSelectorLocal != null) {
            acceptSelectorLocal.wakeup();
        }
        if (acceptThreadLocal != null) {
            try {
                // Budget covers the acceptor thread's own teardown, which joins every worker thread.
                acceptThreadLocal.join(Worker.SELECT_TIMEOUT_MS * (workersLocal.length + 2L));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void acceptLoop(final CountDownLatch bindLatch) {
        // All of these are written once in start() before this thread is started, so
        // Thread.start() publishes them here. Snapshotting them into locals makes "not changed on
        // the fly" explicit and avoids re-reading the fields on every loop iteration.
        final AtomicLong roundRobin = this.roundRobin;
        final AcceptHandler acceptHandler = this.acceptHandler;
        final Worker[] workers = this.workers;
        final Selector acceptSelector = this.acceptSelector;
        final Set<SelectionKey> acceptSelectedKeys = this.acceptSelectedKeys;

        // Owned by this thread: opened, tuned and bound here so onOpen can set pre-bind options.
        ServerSocketChannel serverChannel = null;
        // onClose is the mirror of onOpen: only fire it if onOpen actually ran, so a failure in
        // ServerSocketChannel.open() (before onOpen) doesn't produce a lone onClose.
        boolean opened = false;
        try {
            try {
                serverChannel = ServerSocketChannel.open();
                serverChannel.configureBlocking(false);
                acceptHandler.onOpen(serverChannel);        // pre-bind: handler tunes server-socket options
                opened = true;
                serverChannel.bind(new InetSocketAddress(config.bindAddress(), config.port()),
                        config.acceptBacklog());
                serverChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
                boundPort = serverChannel.socket().getLocalPort();
                running = true;
            } catch (final IOException e) {
                startException = e; // start() rethrows this; the outer finally tears everything down
                return;
            } finally {
                // Exactly once, on success or failure, before select() can block: releases start().
                bindLatch.countDown();
            }

            while (running) {
                acceptSelector.select();
                if (!running) {
                    break;
                }
                if (!acceptSelectedKeys.isEmpty()) {
                    final Iterator<SelectionKey> iterator = acceptSelectedKeys.iterator();
                    while (iterator.hasNext()) {
                        final SelectionKey key = iterator.next();
                        iterator.remove();
                        if (!key.isValid() || !key.isAcceptable()) {
                            continue;
                        }
                        SocketChannel channel;
                        while ((channel = serverChannel.accept()) != null) {
                            channel.configureBlocking(false);
                            if (acceptHandler.onAccept(serverChannel, channel)) {
                                final Worker worker = workers[Math.floorMod(roundRobin.getAndIncrement(),
                                        workers.length)];
                                worker.assign(channel);
                            } else {
                                closeQuietly(channel); // rejected: never handed to a worker, no onOpen
                            }
                        }
                    }
                }
            }
        } catch (final IOException e) {
            if (running) {
                acceptHandler.onError(serverChannel, e);
            }
        } finally {
            // Tear down on the acceptor thread so onClose (the terminal hook) is the last thing
            // this thread does, after the listening socket and all workers are released.
            closeQuietly(serverChannel);
            closeQuietly(acceptSelector);
            for (int i = 0; i < workers.length; i++) {
                workers[i].stop();
            }
            if (opened) {
                acceptHandler.onClose(serverChannel);   // mirror onOpen; skip if onOpen never ran
            }
        }
    }

    private static ThreadFactory defaultAcceptorThreadFactory() {
        return runnable -> {
            final Thread thread = new Thread(runnable, "http-acceptor");
            thread.setDaemon(true);
            return thread;
        };
    }

    private static ThreadFactory defaultWorkerThreadFactory() {
        final AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            final Thread thread = new Thread(runnable, "http-worker-" + sequence.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * Monotonic "now" in milliseconds for deadline arithmetic. Derived from {@link System#nanoTime()}
     * (arbitrary origin but never steps backward), so all timeouts and the timing wheel are immune to
     * wall-clock/NTP adjustments. All deadlines are relative, so the arbitrary origin does not matter;
     * the one invariant is that every timing-path site uses this and never mixes in
     * {@code System.currentTimeMillis()}.
     */
    private static long nowMs() {
        return System.nanoTime() / 1_000_000L;
    }

    /**
     * Run a release listener and swallow whatever it throws. Written once and called from all three
     * places that hand bytes back -- a listener is the caller's code, and a caller that throws in it
     * must not take down the response it was told about.
     */
    private static void fireListener(final Runnable listener) {
        if (listener == null) {
            return;
        }
        try {
            listener.run();
        } catch (final Throwable ignore) {
            // best-effort: a release listener must not disrupt the response
        }
    }

    private static void closeQuietly(final AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (final Exception ignore) {
            // best-effort
        }
    }

    /**
     * Server configuration: the five layered timeouts that bound total progress of a connection
     * (Slowloris &amp; slow-consumer defense), a per-worker connection admission limit, gzip settings,
     * an optional request-body cap, and the per-connection buffer / accept-queue sizes. Deadlines are
     * enforced by a per-worker timing wheel; state deadlines are hard (set once when the state is
     * entered, not pushed forward on per-byte progress) so an attacker cannot appear "active" by
     * dribbling one byte before each timeout.
     *
     * <p>Every tunable has a sensible default declared inline on its field (the single source of
     * truth), so a fresh config is immediately usable. A config is immutable to callers: build one
     * with the {@link HttpServerConfigBuilder} obtained from {@link HttpServer#builder()},
     * overriding only what you need, then call {@link HttpServerConfigBuilder#build()} -- see
     * <em>2. Configuration</em> in the {@link HttpServer} manual. Once built the config is only read (by the
     * acceptor and worker threads).
     *
     * <ul>
     *   <li>{@code bindAddress} -- the local address to bind the listening socket to; defaults to
     *       {@code 127.0.0.1} (loopback only).</li>
     *   <li>{@code port} -- the TCP port to listen on (default {@code 8080}); {@code 0} binds an
     *       ephemeral port (read it back with {@link HttpServer#boundPort()}).</li>
     *   <li>{@code workerCount} -- number of worker threads/selectors the server runs (default
     *       {@code 1}).</li>
     *   <li>{@code headerTimeoutMs} -- request line + headers must complete within this budget.</li>
     *   <li>{@code bodyTimeoutMs} -- the request body must complete within this budget.</li>
     *   <li>{@code requestTimeoutMs} -- absolute cap from request start to request complete
     *       (an un-evadable ceiling folded into the header/body deadlines).</li>
     *   <li>{@code writeTimeoutMs} -- the response must fully drain within this budget of when it
     *       started (hard cap; defeats a slow-reading consumer that trickles ACKs).</li>
     *   <li>{@code keepAliveTimeoutMs} -- idle time allowed between requests on a kept-alive
     *       connection.</li>
     *   <li>{@code maxConnectionsPerWorker} -- connections beyond this are shed at accept, before
     *       any buffers are allocated.</li>
     * </ul>
     */
    public static final class HttpServerConfig {
        // Every tunable's default is declared inline on its field -- this is the single source of
        // truth. A fresh config is immediately usable; callers override what they need through the
        // fluent setters on HttpServerConfigBuilder, which mutate this instance before it is built.
        // The constructor is private: a config is only ever produced by the builder and, once built,
        // is read-only to callers (getters only).
        private String bindAddress = "127.0.0.1";
        private int port = 8080;
        private int workerCount = 1;
        private long headerTimeoutMs = 10_000L;
        private long bodyTimeoutMs = 30_000L;
        private long requestTimeoutMs = 60_000L;
        private long writeTimeoutMs = 30_000L;
        private long keepAliveTimeoutMs = 15_000L;
        private int maxConnectionsPerWorker = 10_000;
        private int gzipLevel = Deflater.DEFAULT_COMPRESSION;
        private long maxRequestBodyBytes = 0L;
        private int inputBufferBytes = 16 * 1024;
        private int outputBufferBytes = 32 * 1024;
        private int acceptBacklog = 1024;
        private int gzipStagingBytes = 8192;
        private int gzipScratchBytes = 8192;
        private int gzipInitialOutBytes = 256;

        private HttpServerConfig() {
        }

        /**
         * Copy constructor used by {@link HttpServerConfigBuilder#build()} so the config handed to
         * a server shares no state with the builder that produced it.
         */
        private HttpServerConfig(final HttpServerConfig other) {
            this.bindAddress = other.bindAddress;
            this.port = other.port;
            this.workerCount = other.workerCount;
            this.headerTimeoutMs = other.headerTimeoutMs;
            this.bodyTimeoutMs = other.bodyTimeoutMs;
            this.requestTimeoutMs = other.requestTimeoutMs;
            this.writeTimeoutMs = other.writeTimeoutMs;
            this.keepAliveTimeoutMs = other.keepAliveTimeoutMs;
            this.maxConnectionsPerWorker = other.maxConnectionsPerWorker;
            this.gzipLevel = other.gzipLevel;
            this.maxRequestBodyBytes = other.maxRequestBodyBytes;
            this.inputBufferBytes = other.inputBufferBytes;
            this.outputBufferBytes = other.outputBufferBytes;
            this.acceptBacklog = other.acceptBacklog;
            this.gzipStagingBytes = other.gzipStagingBytes;
            this.gzipScratchBytes = other.gzipScratchBytes;
            this.gzipInitialOutBytes = other.gzipInitialOutBytes;
        }

        /** The local address the listening socket is bound to (default {@code 127.0.0.1}). */
        public String bindAddress() {
            return bindAddress;
        }

        public int port() {
            return port;
        }

        public int workerCount() {
            return workerCount;
        }

        public long headerTimeoutMs() {
            return headerTimeoutMs;
        }

        public long bodyTimeoutMs() {
            return bodyTimeoutMs;
        }

        public long requestTimeoutMs() {
            return requestTimeoutMs;
        }

        public long writeTimeoutMs() {
            return writeTimeoutMs;
        }

        public long keepAliveTimeoutMs() {
            return keepAliveTimeoutMs;
        }

        public int maxConnectionsPerWorker() {
            return maxConnectionsPerWorker;
        }

        /**
         * The {@link Deflater} compression level (0-9, or {@link Deflater#DEFAULT_COMPRESSION}) used for
         * gzip responses (see {@link Connection#beginResponse(ContentType, boolean, boolean)}).
         */
        public int gzipLevel() {
            return gzipLevel;
        }

        /**
         * Optional cap on the decoded request body size (total {@code Content-Length}, or the sum of
         * chunk data for a chunked request). {@code 0} (the default) means unlimited -- the parser then
         * streams a body of any size, bounded only by {@code bodyTimeoutMs}. When positive, a request
         * whose body exceeds it is rejected with {@code 413 Payload Too Large}.
         */
        public long maxRequestBodyBytes() {
            return maxRequestBodyBytes;
        }

        /**
         * Capacity in bytes of each connection's input buffer. The request line and all headers must
         * fit within it (a larger header block is rejected with {@code 431}); the body may stream
         * across many fills of it.
         */
        public int inputBufferBytes() {
            return inputBufferBytes;
        }

        /**
         * Capacity in bytes of each connection's output buffer -- the byte queue the response drains
         * through. Also bounds a single chunk's data span for chunked responses.
         */
        public int outputBufferBytes() {
            return outputBufferBytes;
        }

        /** The listen backlog passed to the server socket {@code bind()}. */
        public int acceptBacklog() {
            return acceptBacklog;
        }

        /**
         * Size in bytes of the gzip encoder's staging buffer, which pulls raw body bytes out of a
         * {@link ByteContent} before they are deflated.
         */
        public int gzipStagingBytes() {
            return gzipStagingBytes;
        }

        /** Size in bytes of the gzip encoder's deflate-output scratch buffer. */
        public int gzipScratchBytes() {
            return gzipScratchBytes;
        }

        /**
         * Initial capacity in bytes of the gzip encoder's assembled-stream buffer; it grows on demand,
         * so this is only a starting hint.
         */
        public int gzipInitialOutBytes() {
            return gzipInitialOutBytes;
        }

    }

    /**
     * Builder for an {@link HttpServerConfig}. Obtain one via {@link HttpServer#builder()},
     * override only the tunables you need through the fluent setters (each mutates and returns
     * {@code this}), then call {@link #build()} to obtain the immutable config. Size setters fail fast
     * on non-positive/too-small values. Defaults are not restated here -- they live inline on the
     * {@link HttpServerConfig} fields, which this builder starts from.
     */
    public static final class HttpServerConfigBuilder {
        private final HttpServerConfig cfg = new HttpServerConfig();

        public HttpServerConfigBuilder bindAddress(final String v) {
            cfg.bindAddress = v;
            return this;
        }

        public HttpServerConfigBuilder port(final int v) {
            cfg.port = v;
            return this;
        }

        public HttpServerConfigBuilder workerCount(final int v) {
            cfg.workerCount = requireSize("workerCount", v, 1);
            return this;
        }

        public HttpServerConfigBuilder headerTimeoutMs(final long v) {
            cfg.headerTimeoutMs = v;
            return this;
        }

        public HttpServerConfigBuilder bodyTimeoutMs(final long v) {
            cfg.bodyTimeoutMs = v;
            return this;
        }

        public HttpServerConfigBuilder requestTimeoutMs(final long v) {
            cfg.requestTimeoutMs = v;
            return this;
        }

        public HttpServerConfigBuilder writeTimeoutMs(final long v) {
            cfg.writeTimeoutMs = v;
            return this;
        }

        public HttpServerConfigBuilder keepAliveTimeoutMs(final long v) {
            cfg.keepAliveTimeoutMs = v;
            return this;
        }

        public HttpServerConfigBuilder maxConnectionsPerWorker(final int v) {
            cfg.maxConnectionsPerWorker = v;
            return this;
        }

        public HttpServerConfigBuilder gzipLevel(final int v) {
            cfg.gzipLevel = v;
            return this;
        }

        /** {@code 0} means unlimited; a negative limit is a wiring error, not "unlimited". */
        public HttpServerConfigBuilder maxRequestBodyBytes(final long v) {
            if (v < 0) {
                throw new IllegalArgumentException("maxRequestBodyBytes must be >= 0, was " + v);
            }
            cfg.maxRequestBodyBytes = v;
            return this;
        }

        public HttpServerConfigBuilder inputBufferBytes(final int v) {
            cfg.inputBufferBytes = requireSize("inputBufferBytes", v, 64);
            return this;
        }

        public HttpServerConfigBuilder outputBufferBytes(final int v) {
            cfg.outputBufferBytes = requireSize("outputBufferBytes", v, 64);
            return this;
        }

        public HttpServerConfigBuilder acceptBacklog(final int v) {
            cfg.acceptBacklog = requireSize("acceptBacklog", v, 1);
            return this;
        }

        public HttpServerConfigBuilder gzipStagingBytes(final int v) {
            cfg.gzipStagingBytes = requireSize("gzipStagingBytes", v, 1);
            return this;
        }

        public HttpServerConfigBuilder gzipScratchBytes(final int v) {
            cfg.gzipScratchBytes = requireSize("gzipScratchBytes", v, 1);
            return this;
        }

        public HttpServerConfigBuilder gzipInitialOutBytes(final int v) {
            cfg.gzipInitialOutBytes = requireSize("gzipInitialOutBytes", v, 1);
            return this;
        }

        /**
         * The finished config -- a snapshot, not a live view of this builder.
         * <p>
         * Returning the builder's own instance made "immutable to callers" true only for a caller
         * who discarded the builder: anyone holding on to it could still reach through and mutate
         * a config a running server was already using. Copying also makes the builder reusable,
         * so successive {@code build()} calls yield independent configs.
         */
        public HttpServerConfig build() {
            return new HttpServerConfig(cfg);
        }

        private static int requireSize(final String name, final int value, final int min) {
            if (value < min) {
                throw new IllegalArgumentException(name + " must be >= " + min + ", was " + value);
            }
            return value;
        }
    }

    /**
     * Per-connection handler: iteration over parsed request parts. One instance is created for
     * each connection by a {@link ConnectionInitializer}, so it may safely hold per-connection state
     * (a connection lives on one worker thread for life).
     *
     * <p>The {@code ByteBuffer}/offset/length arguments are valid ONLY during the callback; copy
     * out anything that must outlive it. Callbacks run on the worker thread that owns the
     * connection, so they must not block. A handler drives the response from
     * {@link #onRequestComplete(Connection, HttpRequest)} via the {@link Connection} response API.
     *
     * <p>Callbacks fire in request order: {@code onOpen} once, then per request
     * {@code onRequestLine} &rarr; {@code onHeader}* &rarr; {@code onHeadersComplete} &rarr;
     * {@code onBodyChunk}* &rarr; {@code onRequestComplete}, with {@code onClose} once at teardown.
     * Most handlers override only {@code onRequestComplete} and route on the request; see
     * <em>3. Reading the request</em> and <em>10. Failures</em> in the {@link HttpServer} manual.
     */
    public interface ConnectionHandler {
        /**
         * Connection life-cycle: fired once when the connection is active (registered on its worker,
         * timeouts armed), before any request callback. The place to allocate per-connection state
         * or apply admission policy -- a handler may inspect {@link Connection#remoteAddress()} and
         * reject the connection here (via {@link Connection#close()} or
         * {@link Connection#beginResponse()}{@code .error(status)}), e.g. a per-IP connection limiter.
         */
        default void onOpen(final Connection connection) {
        }

        default void onRequestLine(final Connection connection,
                                   final HttpRequest request) {
        }

        default void onHeader(Connection connection, HttpRequest request, ByteBuffer buffer,
                              int nameOffset, int nameLength, int valueOffset, int valueLength) {
        }

        default void onHeadersComplete(Connection connection,
                                       HttpRequest request) {
        }

        default void onBodyChunk(Connection connection,
                                 HttpRequest request,
                                 ByteBuffer buffer,
                                 int offset,
                                 int length) {
        }

        default void onRequestComplete(Connection connection, HttpRequest request) {
        }

        /**
         * Protocol-error notification (e.g. {@code 400}, {@code 408}, {@code 431}). The handler may
         * render a custom response (via the {@code beginResponse} API -- e.g.
         * {@link Connection#beginResponse()}{@code .error(status)}); if it does not, the server sends a
         * default {@code status} response and closes the
         * connection. Runs on the owning worker thread; must not block.
         */
        default void onError(Connection connection, int status, String reason) {
        }

        /**
         * Unexpected-failure notification: an exception escaped a callback or an I/O error hit the
         * socket. Invoked once, just before the connection is force-closed (followed by
         * {@link #onClose}); {@code cause} is the offending {@link Throwable}. Best-effort -- must not
         * throw.
         */
        default void onException(Connection connection, Throwable cause) {
        }

        /**
         * Connection life-cycle: the terminal hook, invoked exactly once for every connection that
         * fired {@link #onOpen}, on every teardown path (clean EOF, timeout, protocol error,
         * exception, and server shutdown). The place to release per-connection resources.
         * Best-effort -- must not throw.
         */
        default void onClose(Connection connection) {
        }
    }

    /**
     * Netty-style factory: creates the {@link ConnectionHandler} for a newly accepted
     * connection. Invoked once per connection, on the owning worker thread, with the freshly
     * built {@link Connection} so the handler can capture it for per-connection state. Analogous
     * to Netty's {@code ChannelInitializer.initChannel(channel)}.
     *
     * <p>To keep the classic single shared-handler behavior, return the same instance every time
     * ({@code connection -> sharedHandler}); for per-connection handlers, return a new one
     * ({@code connection -> new MyHandler(connection)}). A per-connection handler is what lets you
     * hold request-scoped state (e.g. an {@link HttpRequestHeader} collector) without any
     * synchronization, since the connection stays on one worker thread for life. See
     * <em>4. Per-connection state</em> in the {@link HttpServer} manual.
     */
    @FunctionalInterface
    public interface ConnectionInitializer {
        ConnectionHandler initConnection(Connection connection);
    }

    /**
     * Accept-loop life-cycle listener, mirroring {@link ConnectionHandler}'s style. Every callback
     * runs on the single acceptor thread: {@link #onOpen(ServerSocketChannel)} once as the listening
     * socket is being set up (before it is bound), {@link #onAccept(ServerSocketChannel,
     * SocketChannel)} for each accepted socket (as an admission gate, before it is handed to a
     * worker), {@link #onError(ServerSocketChannel, Throwable)} on an accept-loop failure, and
     * {@link #onClose(ServerSocketChannel)} exactly once as the terminal step after the listening
     * socket and all workers have been released. Callbacks must not block; all default to no-ops so a
     * listener overrides only what it needs.
     *
     * <p>Typical use is pre-bind server-socket options in {@code onOpen} and per-connection options
     * (plus an admission gate) in {@code onAccept} -- see <em>9. The accept loop</em> in the {@link HttpServer} manual.
     * When you have nothing to tune, pass a bare {@code new AcceptHandler() { }}.
     */
    public interface AcceptHandler {
        /**
         * Fired once, on the acceptor thread, after the listening channel is opened and put in
         * non-blocking mode but <em>before</em> it is bound -- the place to set server-socket options
         * that must precede {@code bind()}, e.g.
         * {@code serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true)} (the server sets
         * none by default), {@code SO_RCVBUF}, or {@code IP_TOS}. {@link HttpServer#boundPort()} is
         * not yet valid here. A throw (e.g. from {@code setOption}) aborts the bind and surfaces from
         * {@link HttpServer#start()}.
         */
        default void onOpen(final ServerSocketChannel serverChannel) throws IOException {
        }

        /**
         * Admission gate for a freshly accepted, non-blocking socket, invoked on the acceptor thread
         * before any per-connection buffers are allocated. Return {@code true} to hand the socket to
         * a worker (the normal path); return {@code false} to reject it -- the server closes the
         * channel immediately and it never reaches a worker (no {@code onOpen}). Cheap enough for a
         * per-IP connection limiter or blocklist keyed on {@code channel.getRemoteAddress()}.
         * <p>Also this is a recommended way to tune-up the connection set proper socket options:
         * {@code channel.setOption(StandardSocketOptions.TCP_NODELAY, true);}
         */
        default boolean onAccept(final ServerSocketChannel serverChannel,
                                 final SocketChannel channel) throws IOException {
            return true;
        }

        /**
         * The accept loop or a worker's select loop failed.
         *
         * @param serverChannel the listening channel, or {@code null} when the failure came from
         *                      a worker's own select loop rather than the accept loop -- that
         *                      path used to print a stack trace directly, past this seam
         */
        default void onError(final ServerSocketChannel serverChannel,
                             final Throwable cause) {
        }

        default void onClose(final ServerSocketChannel serverChannel) {
        }
    }

    /**
     * Response body abstraction: write bytes into the output buffer, zero-alloc, resumable.
     *
     * <p>A body larger than the output buffer is written across many calls: the driver invokes
     * {@link #write(int, ByteBuffer)} repeatedly, flushing between calls, advancing {@code sourceOffset}
     * by the bytes written and stopping once it has collected {@link #length()} bytes. An instance may
     * be stateful (a shareable constant backed by immutable data is a pure function of its inputs; a
     * sequential generator keeps a cursor and is then single-use); it is used for one response on one
     * connection thread, so it need not be thread-safe. The built-in {@link Connection#beginResponse} +
     * {@link DefaultHttpResponse#setBody}/{@link DefaultHttpResponse#addChunk} overloads back your raw
     * {@code byte[]}/{@link CharSequence} with pooled, copy-free content; implement this only for
     * custom or generated bodies.
     *
     * <p>For a plain {@code byte[]} the built-in {@link ByteArrayContent} is a ready, shareable
     * constant. Implement this interface directly only to <em>generate</em> a body without
     * materializing it -- a worked example is under <em>8. Saying it without allocating</em> in
     * the {@link HttpServer} manual. The driver calls {@code write} repeatedly, flushing between calls, until it has collected
     * {@link #length()} bytes -- so this streams a body far larger than the output buffer.
     */
    public interface ByteContent {
        /**
         * Copy this content's bytes starting at {@code sourceOffset} into {@code target} using
         * <em>relative</em> puts (advancing {@code target.position()}). Writes at most
         * {@code target.remaining()} bytes and returns the number written, always {@code >= 0};
         * returns {@code 0} <em>only</em> when not even one unit fits in the remaining room (e.g. a
         * multi-byte character), so the caller must flush and retry. Never returns a negative "end"
         * sentinel.
         *
         * <p>Precondition: {@code 0 <= sourceOffset < length()}. The caller counts the bytes it has
         * collected and stops at {@link #length()}, so it never calls with
         * {@code sourceOffset >= length()}. Violating this, or being unable to produce the bytes that
         * {@link #length()} promises, results in a thrown exception (e.g. an
         * {@link IndexOutOfBoundsException} from the backing store, or an explicit
         * {@link IllegalStateException}) -- never a sentinel return.
         *
         * <p>An implementation that is a pure function of {@code sourceOffset} -- as all the built-ins
         * backed by immutable data are -- is safe to share across threads and connections (one constant
         * instance serving many responses). A sequential generator may keep internal state but is then
         * single-use and not shareable.
         */
        int write(int sourceOffset, ByteBuffer target);

        /**
         * Exact number of body bytes this content will produce -- mandatory and a binding guarantee:
         * {@link #write} must deliver exactly this many bytes across calls, or throw. Used by
         * {@link DefaultHttpResponse#setBody(ByteContent)} as the Content-Length. Must be {@code >= 0} and
         * stable for the life of the response, and consistent with the total bytes written across
         * {@link #write}.
         */
        long length();
    }

    /**
     * Request view: slices into the input buffer. No per-request String/array allocations.
     *
     * <p>The {@code (buffer, offset, length)} slices are valid only while the owning
     * {@link Connection} is processing the current request; do not retain a reference across
     * callbacks. Handlers read the request through the zero-allocation accessors
     * ({@link #pathEquals(String)}, {@link #keepAlive()}, ...); the parser populates the fields.
     *
     * <p>Prefer the allocation-free comparisons on the hot path, as
     * <em>3. Reading the request</em> in the {@link HttpServer} manual shows.
     * {@link #methodString()} / {@link #pathString()} materialize (and cache) a {@code String} when
     * you need one -- but call them <em>before</em> reading a request body: a body read compacts the
     * input buffer and can invalidate the underlying slice, so a handler that needs the path during
     * {@code onBodyChunk}/{@code onRequestComplete} should cache it in {@code onRequestLine} first.
     */
    public static final class HttpRequest {
        private int methodOffset;
        private int methodLength;
        private int pathOffset;
        private int pathLength;
        private int versionMinor;
        private long contentLength = -1;
        private boolean chunked;
        private boolean keepAlive = true;
        private boolean expectContinue;
        private boolean gzipAccepted;
        private ByteBuffer buffer;

        // Lazily materialized String views of the method/path slices, cached so the router, the
        // handler and any logging that ask for them across a single request pay at most one
        // allocation each; cleared in reset() before the next request reuses this instance.
        private String methodStringCache;
        private String pathStringCache;

        private void reset() {
            methodOffset = methodLength = pathOffset = pathLength = 0;
            versionMinor = 1;
            contentLength = -1;
            chunked = false;
            keepAlive = true;
            expectContinue = false;
            gzipAccepted = false;
            buffer = null;
            methodStringCache = null;
            pathStringCache = null;
        }

        public boolean keepAlive() {
            return keepAlive;
        }

        /**
         * True if the request advertised gzip in {@code Accept-Encoding}. A response begun with the
         * gzip flag (see {@link Connection#beginResponse(ContentType, boolean, boolean)}) is only
         * compressed when this is true; otherwise the flag no-ops and the body is sent identity.
         */
        public boolean gzipAccepted() {
            return gzipAccepted;
        }

        private void forceKeepAlive(final boolean value) {
            keepAlive = value;
        }

        /**
         * Zero-allocation path comparison against an ASCII literal.
         */
        public boolean pathEquals(final String path) {
            if (buffer == null || pathLength != path.length()) {
                return false;
            }
            for (int i = 0; i < pathLength; i++) {
                if ((buffer.get(pathOffset + i) & 0xFF) != path.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        public boolean methodEquals(final String method) {
            if (buffer == null || methodLength != method.length()) {
                return false;
            }
            for (int i = 0; i < methodLength; i++) {
                if ((buffer.get(methodOffset + i) & 0xFF) != method.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        public String methodString() {
            if (methodStringCache == null) {
                methodStringCache = slice(buffer, methodOffset, methodLength);
            }
            return methodStringCache;
        }

        public String pathString() {
            if (pathStringCache == null) {
                pathStringCache = slice(buffer, pathOffset, pathLength);
            }
            return pathStringCache;
        }

        private static String slice(final ByteBuffer buffer, final int offset, final int length) {
            if (buffer == null || length == 0) {
                return "";
            }
            final byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = buffer.get(offset + i);
            }
            // UTF-8, not US-ASCII: the method and path reach handlers as Strings that are
            // re-encoded as UTF-8 downstream (an agent key, a CAS expected-value header), so
            // decoding as ASCII replaced every non-ASCII byte with U+FFFD before the caller
            // ever saw it -- and the round trip could then never match the original bytes.
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * A single client connection: one input buffer and one output buffer, each used as a byte
     * queue. Lives on exactly one {@link Worker} thread for its whole life, so its state needs no
     * synchronization.
     *
     * <p>Handlers respond through the public API ({@link #beginResponse} /
     * {@link #beginChunkedResponse}, which return a reusable {@link DefaultHttpResponse} carrying
     * {@link DefaultHttpResponse#setBody}/{@link DefaultHttpResponse#addChunk} and {@link DefaultHttpResponse#commit}, plus
     * the {@link DefaultHttpResponse#error} terminal -- reachable for a bare reject via
     * {@link #beginResponse()}{@code .error(status)}).
     * The read/write/parse machinery is package-private and invoked by the owning worker.
     *
     * <p>Three response shapes, all begun from a request callback and worker-thread-confined: a
     * fixed-length body, a header-only reply or reject, and a chunked stream. Each is worked
     * through under <em>5. Responses</em> in the {@link HttpServer} manual.
     * A response can also be produced later, off the request callback: begin it, hand its handle to
     * {@link #execute(Response, Runnable)} / {@link #schedule(Response, Runnable, long)} /
     * {@link Response#async(CompletionStage)}, and {@code commit} from the continuation (all of which
     * run back on this worker thread). Request-body backpressure is available via
     * {@link #pauseInput()} / {@link #resumeInput()}, and {@link #userState()} is a free per-connection
     * slot for your own state.
     */
    public static final class Connection {
        // Input/output buffer capacities come from HttpServerConfig (inputBufferBytes /
        // outputBufferBytes), allocated per connection in the constructor.

        // Timeout state: the connection lives in exactly one at a time; entering a state arms a
        // hard deadline for it (see enterTimeoutState). A freshly-accepted connection starts in
        // TS_HEADERS.
        private static final int TS_HEADERS = 0;    // receiving request line + headers
        private static final int TS_BODY = 1;       // receiving the request body
        private static final int TS_RESPONDING = 2; // draining the response
        private static final int TS_KEEPALIVE = 3;  // idle between requests

        // Response-progress state: the linear lifecycle of the reply on this connection, tracked as
        // one machine (was three booleans). RS_SENT is a transient step between the body finishing
        // and the next-request reset / close.
        private static final int RS_IDLE = 0;       // no response begun
        private static final int RS_DEFERRED = 1;   // async response owed, nothing on the wire yet
        private static final int RS_STREAMING = 2;  // headers on the wire, body draining
        private static final int RS_SENT = 3;       // body finished, headers were sent (pre-reset)

        private final SocketChannel channel;
        private final InetSocketAddress remoteAddress;
        private final ConnectionHandler handler;
        private final Worker worker;
        private final HttpServerConfig config;
        private SelectionKey key;

        // Timeout state (worker-thread confined). 'deadlineMs' is the absolute wall-clock deadline
        // for the current state; 'requestStartMs' anchors the absolute request cap, folded into the
        // header/body deadlines. The state deadline is armed in the worker's TimeoutWheel through
        // an intrusive TimerNode (composition, so the wheel stays a generic scheduler and the node
        // machinery does not leak onto the public Connection type).
        private int timeoutState = TS_HEADERS;
        private long deadlineMs;
        private long requestStartMs;
        // Read via isOpen() from async producers on other threads (see its Javadoc), so volatile:
        // written on the worker thread, its value must be visible to those off-thread readers.
        private volatile boolean closed;
        private final TimerNode timeoutTimer = new TimerNode() {
            @Override
            void onExpire(final long now) {
                onDeadline(now);
            }
        };

        // Free-form slot the handler may use to pool its own per-connection state across requests
        // (a connection lives on one worker thread for life).
        private Object userState;

        private final ByteBuffer in;   // append via read; parse & compact (sized from config)
        private final ByteBuffer out;  // append via writer; flip/drain/compact (sized from config)

        private final HttpParser parser;
        private final HttpRequest request = new HttpRequest();
        private final ResponseWriter responseWriter = new ResponseWriter();
        private final ResponseBodyDriver bodyDriver;
        private final DefaultHttpResponse response = new DefaultHttpResponse(this);
        private GzipEncoder gzipEncoder;         // lazily created on first gzip response, reused after

        private int responseState = RS_IDLE;     // linear lifecycle of the reply (see RS_* above)
        // monotonic; bumped by DefaultHttpResponse.begin (see responsesBegun())
        private long responsesBegun;
        private boolean continueSent = false;   // 100-continue already emitted for this request
        private boolean inputPaused = false;     // handler-requested request-body backpressure
        private boolean closeAfterFlush = false;
        private boolean readSuspended = false;
        // At most one outstanding whenDrained() future: completed when the recorded chunks have drained
        // (see signalDrainIfIdle), failed on teardown, cleared per request.
        private Future<Void> drainWaiter;

        // Response-progress predicates over responseState, mirroring the former booleans so the
        // read sites stay legible: response owed but not on the wire; headers already written; body
        // currently draining.
        private boolean responseDeferred() {
            return responseState == RS_DEFERRED;
        }

        private boolean headersSent() {
            return responseState >= RS_STREAMING;
        }

        private boolean streamingBody() {
            return responseState == RS_STREAMING;
        }

        /**
         * Monotonic count of responses begun on this connection (one per {@code beginResponse*} /
         * {@code beginChunkedResponse*}). Never reset per request, so a dispatcher such as
         * {@link PrefixRouter} can snapshot it before invoking a handler and detect afterwards that the
         * handler took ownership of the reply -- reliable even when a fully-synchronous response has
         * already completed and reset {@code responseState} back to {@code RS_IDLE} by the time the
         * callback returns. Worker-thread-confined, like the rest of the connection state.
         */
        long responsesBegun() {
            return responsesBegun;
        }

        /** Bumped from {@link DefaultHttpResponse#begin} -- the single choke point for every response. */
        private void markResponseBegun() {
            responsesBegun++;
        }

        /**
         * True once the current request has been fully parsed (request line, headers and body all
         * consumed). Used to decide whether an error response may keep the connection alive: an error
         * sent before the body is drained cannot safely be followed by another request on the socket.
         */
        private boolean requestFullyRead() {
            return parser.state() == HttpParser.ST_DONE;
        }

        private Connection(final SocketChannel channel, final ConnectionInitializer initializer,
                   final Worker worker, final HttpServerConfig config) {
            this.channel = channel;
            InetSocketAddress addr = null;
            try {
                addr = (InetSocketAddress) channel.getRemoteAddress();
            } catch (final IOException ignore) {
                // best-effort: remoteAddress() may be null if the peer is already gone
            }
            this.remoteAddress = addr;
            this.worker = worker;
            this.config = config;
            this.parser = new HttpParser(config.maxRequestBodyBytes());
            this.in = ByteBuffer.allocateDirect(config.inputBufferBytes());
            this.out = ByteBuffer.allocateDirect(config.outputBufferBytes());
            this.bodyDriver = new ResponseBodyDriver(config.outputBufferBytes());
            in.clear();
            out.clear(); // append mode
            responseWriter.bind(out);
            resetForNextRequest();
            // Last: everything else is initialized, so the initializer sees a fully-usable
            // connection (except the handler field it is producing).
            this.handler = initializer.initConnection(this);
        }

        /**
         * The peer's address, or {@code null} if it could not be resolved (e.g. the peer had already
         * disconnected). Stable for the life of the connection; usable from any callback (e.g. a
         * per-IP limiter keying on {@code remoteAddress().getAddress()}).
         */
        public InetSocketAddress remoteAddress() {
            return remoteAddress;
        }

        /**
         * True until the connection is torn down. Async producers and scheduled tasks (which may
         * fire after a peer disconnect) must check this before touching the connection -- writing to a
         * closed connection is a no-op, but building a response against one is wasted work.
         */
        public boolean isOpen() {
            return !closed;
        }

        /**
         * Run {@code task} on this connection's owning worker thread and mark {@code response}
         * deferred: an HTTP/1.1 connection serves one response at a time, so reads are suspended and
         * the write-timeout budget armed until the response is committed from a later continuation.
         * The response must already have been begun ({@link #beginResponse}/{@link
         * #beginChunkedResponse}) so its reference can be passed here. Call on the worker thread; a
         * task that lands after teardown should guard on {@link #isOpen()}.
         */
        public void execute(final Response response, final Runnable task) {
            requireEventLoop();
            markDeferred(response);
            worker.execute(task);
        }

        /**
         * Arm a one-shot {@code task} to run on this connection's worker thread after {@code delayMs}
         * milliseconds, marking {@code response} deferred; returns a {@link Cancelable} to cancel
         * it. Periodic work (e.g. emit one body part per second) is expressed by the task
         * re-scheduling itself. Call on the worker thread.
         *
         * <p>Granularity: the task fires at the next timing-wheel tick at or after the deadline, so it
         * runs at least one tick (~250&nbsp;ms) out and a very small {@code delayMs} is effectively
         * rounded up to one tick. For "run as soon as possible on the loop" use
         * {@link #execute(Response, Runnable)} instead.
         *
         * <p>Re-scheduling the task from inside itself is how a body is produced over time; see
         * <em>6.1 On a timer</em> in the {@link HttpServer} manual.
         */
        public Cancelable schedule(final Response response, final Runnable task, final long delayMs) {
            requireEventLoop();
            markDeferred(response);
            return worker.schedule(task, delayMs);
        }

        /**
         * Internal implementation behind {@link Response#async(CompletionStage)}: bridge a JDK
         * {@link java.util.concurrent.CompletionStage} (e.g. an AWS SDK async result) into a
         * worker-thread {@link Future} and mark {@code response} deferred. The stage's completion --
         * arriving on some library thread -- only calls {@code complete}/{@code fail}, which hop onto
         * this connection's worker thread. Call on the worker thread. Package-private: callers reach
         * it through {@code resp.async(stage)}, never directly on the connection.
         */
        private <T> Future<T> async(final Response response, final CompletionStage<T> stage) {
            requireEventLoop();
            markDeferred(response);
            final Promise<T> promise = new Promise<>(this);
            stage.whenComplete((value, error) -> {
                if (error != null) {
                    promise.fail(error);
                } else {
                    promise.complete(value);
                }
            });
            return promise.future();
        }

        /**
         * Mark this connection's response deferred: it will be committed from a later async
         * continuation rather than synchronously in {@code onRequestComplete}. Idempotent across the
         * ops of a multi-read chain; throws if the supplied handle is not this connection's response
         * or if that response has already been completed (the sync/async-mixed and double-response
         * guard).
         */
        private void markDeferred(final Response handle) {
            if (handle != response) {
                throw new IllegalStateException("Response handle was not created for this connection");
            }
            if (response.isDone()) {
                throw new IllegalStateException("Response already completed; cannot start async work on it");
            }
            if (responseState == RS_IDLE) {
                responseState = RS_DEFERRED;
                enterTimeoutState(TS_RESPONDING);   // bound the async build with the write timeout
                suspendRead();                      // HTTP/1.1: one response at a time
            }
        }

        /** Package-private cross-thread hop used by {@link Promise} to deliver continuations. */
        private void runOnLoop(final Runnable task) {
            worker.execute(task);
        }

        /**
         * Backing for {@link ChunkedResponse#whenDrained()}: hand back a worker-thread {@link Future}
         * that completes once the chunks recorded so far have been fully flushed. Completed inline when
         * nothing is pending; otherwise stashed and completed at the next drain checkpoint (see
         * {@link #signalDrainIfIdle()}). Fails immediately on a closed connection; only one waiter may be
         * outstanding at a time.
         */
        private Future<Void> whenDrained(final Response handle) {
            requireEventLoop();
            if (handle != response) {
                throw new IllegalStateException("Response handle was not created for this connection");
            }
            final Future<Void> f = new Future<>(this);
            if (closed) {
                f.failNow(new IllegalStateException("Connection closed"));
                return f;
            }
            if (out.position() == 0 && bodyDriver.chunksDrained()) {
                // Nothing pending on the wire. Complete on the NEXT loop turn, not inline: a fast
                // consumer draining every chunk synchronously would otherwise recurse
                // addChunk -> onSuccess -> addChunk to unbounded depth (stack overflow). Hopping through
                // the task queue turns that recursion into iteration.
                worker.execute(() -> {
                    if (closed) {
                        f.failNow(new IllegalStateException("Connection closed"));
                    } else {
                        f.succeed(null);
                    }
                });
                return f;
            }
            if (drainWaiter != null) {
                throw new IllegalStateException("A whenDrained() future is already outstanding");
            }
            drainWaiter = f;
            return f;
        }

        /**
         * Complete an outstanding {@link #drainWaiter} once the response has drained everything recorded
         * so far (output buffer empty and no chunk entries pending). Called at the streaming drain
         * checkpoints. The continuation it fires may add the next chunk (re-entering the driver), which
         * is why state is settled before this runs.
         */
        private void signalDrainIfIdle() {
            if (drainWaiter == null) {
                return;
            }
            if (out.position() == 0 && streamingBody() && bodyDriver.chunksDrained()) {
                final Future<Void> w = drainWaiter;
                drainWaiter = null;
                w.succeed(null);
            }
        }

        private void requireEventLoop() {
            if (!worker.inEventLoop()) {
                throw new IllegalStateException(
                        "Connection I/O must run on the owning worker thread; hop via Connection.execute()");
            }
        }

        /**
         * Fire the {@code onOpen} life-cycle callback once the connection is active. A throw from the
         * handler is routed to {@code onException} + close.
         */
        private void notifyOpen() {
            try {
                handler.onOpen(this);
            } catch (final Throwable t) {
                handleException(t);
            }
        }

        /**
         * Enter a timeout state and arm its hard deadline in the worker's timing wheel. State
         * deadlines are set once here, not advanced on per-byte progress, so a slow requester that
         * drips one byte before each timeout still dies. The absolute request cap
         * ({@code requestTimeoutMs}) is folded into the header/body deadlines so the wheel only
         * tracks one deadline per connection.
         */
        private void enterTimeoutState(final int newState) {
            final long now = nowMs();
            timeoutState = newState;
            switch (newState) {
                case TS_HEADERS:
                    requestStartMs = now;
                    deadlineMs = now + config.headerTimeoutMs();
                    break;
                case TS_BODY:
                    deadlineMs = Math.min(now + config.bodyTimeoutMs(),
                            requestStartMs + config.requestTimeoutMs());
                    break;
                case TS_RESPONDING:
                    deadlineMs = now + config.writeTimeoutMs();
                    break;
                default: // TS_KEEPALIVE
                    deadlineMs = now + config.keepAliveTimeoutMs();
                    break;
            }
            worker.wheel().schedule(timeoutTimer, deadlineMs);
        }

        /**
         * Called by the worker's timing wheel when this connection's state deadline expires. The
         * wheel re-arms deadlines clamped beyond its span before firing, so a fire here is always a
         * real expiry: a single state check decides the outcome. Read-side states send a
         * best-effort {@code 408}; write / keep-alive states just drop.
         */
        private void onDeadline(final long now) {
            if (timeoutState == TS_HEADERS || timeoutState == TS_BODY) {
                abortSlow();
            } else {
                close();
            }
        }

        /**
         * Abort a connection that timed out while we were still receiving its request: frame a
         * best-effort {@code 408 Request Timeout} if we have not started a response yet, then close.
         * A truly slow/malicious peer will not read the 408 anyway, so the slot is always reclaimed.
         */
        private void abortSlow() {
            if (out.position() == 0) {
                onProtocolError(408, "request timeout");
            }
            close();
        }

        private void attach(final SelectionKey key) {
            this.key = key;
        }

        private void resetForNextRequest() {
            parser.reset();
            request.reset();
            bodyDriver.reset();
            response.recycleAll();  // fire any pending release callbacks, return wrappers to the pool
            responseState = RS_IDLE;
            continueSent = false;
            inputPaused = false;
            drainWaiter = null;     // defensive: finishResponse/hardClose already settled it

        }

        private void onReadable() throws IOException {
            if (readSuspended) {
                return;
            }

            final int bytesRead = channel.read(in);   // in is in append mode
            if (bytesRead == -1) {
                close();
                return;
            }
            if (bytesRead == 0) {
                return;
            }

            in.flip();                        // read mode for parser: [pos..limit]
            try {
                processInput();
            } finally {
                in.compact();                 // keep unconsumed bytes, back to append mode
                if (in.position() == in.capacity()) {
                    // The buffer is full of unconsumed bytes and cannot grow: a single framing line
                    // that never terminates. Reject rather than wedge until bodyTimeout.
                    final int st = parser.state();
                    if (st < HttpParser.ST_BODY) {
                        onProtocolError(431, "request header fields too large"); // headers didn't fit
                    } else if (st == HttpParser.ST_CHUNK_SIZE || st == HttpParser.ST_TRAILERS) {
                        onProtocolError(400, "chunk framing line too large"); // chunk-size/trailer too long
                    }
                }
            }
        }

        private void processInput() {
            while (in.hasRemaining()) {
                if (streamingBody()) {
                    return; // don't parse next request until current response done
                }
                if (inputPaused) {
                    return; // request-body backpressure: wait for resumeInput()
                }

                // First bytes of a new request (fresh connection or after a kept-alive response):
                // arm the header timeout.
                if (timeoutState == TS_KEEPALIVE) {
                    enterTimeoutState(TS_HEADERS);
                }

                final int stateBefore = parser.state();
                final int parseResult = parser.parse(in, request, this, handler);
                // Headers just finished and a body follows (not an immediately-complete request):
                // switch from the header budget to the body budget.
                if (timeoutState == TS_HEADERS && stateBefore < HttpParser.ST_BODY
                        && parser.state() >= HttpParser.ST_BODY
                        && parser.state() != HttpParser.ST_DONE) {
                    enterTimeoutState(TS_BODY);
                }
                maybeSendContinue();
                if (parseResult == HttpParser.RESULT_NEED_MORE) {
                    return;
                }
                if (parseResult == HttpParser.RESULT_ERROR) {
                    onProtocolError(parser.errorStatus(), "malformed request");
                    return;
                }
                if (parseResult == HttpParser.RESULT_MESSAGE_COMPLETE) {
                    handler.onRequestComplete(this, request);
                    // If the handler committed a body still draining, or deferred the response to an
                    // async continuation, stop parsing: an HTTP/1.1 connection serves one response at
                    // a time, so the next (pipelined) request waits until this one is done.
                    if (streamingBody() || responseDeferred()) {
                        return;
                    }
                    // else response fully sent synchronously; continue for pipelined requests.
                }
            }
        }

        /**
         * If the client sent {@code Expect: 100-continue} and a request body is still pending,
         * emit a bare {@code HTTP/1.1 100 Continue} interim response exactly once so the client
         * proceeds to send the body.
         */
        private void maybeSendContinue() {
            if (!request.expectContinue || continueSent) {
                return;
            }
            // Never emit an interim 100 once the request has been answered/rejected: a handler that
            // responded during onRequestLine/onHeader/onHeadersComplete already queued its (final)
            // response into 'out', so appending 100 Continue after it would corrupt the stream.
            if (isClosing() || responseState != RS_IDLE || headersSent()) {
                return;
            }
            final int st = parser.state();
            if (st == HttpParser.ST_BODY || st == HttpParser.ST_CHUNK_SIZE
                    || st == HttpParser.ST_CHUNK_DATA || st == HttpParser.ST_CHUNK_CRLF
                    || st == HttpParser.ST_TRAILERS) {
                continueSent = true;
                responseWriter.statusLine(100);
                responseWriter.endHeaders();
                tryFlush();
            }
        }

        /**
         * Begin a fixed-length response -- <em>without</em> a status. Records the response on this
         * connection's reusable handle without touching the output buffer; collect the body
         * ({@link HttpResponse#setBody}/{@link HttpResponse#addBody}, buffered) and finish with
         * {@link HttpResponse#commit(int)} (which supplies the status and the summed
         * {@code Content-Length}) or {@link HttpResponse#error(int)}. Nothing reaches the wire until
         * then. Worker-thread-confined.
         */
        public HttpResponse beginResponse(final ContentType contentType, final boolean keepAlive) {
            return beginResponse(contentType, keepAlive, false);
        }

        /**
         * As {@link #beginResponse(ContentType, boolean)}, but opting the buffered body into gzip
         * compression: when {@code gzip} is {@code true} <em>and</em> the request advertised gzip
         * ({@link HttpRequest#gzipAccepted()}), the collected body is compressed at
         * {@link HttpResponse#commit(int) commit} -- the server sets the compressed {@code Content-Length}
         * and emits {@code Content-Encoding: gzip} / {@code Vary: Accept-Encoding} itself. If the client
         * does not accept gzip (or the response has no body) the flag transparently no-ops and the body
         * is sent identity-encoded.
         *
         * <p><b>Known cost (by design):</b> the whole body is compressed synchronously on the worker
         * thread at commit, which blocks the event loop -- and therefore every other connection on that
         * worker -- for the duration of the deflate. Keep it to small/medium bodies; for a large body
         * either defer the commit off the hot path via {@link #execute(Response, Runnable)}, or stream
         * it with {@link #beginChunkedResponse(ContentType, boolean, boolean)} (which compresses
         * incrementally). Worker-thread-confined.
         */
        public HttpResponse beginResponse(final ContentType contentType, final boolean keepAlive,
                                          final boolean gzip) {
            requireEventLoop();
            return response.begin(contentType, keepAlive, false, gzip, ChunkPolicy.PER_CHUNK);
        }

        /**
         * Begin a <em>header-only</em> fixed response with no content type and {@code keepAlive=false} --
         * the convenience for a reject/error, where the intended terminal is
         * {@link HttpResponse#error(int)} / {@link HttpResponse#error(int, CharSequence)}. Because
         * keep-alive follows the begun flag, this {@code keepAlive=false} form makes {@code error()}
         * close the connection (and, with no content type set, a reason defaults to {@code text/plain});
         * use {@link #beginResponse(boolean)} to keep the connection alive on error, or a
         * {@code beginResponse(contentType, keepAlive)} to send a typed (e.g. JSON) error body. A
         * response begun without a content type carries no body: {@code setBody*}/{@code addBody*} throw;
         * finish with a bodyless {@link HttpResponse#commit(int)} (emits {@code Content-Length: 0}, no
         * {@code Content-Type}) or {@code error}. Nothing reaches the wire until the terminal call.
         * Worker-thread-confined.
         */
        public HttpResponse beginResponse() {
            return beginResponse(false);
        }

        /**
         * Begin a <em>header-only</em> fixed response with no content type, choosing keep-alive -- for a
         * bodyless reply such as a redirect ({@code 301}/{@code 302} with a {@code Location} header),
         * {@code 204} or {@code 304}. A response begun without a content type carries no body:
         * {@code setBody*}/{@code addBody*} throw; add headers, then finish with a bodyless
         * {@link HttpResponse#commit(int)} (emits {@code Content-Length: 0}, no {@code Content-Type}) or
         * {@link HttpResponse#error(int)}. Nothing reaches the wire until the terminal call.
         * Worker-thread-confined.
         */
        public HttpResponse beginResponse(final boolean keepAlive) {
            requireEventLoop();
            return response.begin(null, keepAlive, false, false, ChunkPolicy.PER_CHUNK);
        }

        /**
         * Begin a {@code 200 OK} chunked ({@code Transfer-Encoding: chunked}) response. The first
         * {@link ChunkedResponse#addChunk} writes the headers + first chunk; each subsequent
         * {@code addChunk} is flushed as it is added, and {@link ChunkedResponse#commit()} writes the
         * terminating last chunk. Before the first chunk you may still {@link ChunkedResponse#error(int)}.
         * Custom headers must be added before the first chunk. Worker-thread-confined.
         */
        public ChunkedResponse beginChunkedResponse(final ContentType contentType, final boolean keepAlive) {
            return beginChunkedResponse(contentType, keepAlive, false, ChunkPolicy.PER_CHUNK);
        }

        /**
         * As {@link #beginChunkedResponse(ContentType, boolean)}, opting the chunked body into gzip: when
         * {@code gzip} is {@code true} <em>and</em> the request advertised gzip
         * ({@link HttpRequest#gzipAccepted()}), the streamed chunks are compressed on the fly -- the server
         * emits {@code Content-Encoding: gzip} / {@code Vary: Accept-Encoding} and (still) frames the body
         * as {@code Transfer-Encoding: chunked}. If the client does not accept gzip (or no chunk is ever
         * added) the flag transparently no-ops and the body is streamed identity-encoded.
         * Worker-thread-confined.
         */
        public ChunkedResponse beginChunkedResponse(final ContentType contentType, final boolean keepAlive,
                                                    final boolean gzip) {
            return beginChunkedResponse(contentType, keepAlive, gzip, ChunkPolicy.PER_CHUNK);
        }

        /**
         * As {@link #beginChunkedResponse(ContentType, boolean, boolean)}, with an explicit
         * {@link ChunkPolicy} governing where chunk boundaries (and, for gzip, {@code SYNC_FLUSH} points)
         * fall. {@link ChunkPolicy#PER_CHUNK} keeps the eager one-chunk-per-{@code addChunk} behavior;
         * {@link ChunkPolicy#bySize(int)} coalesces to a target size. Worker-thread-confined.
         */
        public ChunkedResponse beginChunkedResponse(final ContentType contentType, final boolean keepAlive,
                                                    final boolean gzip, final ChunkPolicy chunkPolicy) {
            requireEventLoop();
            return response.begin(contentType, keepAlive, true, gzip, chunkPolicy);
        }

        /**
         * Terminal step invoked by {@link DefaultHttpResponse#commit()}. Fixed and chunked responses have
         * separate implementations: a fixed response is buffered and written whole here; a chunked
         * response already streamed its chunks via {@link #onChunkAdded()}, so here it only emits the
         * terminating last chunk (writing the headers first if no chunk was ever added).
         */
        private void commitResponse() {
            requireEventLoop();
            if (response.chunked) {
                finalizeChunked();
            } else {
                commitFixed();
            }
        }

        // Fixed (buffered) response: headers + the single setBody body are written at commit().
        private void commitFixed() {
            final DefaultHttpResponse r = response;
            // Activate gzip only when the caller opted in, the client advertised gzip, and there is a
            // body to compress. Compress up front (into the connection's reusable encoder buffer) so the
            // compressed size is the Content-Length; the raw parts are then replaced by the encoder view.
            // Known cost (see beginResponse(..., gzip)): this deflate runs synchronously on the event
            // loop and blocks the whole worker for its duration -- fine for small/medium bodies only.
            r.gzipActive = r.gzip && request.gzipAccepted() && r.entryCount > 0;
            if (r.gzipActive) {
                final GzipEncoder encoder = gzipEncoder();
                encoder.reset();
                for (int i = 0; i < r.entryCount; i++) {
                    encoder.add(r.entry(i).content);
                }
                encoder.finish();
                r.useGzipBody(encoder);         // release raw parts, record the compressed buffer
            }
            final long contentLength = r.contentLength < 0 ? 0 : r.contentLength; // no body => header-only
            if (!writeHeaders(false, contentLength)) {
                return;                             // header overflow -> connection dropped
            }
            responseState = RS_STREAMING;
            // A HEAD response carries the same headers as GET (incl. Content-Length) but no body.
            if (request.methodEquals("HEAD") || r.entryCount == 0) {
                bodyDriver.reset();                 // header-only / HEAD
            } else {
                bodyDriver.beginFixed(r, contentLength);
            }
            enterTimeoutState(TS_RESPONDING);       // arm the write timeout for the response drain
            tryFlush();                             // flush headers first
            driveBody();
        }

        // A chunk was added by the handler. On the first chunk, decide gzip negotiation and, when the
        // response either compresses or coalesces (bySize), arm the streaming chunker. Then, per the
        // ChunkPolicy: PER_CHUNK plain records the raw content and flushes it as its own chunk (the
        // original fast path); otherwise the content is compressed/copied into the chunker and one or
        // more chunk spans are cut and flushed (PER_CHUNK: one per add; bySize: once the target is met).
        private void chunkAdded(final ByteContent content, final Runnable listener, final boolean owned) {
            final DefaultHttpResponse r = response;
            final boolean perChunk = r.chunkPolicy.perChunk();
            final boolean first = !r.firstChunkSeen;
            if (first) {
                r.firstChunkSeen = true;
                r.gzipActive = r.gzip && request.gzipAccepted();
                if (r.gzipActive || !perChunk) {
                    gzipEncoder().beginStream(r.gzipActive);   // gzip stream, or plain coalescing buffer
                }
            }

            // Plain + PER_CHUNK: unchanged -- record the caller's content, flush it as one chunk.
            if (!r.gzipActive && perChunk) {
                r.addEntry(content, listener, owned);
                onChunkAdded();
                return;
            }

            // HEAD carries no body: consume the caller's content (fire its release) and only ensure the
            // header block is written once.
            if (request.methodEquals("HEAD")) {
                fireListener(listener);
                onChunkAdded();
                return;
            }

            final GzipEncoder enc = gzipEncoder();
            enc.streamAdd(content, /*flush*/ perChunk && r.gzipActive);
            fireListener(listener);            // the caller's bytes are now consumed into the chunker

            if (perChunk) {
                recordChunkCut();              // gzip already SYNC_FLUSHed above; one chunk per add
                onChunkAdded();
                return;
            }
            // bySize: cut whole target-sized chunks; a sub-target remainder waits for the next add/commit.
            final int target = r.chunkPolicy.targetBytes();
            boolean cut = false;
            while (enc.streamPending() >= target) {
                enc.streamFlush();             // close the deflate block (no-op in plain mode)
                recordChunkCut();
                cut = true;
            }
            // Flush on a cut; also on the very first chunk (even with nothing yet cut) so the header block
            // is written and reads are suspended for the duration of the response.
            if (cut || first) {
                onChunkAdded();
            }
        }

        // Record the chunker's pending span as a chunk entry the body driver will frame. The span views
        // the chunker's buffer directly (no copy); it is owned so its wrapper returns to the pool, and it
        // carries no release listener (the caller's listener already fired when its bytes were consumed).
        private void recordChunkCut() {
            final GzipEncoder enc = gzipEncoder();
            final int start = enc.streamCutStart();
            final int len = enc.streamPending();
            enc.streamAdvanceCut();
            final PooledByteArrayContent span = response.acquireArray();
            span.reset(enc.streamBuffer(), start, len);
            response.addEntry(span, null, true);
        }

        // Chunked response: a chunk was just recorded -- write the headers on the first one, then
        // flush the newly-added chunk(s) to the wire.
        private void onChunkAdded() {
            if (!headersSent() && !startChunkedResponse()) {
                return;                             // header overflow -> connection dropped
            }
            if (request.methodEquals("HEAD")) {
                return;                             // HEAD carries no body
            }
            driveBody();
        }

        // Chunked commit(): emit the terminating last chunk (writing headers first if the response
        // had no chunks at all), then drain and finish.
        private void finalizeChunked() {
            final DefaultHttpResponse r = response;
            // Flush the chunker's tail as a final chunk: for gzip, finish the member (final block +
            // trailer); then record any remainder (a sub-target bySize tail, or the whole gzip finish
            // output). HEAD has no body, and a streaming response only exists once a chunk was added.
            if (r.firstChunkSeen && (r.gzipActive || !r.chunkPolicy.perChunk())
                    && !request.methodEquals("HEAD")) {
                final GzipEncoder enc = gzipEncoder();
                enc.streamFinish();
                if (enc.streamPending() > 0) {
                    recordChunkCut();
                }
            }
            if (!headersSent() && !startChunkedResponse()) {
                return;
            }
            if (request.methodEquals("HEAD")) {     // header-only; no last chunk
                if (out.position() == 0) {
                    finishResponse();
                } else {
                    enableWriteInterest();
                    suspendRead();
                }
                return;
            }
            bodyDriver.markStreamClosed();
            driveBody();
        }

        // Write the chunked response's status line + framing headers (once, on the first chunk or at
        // commit if there were none) and arm the incremental chunked driver.
        private boolean startChunkedResponse() {
            if (!writeHeaders(true, 0)) {
                return false;
            }
            responseState = RS_STREAMING;
            if (request.methodEquals("HEAD")) {
                bodyDriver.reset();                 // HEAD: no body
            } else {
                bodyDriver.beginChunked(response);
            }
            enterTimeoutState(TS_RESPONDING);       // arm the write timeout for the response drain
            suspendRead();                          // don't read the next request while responding
            tryFlush();                             // flush headers
            return true;
        }

        // Emit the status line, framing headers and any recorded custom headers into 'out'. Returns
        // false (after dropping the connection) if the header block overflows the output buffer.
        private boolean writeHeaders(final boolean chunked, final long contentLength) {
            final DefaultHttpResponse r = response;
            request.forceKeepAlive(r.keepAlive);
            responseWriter.statusLine(r.status);
            if (r.contentType != null) {        // header-only response (begun without a content type)
                responseWriter.contentType(r.contentType);
            }
            if (chunked) {
                responseWriter.transferEncodingChunked();
            } else {
                responseWriter.contentLength(contentLength);
            }
            if (r.gzipActive) {
                responseWriter.contentEncodingGzip();    // Content-Encoding: gzip
                responseWriter.varyAcceptEncoding();     // Vary: Accept-Encoding
            }
            responseWriter.connection(r.keepAlive);
            for (int i = 0; i < r.headerCount; i++) {
                final HeaderEntry h = r.headerEntry(i);
                if (!responseWriter.header(h.name, h.content)) {
                    // The header block overflowed the output buffer: unrecoverable (headers are not
                    // resumable across flushes), so fail the response and drop the connection.
                    handleException(new IllegalStateException("Response headers exceed output buffer"));
                    return false;
                }
                h.fire();   // value fully copied into 'out' -> the source is safe to reuse
            }
            responseWriter.endHeaders();
            r.markStarted();
            return true;
        }

        /**
         * Pump the body driver once and act on its tri-state result: finish when the whole body is
         * enqueued and drained; arm OP_WRITE and suspend reads on backpressure; or, for an open
         * chunked response that has drained the chunks recorded so far but is not yet committed, wait
         * for the next {@link DefaultHttpResponse#addChunk}/{@code commit()}. Reads stay suspended for the
         * whole response (HTTP/1.1 serialization).
         */
        private void driveBody() {
            final int result = bodyDriver.pump(this);
            tryFlush();
            if (result == ResponseBodyDriver.PUMP_DONE) {
                if (out.position() == 0) {
                    finishResponse();
                } else {
                    enableWriteInterest();      // body fully produced; drain the tail, finish in onWritable
                    suspendRead();
                }
            } else if (result == ResponseBodyDriver.PUMP_BLOCKED) {
                enableWriteInterest();          // backpressure: wait for OP_WRITE
                suspendRead();
            } else {                            // PUMP_AWAIT: open chunked, more chunks expected
                if (out.position() > 0) {
                    enableWriteInterest();
                }
                suspendRead();
                signalDrainIfIdle();            // chunks recorded so far are flushed -> unblock producer
            }
        }

        /**
         * Free-form per-connection slot for handler use -- e.g., to pool custom per-connection state
         * across requests without cross-thread sharing (a connection lives on one worker thread for
         * life).
         */
        public Object userState() {
            return userState;
        }

        public void setUserState(final Object state) {
            requireEventLoop();
            this.userState = state;
        }

        /**
         * Apply backpressure to the request body: stop delivering {@code onBodyChunk} callbacks and
         * stop reading from the socket (so TCP flow control throttles the sender) until
         * {@link #resumeInput()} is called. Intended to be called from within an {@code onBodyChunk}
         * callback on the worker thread when a downstream consumer cannot keep up.
         *
         * <p>See <em>3.1 Request-body backpressure</em> in the {@link HttpServer} manual for pausing while an
         * off-thread write of the chunk is in flight.
         */
        public void pauseInput() {
            requireEventLoop();
            inputPaused = true;
            suspendRead();
        }

        /**
         * Resume request-body delivery after {@link #pauseInput()}. Safe to call from any thread:
         * the work is marshalled onto the owning worker thread, where any already-buffered body is
         * re-parsed and reads are re-enabled.
         */
        public void resumeInput() {
            worker.execute(this::doResumeInput);
        }

        /**
         * Parse what is already sitting in {@code in} instead of waiting for another read. Called
         * where the buffer is in append mode: flip to read it, and compact back whatever a
         * half-arrived request leaves behind. The condition for calling it differs at each site and
         * stays there.
         */
        private void processBuffered() {
            in.flip();
            try {
                processInput();
            } finally {
                in.compact();
            }
        }

        private void doResumeInput() {
            if (!inputPaused) {
                return;
            }
            inputPaused = false;
            resumeRead();
            // Re-parse body bytes buffered in 'in' (append mode here) before waiting for more reads.
            if (!streamingBody() && in.position() > 0) {
                processBuffered();
            }
        }

        private boolean inputPaused() {
            return inputPaused;
        }

        private void finishResponse() {
            if (closed) {
                return; // driver aborted the response (e.g. body under-run) and already hard-closed
            }
            // The whole response drained: satisfy any outstanding whenDrained() before we recycle.
            if (drainWaiter != null) {
                final Future<Void> w = drainWaiter;
                drainWaiter = null;
                w.succeed(null);
            }
            final boolean keepAlive = request.keepAlive() && !closeAfterFlush;
            responseState = RS_SENT;
            if (!keepAlive) {
                closeAfterFlush();
                return;
            }
            resetForNextRequest();
            enterTimeoutState(TS_KEEPALIVE);    // arm the keep-alive idle timeout between requests
            resumeRead();
            // A pipelined request already buffered in 'in' is handled by the caller: the
            // synchronous path (processInput -> handler -> driveResponse) continues its own loop;
            // the asynchronous path (onWritable) drains 'in' explicitly. Doing it here would
            // re-read bytes while processInput is still iterating 'in' in read mode.
        }

        /**
         * Notify the handler of a protocol error and guarantee a response: the handler's
         * {@code onError} may render a custom reply, but if it does not (a no-op notification), the
         * server emits the default {@code status} response and closes. A throw from {@code onError}
         * is routed to {@code onException} + close.
         */
        private void onProtocolError(final int status, final String reason) {
            try {
                handler.onError(this, status, reason);
            } catch (final Throwable t) {
                handleException(t);
                return;
            }
            // Fallback if the handler treated onError as a pure notification. closeAfterFlush is
            // sticky (set by sendError), so a handler that DID respond skips this even if its
            // response already fully drained and hardClosed (which resets out.position() to 0).
            if (!closed && !closeAfterFlush && !streamingBody() && out.position() == 0) {
                sendError(status);
            }
        }

        /**
         * Terminal failure path: notify the handler ({@code onException}) then force-close (which
         * fires {@code onClose}). Idempotent via {@link #close()}.
         */
        private void handleException(final Throwable cause) {
            try {
                handler.onException(this, cause);
            } catch (final Throwable ignore) {
                // best-effort: a handler's onException must not mask the original failure
            }
            close();
        }

        /**
         * true once a response/close has been initiated (by an error response, {@code closeAfterFlush}
         * or {@code hardClose}); used by the parser to stop delivering request-body chunks after an
         * early rejection.
         */
        private boolean isClosing() {
            return closed || closeAfterFlush;
        }

        private void sendError(final int status) {
            sendError(status, null);
        }

        /**
         * Send a {@code status} error response and close. With a non-empty {@code reason}, that short
         * message (UTF-8) is the response body; the reason must fit the output buffer alongside the
         * header block (error messages are short) -- an over-long reason is dropped (status only).
         * If the response has already started (headers/chunks on the wire) the connection is dropped
         * instead, since the status can no longer be changed.
         */
        private void sendError(final int status, final CharSequence reason) {
            if (headersSent()) {
                close();
                return;
            }
            responseState = RS_IDLE;            // discard any deferred/half-built response
            response.recycleAll();              // discard any half-built (uncommitted) response
            enterTimeoutState(TS_RESPONDING);   // bound the drain of the error response too
            byte[] body = (reason == null || reason.length() == 0)
                    ? null : reason.toString().getBytes(StandardCharsets.UTF_8);
            responseWriter.statusLine(status);
            responseWriter.contentType(ContentType.TEXT_PLAIN);
            // Reserve a little room for the Content-Length + Connection lines and the blank line;
            // if the reason body would not fit, fall back to a status-only error.
            if (body != null && body.length > out.remaining() - 64) {
                body = null;
            }
            responseWriter.contentLength(body == null ? 0 : body.length);
            responseWriter.connection(false);
            responseWriter.endHeaders();
            if (body != null) {
                out.put(body);
            }
            closeAfterFlush = true; // errors close by default
            tryFlush();
        }

        private boolean tryFlush() {
            if (out.position() == 0) {
                return true;
            }
            out.flip();
            final int written;
            try {
                written = channel.write(out);
            } catch (final IOException e) {
                handleException(e);
                return false;
            }
            // Drain progress re-arms the write deadline: writeTimeoutMs bounds the time spent making
            // NO progress toward draining the response, not the response's total duration. A healthy
            // long stream (SSE/long-poll/large download) that keeps flushing bytes therefore survives,
            // while a genuinely stalled consumer still dies. Cheap (O(1) wheel reschedule per flush).
            if (written > 0 && timeoutState == TS_RESPONDING) {
                enterTimeoutState(TS_RESPONDING);
            }
            final boolean drained = !out.hasRemaining();
            out.compact();
            if (drained) {
                disableWriteInterest();
                if (closeAfterFlush && !streamingBody()) {
                    close();
                    return true;
                }
            } else {
                enableWriteInterest();
            }
            return drained;
        }

        private void onWritable() {
            if (!tryFlush()) {                   // still couldn't fully drain
                return;
            }
            if (streamingBody()) {
                final int result = bodyDriver.pump(this);
                tryFlush();
                if (result == ResponseBodyDriver.PUMP_DONE && out.position() == 0) {
                    finishResponse();
                    // 'in' is in append mode here (compacted by the last onReadable); process any
                    // request pipelined while we were streaming this response.
                    if (!streamingBody() && !closeAfterFlush && in.position() > 0) {
                        processBuffered();
                    }
                } else if (result == ResponseBodyDriver.PUMP_AWAIT && out.position() == 0) {
                    // streaming: drained the parts recorded so far, nothing pending -> idle until the
                    // next writeBody() appends more (OP_WRITE already cleared by tryFlush).
                    suspendRead();
                    signalDrainIfIdle();         // recorded chunks flushed -> unblock the producer chain
                } else {
                    // body not fully sent (backpressure) or tail still buffered: re-arm OP_WRITE (a
                    // preceding tryFlush may have cleared it after draining 'out') and keep reads
                    // suspended.
                    enableWriteInterest();
                    suspendRead();
                }
            } else if (closeAfterFlush && out.position() == 0) {
                close();
            }
        }

        private void suspendRead() {
            if (!key.isValid()) {
                return;
            }
            readSuspended = true;
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        }

        private void resumeRead() {
            if (!key.isValid()) {
                return;
            }
            readSuspended = false;
            key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        }

        private void enableWriteInterest() {
            if (key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            }
        }

        private void disableWriteInterest() {
            if (key.isValid()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        }

        /**
         * Immediately drop the connection (no flush). Part of the handler-facing reject API alongside
         * {@link #closeAfterFlush()} (graceful) and {@link #beginResponse()}{@code .error(status)}
         * (reject with a response); a handler may call it from any callback. Idempotent; fires
         * {@code onClose} exactly once.
         */
        public void close() {
            requireEventLoop();
            if (closed) {
                return;
            }
            closed = true;
            worker.wheel().cancel(timeoutTimer);
            worker.connectionClosed();
            try {
                if (key != null) {
                    key.cancel();
                }
            } catch (final Exception ignore) {
                // best-effort
            }
            try {
                channel.close();
            } catch (final IOException ignore) {
                // best-effort
            }
            try {
                handler.onClose(this);          // guaranteed terminal life-cycle callback
            } catch (final Throwable ignore) {
                // best-effort: onClose must not throw
            }
            // Fail any outstanding whenDrained() so a streaming producer's chain doesn't hang on a
            // connection that will never drain again.
            if (drainWaiter != null) {
                final Future<Void> w = drainWaiter;
                drainWaiter = null;
                w.failNow(new IllegalStateException("Connection closed"));
            }
            // Guarantee every recorded body item's release callback fires even on abnormal teardown,
            // so a caller waiting to reuse a buffer is never left hanging. Idempotent.
            response.recycleAll();
            if (gzipEncoder != null) {
                gzipEncoder.end();          // release the native deflater
                gzipEncoder = null;
            }
        }

        // The connection's reusable gzip compressor, created on first use (a connection that never
        // serves a gzip response never allocates a Deflater).
        private GzipEncoder gzipEncoder() {
            if (gzipEncoder == null) {
                gzipEncoder = new GzipEncoder(config.gzipLevel(), config.gzipStagingBytes(),
                        config.gzipScratchBytes(), config.gzipInitialOutBytes());
            }
            return gzipEncoder;
        }

        /**
         * Close this connection once everything already queued has reached the socket -- the graceful
         * counterpart of {@link #close()}, which drops it immediately. Sticky and idempotent: a
         * response committed after this still goes out, and the close follows it. Worker-thread-confined.
         */
        public void closeAfterFlush() {
            requireEventLoop();
            closeAfterFlush = true;
            if (out.position() == 0 && !streamingBody()) {
                close();
            } else {
                enableWriteInterest();
            }
        }
    }

    /**
     * A response header field-name, pre-encoded once as {@code "Name: "} (name + colon + space) for
     * zero-allocation emission. Build a custom one with {@link #of(String)} (validated as an RFC 7230
     * token) or use one of the common constants. The caller never deals with the {@code ": "}
     * separator or CRLF -- the server frames each value and terminates the line.
     *
     * <p>{@code CONTENT_TYPE}, {@code CONTENT_LENGTH}, {@code CONNECTION}, {@code TRANSFER_ENCODING},
     * {@code DATE} and {@code SERVER} are the framing/metadata headers the server emits itself; they
     * are exposed as constants so the writer emits them through this same type. A caller generally
     * should not re-add the framing ones as custom headers (duplicating {@code Content-Length} etc.
     * would corrupt the response), but {@link #of(String)} does not enforce this.
     *
     * <p>Building a custom field name once and reusing it as a constant is shown under
     * <em>8. Saying it without allocating</em> in the {@link HttpServer} manual.
     * Pass a shared {@link ByteContent} value (e.g. {@link ByteArrayContent#ascii}) instead of a
     * {@code CharSequence} to emit a constant header value with no per-response allocation.
     */
    public static final class HttpHeader {
        public static final HttpHeader LOCATION = of("Location");
        public static final HttpHeader CACHE_CONTROL = of("Cache-Control");
        public static final HttpHeader ETAG = of("ETag");
        public static final HttpHeader LAST_MODIFIED = of("Last-Modified");
        public static final HttpHeader SET_COOKIE = of("Set-Cookie");
        public static final HttpHeader VARY = of("Vary");
        public static final HttpHeader RETRY_AFTER = of("Retry-After");
        public static final HttpHeader WWW_AUTHENTICATE = of("WWW-Authenticate");
        public static final HttpHeader CONTENT_DISPOSITION = of("Content-Disposition");
        public static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN = of("Access-Control-Allow-Origin");
        public static final HttpHeader DATE = of("Date");
        public static final HttpHeader SERVER = of("Server");
        // Framing headers the server emits itself; exposed so the writer emits them through this type.
        public static final HttpHeader CONTENT_TYPE = of("Content-Type");
        public static final HttpHeader CONTENT_LENGTH = of("Content-Length");
        public static final HttpHeader CONNECTION = of("Connection");
        public static final HttpHeader TRANSFER_ENCODING = of("Transfer-Encoding");
        public static final HttpHeader CONTENT_ENCODING = of("Content-Encoding");

        private final byte[] nameColonSpace;

        private HttpHeader(final byte[] nameColonSpace) {
            this.nameColonSpace = nameColonSpace;
        }

        /**
         * Build a header for the given bare field-name (no colon, no CRLF). The name is validated once
         * as an RFC 7230 token (non-empty; visible ASCII excluding space, controls and separators), so
         * a malformed or injected name is rejected up front. Cheap enough to call once per constant.
         */
        public static HttpHeader of(final String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Header name must be non-empty");
            }
            for (int i = 0; i < name.length(); i++) {
                final char c = name.charAt(i);
                if (c <= 0x20 || c >= 0x7F || isSeparator(c)) {
                    throw new IllegalArgumentException("Illegal header-name char at index " + i + ": " + name);
                }
            }
            final byte[] bytes = new byte[name.length() + 2];
            for (int i = 0; i < name.length(); i++) {
                bytes[i] = (byte) name.charAt(i);
            }
            bytes[name.length()] = ':';
            bytes[name.length() + 1] = ' ';
            return new HttpHeader(bytes);
        }

        private byte[] nameColonSpace() {
            return nameColonSpace;
        }

        private static boolean isSeparator(final char c) {
            switch (c) {
                case '(': case ')': case '<': case '>': case '@':
                case ',': case ';': case ':': case '\\': case '"':
                case '/': case '[': case ']': case '?': case '=':
                case '{': case '}':
                    return true;
                default:
                    return false;
            }
        }
    }

    /**
     * A response {@code Content-Type} media type, pre-encoded once as its bare value bytes (no
     * {@code "Content-Type: "} prefix, no CRLF) for zero-allocation emission. Use one of the common
     * constants or build a custom one with {@link #of(String)}. Passed to
     * {@link Connection#beginResponse} / {@link Connection#beginChunkedResponse}; the server frames the
     * line.
     *
     * <p>See <em>8. Saying it without allocating</em> in the {@link HttpServer} manual for a custom media type
     * built once and shared.
     */
    public static final class ContentType {
        public static final ContentType TEXT_PLAIN = of("text/plain; charset=utf-8");
        public static final ContentType TEXT_HTML = of("text/html; charset=utf-8");
        public static final ContentType APPLICATION_JSON = of("application/json");
        public static final ContentType OCTET_STREAM = of("application/octet-stream");

        private final byte[] value;

        private ContentType(final byte[] value) {
            this.value = value;
        }

        /**
         * Build a content type for the given bare media type (no {@code "Content-Type: "}, no CRLF).
         * The value is validated once as visible ASCII plus space (so {@code "; charset=utf-8"} is
         * allowed); CR, LF, other control chars and non-ASCII are rejected to prevent response
         * splitting. Cheap enough to call once per constant.
         */
        public static ContentType of(final String mediaType) {
            if (mediaType == null || mediaType.isEmpty()) {
                throw new IllegalArgumentException("Media type must be non-empty");
            }
            for (int i = 0; i < mediaType.length(); i++) {
                final char c = mediaType.charAt(i);
                if (c < 0x20 || c > 0x7E) {
                    throw new IllegalArgumentException("Illegal media-type char at index " + i + ": " + mediaType);
                }
            }
            final byte[] bytes = new byte[mediaType.length()];
            for (int i = 0; i < mediaType.length(); i++) {
                bytes[i] = (byte) mediaType.charAt(i);
            }
            return new ContentType(bytes);
        }

        private byte[] value() {
            return value;
        }
    }

    /**
     * A response handle in any stage. Carries {@link #async(CompletionStage)} (bridge a JDK stage and
     * defer this response) and is the type accepted by the connection's other async ops
     * ({@link Connection#schedule}, {@link Connection#execute}), which
     * mark the handed-in response deferred. A response is begun in the request callback
     * ({@link Connection#beginResponse} / {@link Connection#beginChunkedResponse}) so its reference
     * can be passed to those ops.
     *
     * <p>Bridging an async data source and committing the reply from the continuation is shown
     * under <em>7. Work off the worker thread</em> in the {@link HttpServer} manual.
     * See {@link Future} for chaining several reads ({@code map}/{@code compose}) or gathering them
     * in parallel ({@link Future#all}).
     */
    public interface Response {
        /**
         * Bridge a JDK {@link java.util.concurrent.CompletionStage} (e.g. an AWS SDK async result)
         * into a worker-thread {@link Future} and mark this response deferred: the stage's completion
         * -- arriving on some library thread -- only calls {@code complete}/{@code fail}, which hop onto
         * this connection's worker thread. Call on the worker thread.
         */
        <T> Future<T> async(CompletionStage<T> stage);
    }

    /**
     * A fixed-length response under construction: collect the body (buffered; {@link #setBody} once
     * or {@link #addBody} in parts, its {@code Content-Length} the sum), set headers, then finish with
     * {@link #commit(int)} / {@link #ok()} (status + body) or {@link #error(int)} /
     * {@link #error(int, CharSequence)} (discard the collected body and send an error, optionally with
     * a short reason message body, instead). {@link #close()} drops the connection. The status is
     * supplied at commit, not at begin. Worker-thread-confined.
     *
     * <p>A response begun without a content type (via {@link Connection#beginResponse()} /
     * {@link Connection#beginResponse(boolean)}) is <em>header-only</em>: {@code setBody*}/{@code addBody*}
     * throw, and a bodyless {@link #commit(int)} emits {@code Content-Length: 0} with no
     * {@code Content-Type} line -- suitable for a redirect, {@code 204} or {@code 304}.
     *
     * <p>Whole-body, assembled-from-parts and discard-and-error forms are all worked through
     * under <em>5.1 Fixed length</em> and <em>5.2 Header-only replies and errors</em> in the {@link HttpServer} manual.
     */
    public interface HttpResponse extends Response {
        HttpResponse header(HttpHeader name, ByteContent value);
        HttpResponse header(HttpHeader name, ByteContent value, Runnable onReleased);
        HttpResponse header(HttpHeader name, CharSequence value);

        HttpResponse setBody(byte[] data);
        HttpResponse setBody(byte[] data, Runnable onReleased);
        HttpResponse setBody(byte[] data, int offset, int length);
        HttpResponse setBody(byte[] data, int offset, int length, Runnable onReleased);
        HttpResponse setBodyUtf8(CharSequence text);
        HttpResponse setBodyUtf8(CharSequence text, Runnable onReleased);
        HttpResponse setBodyAscii(CharSequence text);
        HttpResponse setBodyAscii(CharSequence text, Runnable onReleased);
        HttpResponse setBody(ByteContent content);
        HttpResponse setBody(ByteContent content, Runnable onReleased);

        HttpResponse addBody(byte[] data);
        HttpResponse addBody(byte[] data, Runnable onReleased);
        HttpResponse addBody(byte[] data, int offset, int length);
        HttpResponse addBody(byte[] data, int offset, int length, Runnable onReleased);
        HttpResponse addBodyUtf8(CharSequence text);
        HttpResponse addBodyUtf8(CharSequence text, Runnable onReleased);
        HttpResponse addBodyAscii(CharSequence text);
        HttpResponse addBodyAscii(CharSequence text, Runnable onReleased);
        HttpResponse addBody(ByteContent content);
        HttpResponse addBody(ByteContent content, Runnable onReleased);

        void commit(int status);
        /** Convenience for {@link #commit(int) commit(200)}; keep-alive follows the begun flag. */
        void ok();
        /**
         * As {@link #ok()}, but sends {@code Connection: close} and closes afterwards -- the pair of
         * {@link #errorAndClose(int)}. See {@link DefaultHttpResponse#okAndClose()} for why this is
         * not {@link #ok()} plus {@link Connection#closeAfterFlush()}.
         */
        void okAndClose();
        /**
         * Terminal {@code status} error; keep-alive follows the begun flag.
         * See {@link DefaultHttpResponse#error(int)}.
         */
        void error(int status);
        /**
         * Terminal {@code status} error with a short {@code reason} body (begun content type, else
         * text/plain); keep-alive follows the begun flag.
         */
        void error(int status, CharSequence reason);
        /** As {@link #error(int)}, but always closes the connection afterwards (forceful/transport-level). */
        void errorAndClose(int status);
        /** As {@link #error(int, CharSequence)}, but always closes the connection afterwards. */
        void errorAndClose(int status, CharSequence reason);
        /**
         * Drop the connection at once, discarding anything collected and sending nothing. For a
         * reply that should still go out before the connection closes, use {@link #okAndClose()} or
         * {@link #errorAndClose(int)}.
         */
        void close();
    }

    /**
     * A chunked ({@code Transfer-Encoding: chunked}) {@code 200 OK} response. Set headers, then append
     * chunks with {@link #addChunk} -- the first one writes the headers, each is flushed as it is added
     * -- and finish with {@link #commit()} (terminating last chunk). Before the first chunk you may
     * bail out with {@link #error(int)} / {@link #error(int, CharSequence)}; once a chunk is on the
     * wire the status can no longer change, so a later failure can only {@link #close()} the
     * connection. Worker-thread-confined.
     *
     * <p>Plain streaming and on-the-fly gzip with a coalescing {@link ChunkPolicy} are shown under
     * <em>5.3 Chunked</em> and <em>5.4 gzip, and how chunks are cut</em> in the {@link HttpServer} manual.
     * For a fast producer feeding a slow consumer, gate each add on {@link #whenDrained()} (below) so
     * chunks never buffer unboundedly.
     */
    public interface ChunkedResponse extends Response {
        ChunkedResponse header(HttpHeader name, ByteContent value);
        ChunkedResponse header(HttpHeader name, ByteContent value, Runnable onReleased);
        ChunkedResponse header(HttpHeader name, CharSequence value);

        ChunkedResponse addChunk(byte[] data);
        ChunkedResponse addChunk(byte[] data, Runnable onReleased);
        ChunkedResponse addChunk(byte[] data, int offset, int length);
        ChunkedResponse addChunk(byte[] data, int offset, int length, Runnable onReleased);
        ChunkedResponse addChunkUtf8(CharSequence text);
        ChunkedResponse addChunkUtf8(CharSequence text, Runnable onReleased);
        ChunkedResponse addChunkAscii(CharSequence text);
        ChunkedResponse addChunkAscii(CharSequence text, Runnable onReleased);
        ChunkedResponse addChunk(ByteContent content);
        ChunkedResponse addChunk(ByteContent content, Runnable onReleased);

        /**
         * Producer backpressure for streaming: a {@link Future} that completes (on the worker thread)
         * once every chunk added so far has been fully pushed to the socket and the output buffer has
         * drained. Chain the next {@code addChunk} off it so a fast producer never outruns a slow
         * consumer (which would otherwise buffer unboundedly) -- see
         * <em>6.2 Driven by the consumer</em> in the {@link HttpServer} manual.
         * Completes immediately if nothing is pending on the wire. If the connection is torn down while
         * outstanding, the future fails so the chain does not hang. Only one may be outstanding at a
         * time (a second call before the first completes throws). Worker-thread-confined.
         */
        Future<Void> whenDrained();

        void commit();
        /** Terminal {@code status} error (only before the first chunk); keep-alive follows the begun flag. */
        void error(int status);
        /** Terminal {@code status} error with a short {@code reason} body; keep-alive follows the begun flag. */
        void error(int status, CharSequence reason);
        /** As {@link #error(int)}, but always closes the connection afterwards (forceful/transport-level). */
        void errorAndClose(int status);
        /** As {@link #error(int, CharSequence)}, but always closes the connection afterwards. */
        void errorAndClose(int status, CharSequence reason);
        /**
         * Drop the connection at once, without the terminating chunk. The client is left with an
         * unfinished chunked body and must treat the response as failed -- which is the point: once
         * chunks are on the wire there is no status left to change, so this is the only way to
         * abandon a stream. To finish the stream <em>properly</em> and then hang up, {@link #commit()}
         * it and mark the connection with {@link Connection#closeAfterFlush()}.
         */
        void close();
    }

    /**
     * Immutable policy governing where a {@link ChunkedResponse} cuts HTTP chunk boundaries -- and, for a
     * gzip chunked response, where the deflate stream is {@code SYNC_FLUSH}ed. Orthogonal to gzip;
     * applies to both plain and compressed chunked bodies.
     *
     * <ul>
     *   <li>{@link #PER_CHUNK} (default): one HTTP chunk per {@link ChunkedResponse#addChunk} call, each
     *       flushed as it is added -- low latency, for interactive/streamed bodies.</li>
     *   <li>{@link #bySize(int)}: chunk boundaries fall when the bytes written to the wire since the last
     *       boundary reach {@code targetBytes} (raw bytes in plain mode, compressed bytes in gzip mode),
     *       with any remainder flushed at {@link ChunkedResponse#commit()}. Coalesces many small adds into
     *       fewer, larger chunks and improves the gzip ratio -- for throughput/bulk bodies. Note: under
     *       {@code bySize} an {@code addChunk} no longer necessarily reaches the client promptly; data may
     *       be buffered until the target or {@code commit()}.</li>
     * </ul>
     *
     * Instances are immutable and safe to share as constants across connections and threads.
     *
     * <p>Both settings are shown under <em>5.4 gzip, and how chunks are cut</em> in the {@link HttpServer} manual.
     */
    public static final class ChunkPolicy {
        /** One HTTP chunk per {@code addChunk}, flushed eagerly (the default). */
        public static final ChunkPolicy PER_CHUNK = new ChunkPolicy(0);

        private final int targetBytes;   // 0 => per-chunk; >0 => cut when written bytes since last cut reach it

        private ChunkPolicy(final int targetBytes) {
            this.targetBytes = targetBytes;
        }

        /**
         * Cut a chunk once {@code targetBytes} bytes have been written to the wire since the last cut
         * (compressed bytes for a gzip response), coalescing smaller adds. {@code targetBytes} must be
         * positive.
         */
        public static ChunkPolicy bySize(final int targetBytes) {
            if (targetBytes <= 0) {
                throw new IllegalArgumentException("targetBytes must be > 0: " + targetBytes);
            }
            return new ChunkPolicy(targetBytes);
        }

        private boolean perChunk() {
            return targetBytes == 0;
        }

        private int targetBytes() {
            return targetBytes;
        }
    }

    /**
     * Reusable, per-{@link Connection} concrete response -- the write-side mirror of
     * {@link HttpRequest}, implementing the staged {@link HttpResponse} / {@link ChunkedResponse}
     * views (callers program to those interfaces; this class is the single pooled backing instance).
     * One instance per connection, reused across requests (worker-thread-confined). The built-in body
     * wrappers and entry slots are pooled here, so steady state is allocation-free.
     */
    private static final class DefaultHttpResponse implements HttpResponse, ChunkedResponse {
        private final Connection connection;

        // Response description. The status is NOT set at begin(): it is decided later by
        // commit(status) (fixed); chunked is always 200 OK. error(status) overrides it.
        private int status;
        private ContentType contentType;   // framed into a "Content-Type: ...\r\n" line; null => header-only (no body)
        private long contentLength;   // fixed: sum of body parts (-1 => header-only); unused if chunked
        private boolean keepAlive;
        private boolean chunked;
        private boolean gzip;         // caller opted a fixed body into gzip (see beginResponse(..., gzip))
        private boolean gzipActive;   // gzip && client accepts gzip && body present -- decided at commit
        private ChunkPolicy chunkPolicy;  // chunked: where chunk boundaries fall (per-add vs. by size)
        private boolean firstChunkSeen;   // chunked: first addChunk observed -- gzip/streaming decided then

        // Lifecycle guards (reset by begin()). 'started' = the header block has been written to the
        // wire (at commit for a fixed response, at the first addChunk for a chunked one) -- after that no
        // header may be added. 'done' = terminal: committed or errored.
        private boolean started;
        private boolean done;

        // Recorded body items. A fixed response uses exactly entries[0]; a chunked response uses
        // entries[0..entryCount) in order. Grown lazily and reused across requests; the BodyEntry
        // objects and the built-in content wrappers they hold are pooled.
        private BodyEntry[] entries;
        private int entryCount;

        // Recorded custom response headers, emitted by commit() between the framing headers and the
        // terminating blank line. Grown lazily and reused across requests like 'entries'.
        private HeaderEntry[] headers;
        private int headerCount;

        // Free-lists (LIFO) of the built-in, re-pointable content wrappers, reused across requests.
        private PooledByteArrayContent[] arrayPool;
        private int arrayPoolCount;
        private Utf8Content[] utf8Pool;
        private int utf8PoolCount;
        private AsciiContent[] asciiPool;
        private int asciiPoolCount;

        DefaultHttpResponse(final Connection connection) {
            this.connection = connection;
        }

        private DefaultHttpResponse begin(final ContentType contentType, final boolean keepAlive,
                                          final boolean chunked, final boolean gzip,
                                          final ChunkPolicy chunkPolicy) {
            recycleAll();                  // defensive: reclaim anything a prior response left behind
            connection.markResponseBegun(); // single choke point: lets a dispatcher detect ownership
            this.status = chunked ? 200 : 0;  // chunked is always 200 OK; fixed status set by commit(status)
            this.contentType = contentType;
            this.contentLength = -1L;      // fixed: sum of body parts; -1 => header-only
            this.keepAlive = keepAlive;
            this.chunked = chunked;
            this.gzip = gzip;              // caller opted into gzip; activated once negotiated
            this.gzipActive = false;      // decided at commit (fixed) / first chunk (chunked)
            this.chunkPolicy = chunkPolicy;
            this.firstChunkSeen = false;
            this.started = false;
            this.done = false;
            return this;
        }

        /** Marked by the connection once the header block has been written to the wire. */
        private void markStarted() {
            started = true;
        }

        private boolean isDone() {
            return done;
        }

        public <T> Future<T> async(final CompletionStage<T> stage) {
            return connection.async(this, stage);
        }

        /**
         * Add a response header with a {@link ByteContent} value (the bare value bytes). Use a
         * {@link ByteArrayContent} constant for a static value or any {@link ByteContent} for a
         * dynamic one; the content is not pooled by the server.
         */
        public DefaultHttpResponse header(final HttpHeader name, final ByteContent value) {
            addHeader(name, value, null, false);
            return this;
        }

        /**
         * As {@link #header(HttpHeader, ByteContent)}, but {@code onReleased} runs once the value has
         * been fully copied into the output buffer (or at teardown) -- for a value backed by a
         * leased/poolable buffer the caller wants to reuse. Must not block or throw.
         */
        public DefaultHttpResponse header(final HttpHeader name, final ByteContent value, final Runnable onReleased) {
            addHeader(name, value, onReleased, false);
            return this;
        }

        /**
         * Add a response header with a text value (encoded one byte per char). CR/LF is rejected to
         * prevent response splitting.
         */
        public DefaultHttpResponse header(final HttpHeader name, final CharSequence value) {
            for (int i = 0; i < value.length(); i++) {
                final char c = value.charAt(i);
                if (c == '\r' || c == '\n') {
                    throw new IllegalArgumentException("CR/LF not allowed in a header value");
                }
            }
            final AsciiContent content = acquireAscii();
            content.reset(value);
            addHeader(name, content, null, true);
            return this;
        }

        public DefaultHttpResponse setBody(final byte[] data) {
            return setBody(data, 0, data.length, null);
        }

        public DefaultHttpResponse setBody(final byte[] data, final Runnable onReleased) {
            return setBody(data, 0, data.length, onReleased);
        }

        public DefaultHttpResponse setBody(final byte[] data, final int offset, final int length) {
            return setBody(data, offset, length, null);
        }

        public DefaultHttpResponse setBody(final byte[] data, final int offset, final int length,
                                    final Runnable onReleased) {
            final PooledByteArrayContent content = acquireArray();
            content.reset(data, offset, length);
            single(content, onReleased, true);
            contentLength = length;
            return this;
        }

        public DefaultHttpResponse setBodyUtf8(final CharSequence text) {
            return setBodyUtf8(text, null);
        }

        public DefaultHttpResponse setBodyUtf8(final CharSequence text, final Runnable onReleased) {
            final Utf8Content content = acquireUtf8();
            content.reset(text);
            single(content, onReleased, true);
            contentLength = content.length();
            return this;
        }

        public DefaultHttpResponse setBodyAscii(final CharSequence text) {
            return setBodyAscii(text, null);
        }

        public DefaultHttpResponse setBodyAscii(final CharSequence text, final Runnable onReleased) {
            final AsciiContent content = acquireAscii();
            content.reset(text);
            single(content, onReleased, true);
            contentLength = content.length();
            return this;
        }

        /**
         * Attach a custom body; the content is not pooled. Its {@link ByteContent#length()} (a binding
         * guarantee, {@code >= 0}) becomes the Content-Length.
         */
        public DefaultHttpResponse setBody(final ByteContent content) {
            return setBody(content, null);
        }

        public DefaultHttpResponse setBody(final ByteContent content, final Runnable onReleased) {
            final long len = content.length();
            if (len < 0) {
                throw new IllegalArgumentException("ByteContent.length() must be >= 0, was " + len);
            }
            single(content, onReleased, false);
            contentLength = len;   // the body's length is the Content-Length
            return this;
        }

        public DefaultHttpResponse addBody(final byte[] data) {
            return addBody(data, 0, data.length, null);
        }

        public DefaultHttpResponse addBody(final byte[] data, final Runnable onReleased) {
            return addBody(data, 0, data.length, onReleased);
        }

        public DefaultHttpResponse addBody(final byte[] data, final int offset, final int length) {
            return addBody(data, offset, length, null);
        }

        public DefaultHttpResponse addBody(final byte[] data, final int offset, final int length,
                                    final Runnable onReleased) {
            final PooledByteArrayContent content = acquireArray();
            content.reset(data, offset, length);
            appendBody(content, onReleased, true, length);
            return this;
        }

        public DefaultHttpResponse addBodyUtf8(final CharSequence text) {
            return addBodyUtf8(text, null);
        }

        public DefaultHttpResponse addBodyUtf8(final CharSequence text, final Runnable onReleased) {
            final Utf8Content content = acquireUtf8();
            content.reset(text);
            appendBody(content, onReleased, true, content.length());
            return this;
        }

        public DefaultHttpResponse addBodyAscii(final CharSequence text) {
            return addBodyAscii(text, null);
        }

        public DefaultHttpResponse addBodyAscii(final CharSequence text, final Runnable onReleased) {
            final AsciiContent content = acquireAscii();
            content.reset(text);
            appendBody(content, onReleased, true, content.length());
            return this;
        }

        public DefaultHttpResponse addBody(final ByteContent content) {
            return addBody(content, null);
        }

        public DefaultHttpResponse addBody(final ByteContent content, final Runnable onReleased) {
            final long len = content.length();
            if (len < 0) {
                throw new IllegalArgumentException("ByteContent.length() must be >= 0, was " + len);
            }
            appendBody(content, onReleased, false, len);
            return this;
        }

        public DefaultHttpResponse addChunk(final byte[] data) {
            return addChunk(data, 0, data.length, null);
        }

        public DefaultHttpResponse addChunk(final byte[] data, final Runnable onReleased) {
            return addChunk(data, 0, data.length, onReleased);
        }

        public DefaultHttpResponse addChunk(final byte[] data, final int offset, final int length) {
            return addChunk(data, offset, length, null);
        }

        public DefaultHttpResponse addChunk(final byte[] data, final int offset, final int length,
                                     final Runnable onReleased) {
            final PooledByteArrayContent content = acquireArray();
            content.reset(data, offset, length);
            return addChunkInternal(content, onReleased, true);
        }

        public DefaultHttpResponse addChunkUtf8(final CharSequence text) {
            return addChunkUtf8(text, null);
        }

        public DefaultHttpResponse addChunkUtf8(final CharSequence text, final Runnable onReleased) {
            final Utf8Content content = acquireUtf8();
            content.reset(text);
            return addChunkInternal(content, onReleased, true);
        }

        public DefaultHttpResponse addChunkAscii(final CharSequence text) {
            return addChunkAscii(text, null);
        }

        public DefaultHttpResponse addChunkAscii(final CharSequence text, final Runnable onReleased) {
            final AsciiContent content = acquireAscii();
            content.reset(text);
            return addChunkInternal(content, onReleased, true);
        }

        public DefaultHttpResponse addChunk(final ByteContent content) {
            return addChunk(content, null);
        }

        public DefaultHttpResponse addChunk(final ByteContent content, final Runnable onReleased) {
            final long len = content.length();
            if (len < 0) {
                throw new IllegalArgumentException("ByteContent.length() must be >= 0, was " + len);
            }
            return addChunkInternal(content, onReleased, false);
        }

        /**
         * Commit a fixed response with {@code status}: write the headers ({@code Content-Length} = the
         * sum of the collected body parts) and the buffered body. Terminal.
         */
        public void commit(final int status) {
            requireNotDone();
            requireFixed();
            connection.requireEventLoop();
            this.status = status;
            done = true;
            connection.commitResponse();
        }

        /** Convenience for {@link #commit(int) commit(200)}; keep-alive follows the begun flag. */
        public void ok() {
            commit(200);
        }

        /**
         * As {@link #ok()}, but drops the keep-alive the response was begun with, so the reply goes
         * out carrying {@code Connection: close} and the socket closes behind it -- the successful
         * counterpart of {@link #errorAndClose(int)}.
         * <p>
         * Not the same as {@link #ok()} followed by {@link Connection#closeAfterFlush()}: the header
         * is written from the keep-alive flag as it stands at commit, so that pair announces
         * {@code keep-alive} and then closes anyway, leaving the client to discover it. This sets
         * the flag first, so what the client is told and what happens agree. Terminal.
         */
        public void okAndClose() {
            requireNotDone();
            requireFixed();
            connection.requireEventLoop();
            this.keepAlive = false;
            commit(200);
        }

        /** {@link ChunkedResponse#whenDrained()}: delegate to the connection's drain tracking. */
        public Future<Void> whenDrained() {
            return connection.whenDrained(this);
        }

        /**
         * Commit a chunked response: write the terminating last chunk (the chunks themselves were
         * already flushed by {@link #addChunk}). Terminal.
         */
        public void commit() {
            requireNotDone();
            requireChunked();
            requireStarted();
            connection.requireEventLoop();
            done = true;
            connection.commitResponse();
        }

        /**
         * Turn the response into a terminal {@code status} error, discarding any body collected so far.
         * Keep-alive <b>follows the flag the response was begun with</b> ({@code beginResponse(...,
         * keepAlive)}): an HTTP-level 4xx/5xx on a keep-alive connection stays open, matching how popular
         * clients keep such a connection pooled. Valid only before anything reaches the wire -- a fixed
         * response before {@link #commit(int)}, a chunked response before its first chunk; once headers
         * are on the wire the status can no longer change, so the connection is dropped. Terminal.
         */
        public void error(final int status) {
            error(status, null);
        }

        /**
         * As {@link #error(int)}, but with a short {@code reason} as the response body, sent with the
         * <b>content type the response was begun with</b> (e.g. {@code application/json}), defaulting to
         * {@code text/plain} when the response was begun without one. The {@code reason} bytes are sent
         * verbatim under that content type &mdash; no conversion is done, so the caller is responsible for
         * matching the two (a JSON reason under a JSON-begun response, plain text otherwise). Terminal.
         */
        public void error(final int status, final CharSequence reason) {
            commitError(status, reason, keepAlive);
        }

        /**
         * As {@link #error(int)}, but always closes the connection afterwards regardless of the begun
         * keep-alive flag -- for a forceful or transport-level rejection. Terminal.
         */
        public void errorAndClose(final int status) {
            errorAndClose(status, null);
        }

        /** As {@link #error(int, CharSequence)}, but always closes the connection afterwards. Terminal. */
        public void errorAndClose(final int status, final CharSequence reason) {
            commitError(status, reason, false);
        }

        // Terminal error: drop any half-built body, then commit a fixed error response honoring
        // keepAlive and the begun content type (text/plain default). Reuses the normal commit path so
        // keep-alive disposition, drain, reset and pipelining all apply. Once the headers are on the
        // wire the status is fixed, so the connection is dropped instead.
        private void commitError(final int status, final CharSequence reason, final boolean keepAlive) {
            requireNotDone();
            connection.requireEventLoop();
            if (started) {
                done = true;
                connection.close();     // headers already sent: cannot change the status -> drop
                return;
            }
            recycleBody();              // discard any collected body (keeps content type + headers)
            chunked = false;            // an error is always a fixed response
            // Keep-alive only when it was requested AND the whole request has been read: an early error
            // (body still pending) would otherwise leave undrained request bytes that desync the next
            // request on the connection. Matches "body not fully read -> connection not reused".
            this.keepAlive = keepAlive && connection.requestFullyRead();
            this.gzip = false;          // never gzip a small error body
            contentLength = -1L;        // bodyless unless a reason is set below
            if (reason != null && reason.length() > 0) {
                if (contentType == null) {
                    contentType = ContentType.TEXT_PLAIN;   // default framing for the reason body
                }
                setBodyUtf8(reason);    // records the body and its Content-Length
            }
            commit(status);
        }

        /** Drop the connection (delegates to {@link Connection#close()}). */
        public void close() {
            connection.close();
        }

        private void requireNotDone() {
            if (done) {
                throw new IllegalStateException("Response already completed");
            }
        }

        private void requireFixed() {
            if (chunked) {
                throw new IllegalStateException("Not a fixed response; use addChunk");
            }
        }

        private void requireChunked() {
            if (!chunked) {
                throw new IllegalStateException("Not a chunked response; use setBody/addBody");
            }
        }

        private void requireStarted() {
            if (!started) {
                throw new IllegalStateException("Add at least one chunk before commit()");
            }
        }

        private void requireContentType() {
            if (contentType == null) {
                throw new IllegalStateException(
                        "Response begun without a content type is header-only; cannot add a body/chunk");
            }
        }

        // Record the single body of a fixed response; a later setBody* replaces (and releases) an
        // earlier one. Guards run before recycleAll so a rejected call leaves the prior body intact.
        private void single(final ByteContent content, final Runnable listener, final boolean owned) {
            requireNotDone();
            requireFixed();
            requireContentType();
            connection.requireEventLoop();
            recycleBody();                  // release any already-recorded body (fire callbacks, pool wrappers)
            addEntry(content, listener, owned);
        }

        // Append one part to a fixed (buffered) body; the Content-Length is the running sum.
        private void appendBody(final ByteContent content, final Runnable listener, final boolean owned,
                                final long len) {
            requireNotDone();
            requireFixed();
            requireContentType();
            connection.requireEventLoop();
            addEntry(content, listener, owned);
            contentLength = (contentLength < 0 ? 0 : contentLength) + len;
        }

        // Replace the recorded raw body parts with the single compressed buffer produced by the
        // connection's GzipEncoder (which has already read -- and here we release -- the raw parts). The
        // compressed view is connection-owned, so it is recorded as not-owned (never pooled by this
        // response). The compressed length becomes the Content-Length.
        private void useGzipBody(final ByteContent compressed) {
            recycleBody();
            addEntry(compressed, null, false);
            contentLength = compressed.length();
        }

        // Record one chunk and (per the ChunkPolicy / gzip) flush it. The first chunk writes the 200 OK
        // header block first; the connection's chunkAdded() handles that transition and the compression /
        // size-coalescing.
        private DefaultHttpResponse addChunkInternal(final ByteContent content, final Runnable listener,
                                              final boolean owned) {
            requireNotDone();
            requireChunked();
            requireContentType();
            connection.requireEventLoop();
            connection.chunkAdded(content, listener, owned);
            return this;
        }

        private void addEntry(final ByteContent content, final Runnable listener, final boolean owned) {
            if (entries == null) {
                entries = new BodyEntry[4];
            } else if (entryCount == entries.length) {
                entries = Arrays.copyOf(entries, entries.length * 2);
            }
            BodyEntry entry = entries[entryCount];
            if (entry == null) {
                entry = new BodyEntry();
                entries[entryCount] = entry;
            }
            entry.set(content, listener, owned);
            entryCount++;
        }

        private BodyEntry entry(final int index) {
            return entries[index];
        }

        private HeaderEntry headerEntry(final int index) {
            return headers[index];
        }

        private void addHeader(final HttpHeader name, final ByteContent value, final Runnable listener,
                               final boolean owned) {
            requireNotDone();
            if (started) {
                // The header block is written at commit (fixed) or on the first chunk (chunked); a
                // header added after that would never reach the wire.
                throw new IllegalStateException("Headers already sent; add headers before commit/first addChunk");
            }
            if (headers == null) {
                headers = new HeaderEntry[4];
            } else if (headerCount == headers.length) {
                headers = Arrays.copyOf(headers, headers.length * 2);
            }
            HeaderEntry entry = headers[headerCount];
            if (entry == null) {
                entry = new HeaderEntry();
                headers[headerCount] = entry;
            }
            entry.set(name.nameColonSpace(), value, listener, owned);
            headerCount++;
        }

        /**
         * Fire each recorded header's release listener (backstop) and return any server-acquired value
         * wrapper to its pool. Idempotent; invoked from {@link #recycleAll()}.
         */
        private void recycleHeaders() {
            for (int i = 0; i < headerCount; i++) {
                final HeaderEntry entry = headers[i];
                entry.fire();               // no-op if commit already fired it after copying the value
                if (entry.owned) {
                    final ByteContent content = entry.content;
                    if (content instanceof AsciiContent) {
                        ((AsciiContent) content).clear();
                        pushAscii((AsciiContent) content);
                    } else if (content instanceof PooledByteArrayContent) {
                        ((PooledByteArrayContent) content).clear();
                        pushArray((PooledByteArrayContent) content);
                    } else if (content instanceof Utf8Content) {
                        ((Utf8Content) content).clear();
                        pushUtf8((Utf8Content) content);
                    }
                }
                entry.clearRefs();
            }
            headerCount = 0;
        }

        /**
         * Fire the release callback for every recorded entry not already fired (the {@code onReleased} Runnable),
         * return the built-in wrappers to their pools with their data references nulled, and clear the
         * recorded model. Idempotent -- an entry consumed (and fired) by the driver is skipped here.
         */
        private void recycleAll() {
            recycleBody();
            recycleHeaders();
        }

        /** Release the recorded body entries only (used by setBody's last-set-wins replacement). */
        private void recycleBody() {
            for (int i = 0; i < entryCount; i++) {
                final BodyEntry entry = entries[i];
                entry.fire();               // no-op if the driver already fired it at consume
                if (entry.owned) {
                    // Only wrappers the server itself acquired go back to the pool; a user-supplied
                    // ByteContent (even one of the now-public wrapper types) is never captured.
                    final ByteContent content = entry.content;
                    if (content instanceof PooledByteArrayContent) {
                        ((PooledByteArrayContent) content).clear();
                        pushArray((PooledByteArrayContent) content);
                    } else if (content instanceof Utf8Content) {
                        ((Utf8Content) content).clear();
                        pushUtf8((Utf8Content) content);
                    } else if (content instanceof AsciiContent) {
                        ((AsciiContent) content).clear();
                        pushAscii((AsciiContent) content);
                    }
                }
                entry.clearRefs();
            }
            entryCount = 0;
        }

        private PooledByteArrayContent acquireArray() {
            return arrayPoolCount > 0 ? arrayPool[--arrayPoolCount] : new PooledByteArrayContent();
        }

        private void pushArray(final PooledByteArrayContent content) {
            if (arrayPool == null) {
                arrayPool = new PooledByteArrayContent[4];
            } else if (arrayPoolCount == arrayPool.length) {
                arrayPool = Arrays.copyOf(arrayPool, arrayPool.length * 2);
            }
            arrayPool[arrayPoolCount++] = content;
        }

        private Utf8Content acquireUtf8() {
            return utf8PoolCount > 0 ? utf8Pool[--utf8PoolCount] : new Utf8Content();
        }

        private void pushUtf8(final Utf8Content content) {
            if (utf8Pool == null) {
                utf8Pool = new Utf8Content[4];
            } else if (utf8PoolCount == utf8Pool.length) {
                utf8Pool = Arrays.copyOf(utf8Pool, utf8Pool.length * 2);
            }
            utf8Pool[utf8PoolCount++] = content;
        }

        private AsciiContent acquireAscii() {
            return asciiPoolCount > 0 ? asciiPool[--asciiPoolCount] : new AsciiContent();
        }

        private void pushAscii(final AsciiContent content) {
            if (asciiPool == null) {
                asciiPool = new AsciiContent[4];
            } else if (asciiPoolCount == asciiPool.length) {
                asciiPool = Arrays.copyOf(asciiPool, asciiPool.length * 2);
            }
            asciiPool[asciiPoolCount++] = content;
        }
    }

    /**
     * Write side of a {@link Future}, bound to a {@link Connection}. {@link #complete(Object)} and
     * {@link #fail(Throwable)} may be called from <em>any</em> thread (e.g. an AWS SDK completion
     * thread); the first call wins and the future's continuations are then delivered on the
     * connection's worker thread. Internal plumbing: built by {@link Connection#async} to bridge a JDK
     * {@link CompletionStage} (via {@link Response#async(CompletionStage)}) onto the worker thread.
     */
    private static final class Promise<T> {
        private final Connection connection;
        private final Future<T> future;
        private final AtomicBoolean completed = new AtomicBoolean();

        Promise(final Connection connection) {
            this.connection = connection;
            this.future = new Future<>(connection);
        }

        /** The read side handed to callers to attach continuations. */
        public Future<T> future() {
            return future;
        }

        /** Complete successfully (any thread); continuations run on the worker thread. First call wins. */
        public void complete(final T value) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }
            connection.runOnLoop(() -> future.succeed(value));
        }

        /** Complete with a failure (any thread); continuations run on the worker thread. First call wins. */
        public void fail(final Throwable error) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }
            connection.runOnLoop(() -> future.failNow(error));
        }
    }

    /**
     * A composable async result bound to a {@link Connection}'s worker thread: every continuation and
     * every {@link #map}/{@link #compose}/{@link #recover} function runs on that thread, so a handler
     * can assemble a response from async reads without ever touching connection state off-thread.
     * Completed through a {@link Promise} (typically via {@link Response#async}) or derived from
     * another future. Not thread-safe: all building and completion happen on the worker thread (a
     * {@code Promise} hops there first).
     *
     * <p>Chain dependent reads with {@link #map}/{@link #compose}, or fan several out and gather
     * them in order with {@link #all}; both are worked through under <em>7. Work off the worker
     * thread</em> in the {@link HttpServer} manual. {@link #recover} supplies a fallback value for a failed step.
     */
    public static final class Future<T> {
        private final Connection connection;
        private boolean completed;
        private T value;
        private Throwable error;
        private List<BiConsumer<T, Throwable>> handlers;

        private Future(final Connection connection) {
            this.connection = connection;
        }

        private void succeed(final T v) {
            if (completed) {
                return;
            }
            completed = true;
            value = v;
            fire();
        }

        private void failNow(final Throwable e) {
            if (completed) {
                return;
            }
            completed = true;
            error = e;
            fire();
        }

        private void fire() {
            if (handlers == null) {
                return;
            }
            final List<BiConsumer<T, Throwable>> hs = handlers;
            handlers = null;
            for (int i = 0; i < hs.size(); i++) {
                try {
                    hs.get(i).accept(value, error);
                } catch (final Throwable ignore) {
                    // best-effort: one continuation must not disrupt the others
                }
            }
        }

        private void addHandler(final BiConsumer<T, Throwable> h) {
            connection.requireEventLoop();
            if (completed) {
                try {
                    h.accept(value, error);
                } catch (final Throwable ignore) {
                    // best-effort
                }
                return;
            }
            if (handlers == null) {
                handlers = new ArrayList<>(2);
            }
            handlers.add(h);
        }

        public Future<T> onSuccess(final Consumer<? super T> action) {
            addHandler((v, e) -> {
                if (e == null) {
                    action.accept(v);
                }
            });
            return this;
        }

        public Future<T> onFailure(final Consumer<? super Throwable> action) {
            addHandler((v, e) -> {
                if (e != null) {
                    action.accept(e);
                }
            });
            return this;
        }

        public Future<T> onComplete(final BiConsumer<? super T, ? super Throwable> action) {
            addHandler((v, e) -> action.accept(v, e));
            return this;
        }

        /** Synchronously transform the success value; a throw fails the derived future. */
        public <U> Future<U> map(final Function<? super T, ? extends U> fn) {
            final Future<U> out = new Future<>(connection);
            addHandler((v, e) -> {
                if (e != null) {
                    out.failNow(e);
                    return;
                }
                final U mapped;
                try {
                    mapped = fn.apply(v);
                } catch (final Throwable t) {
                    out.failNow(t);
                    return;
                }
                out.succeed(mapped);
            });
            return out;
        }

        /** Chain another async step; flattens so read-A-then-B-then-C stays a flat pipeline. */
        public <U> Future<U> compose(final Function<? super T, Future<U>> fn) {
            final Future<U> out = new Future<>(connection);
            addHandler((v, e) -> {
                if (e != null) {
                    out.failNow(e);
                    return;
                }
                final Future<U> next;
                try {
                    next = fn.apply(v);
                } catch (final Throwable t) {
                    out.failNow(t);
                    return;
                }
                next.addHandler((v2, e2) -> {
                    if (e2 != null) {
                        out.failNow(e2);
                    } else {
                        out.succeed(v2);
                    }
                });
            });
            return out;
        }

        /** Single error-recovery point: map a failure to a fallback value. */
        public Future<T> recover(final Function<? super Throwable, ? extends T> fn) {
            final Future<T> out = new Future<>(connection);
            addHandler((v, e) -> {
                if (e == null) {
                    out.succeed(v);
                    return;
                }
                final T recovered;
                try {
                    recovered = fn.apply(e);
                } catch (final Throwable t) {
                    out.failNow(t);
                    return;
                }
                out.succeed(recovered);
            });
            return out;
        }

        // Register a handler ignoring the value type (used by all()).
        private void addAny(final BiConsumer<Object, Throwable> h) {
            addHandler((v, e) -> h.accept(v, e));
        }

        /**
         * Combine futures: completes with their results <em>in order</em> once all succeed, or fails
         * fast with the first failure -- the parallel fan-out gather. All futures must belong to the
         * same connection; call on the worker thread.
         */
        public static Future<Object[]> all(final Future<?>... futures) {
            if (futures.length == 0) {
                throw new IllegalArgumentException("all() requires at least one future");
            }
            final Future<Object[]> out = new Future<>(futures[0].connection);
            final Object[] results = new Object[futures.length];
            final int[] remaining = { futures.length };
            final boolean[] settled = { false };
            for (int i = 0; i < futures.length; i++) {
                final int idx = i;
                futures[i].addAny((v, e) -> {
                    if (settled[0]) {
                        return;
                    }
                    if (e != null) {
                        settled[0] = true;
                        out.failNow(e);
                        return;
                    }
                    results[idx] = v;
                    if (--remaining[0] == 0) {
                        settled[0] = true;
                        out.succeed(results);
                    }
                });
            }
            return out;
        }
    }

    /**
     * One recorded body item: the {@link ByteContent} that produces its bytes, the optional
     * {@code onReleased} listener run once the data is safe to reuse, and a one-shot {@code released}
     * latch. Pooled and reused by {@link DefaultHttpResponse}.
     */
    private static final class BodyEntry {
        private ByteContent content;
        private Runnable listener;
        private boolean released;
        private boolean owned;      // true if the server acquired the wrapper (pool it) vs. a user-supplied one

        private void set(final ByteContent content, final Runnable listener, final boolean owned) {
            this.content = content;
            this.listener = listener;
            this.released = false;
            this.owned = owned;
        }

        /** Fire the release callback exactly once (best-effort; a listener must not throw or block). */
        private void fire() {
            if (released) {
                return;
            }
            released = true;
            fireListener(listener);
        }

        private void clearRefs() {
            content = null;
            listener = null;
            released = false;
            owned = false;
        }
    }

    /**
     * One recorded custom response header: the pre-encoded {@code "Name: "} bytes, the
     * {@link ByteContent} that produces its value, an optional {@code onReleased} listener, and the
     * {@code owned} flag (true when the server acquired the value wrapper, so it may be pooled).
     * Pooled and reused by {@link DefaultHttpResponse}.
     */
    private static final class HeaderEntry {
        private byte[] name;
        private ByteContent content;
        private Runnable listener;
        private boolean released;
        private boolean owned;

        private void set(final byte[] name, final ByteContent content, final Runnable listener, final boolean owned) {
            this.name = name;
            this.content = content;
            this.listener = listener;
            this.released = false;
            this.owned = owned;
        }

        /** Fire the release callback exactly once (best-effort; a listener must not throw or block). */
        private void fire() {
            if (released) {
                return;
            }
            released = true;
            fireListener(listener);
        }

        private void clearRefs() {
            name = null;
            content = null;
            listener = null;
            released = false;
            owned = false;
        }
    }

    /**
     * Built-in immutable {@link ByteContent} over a {@code byte[]} range -- a shareable, thread-safe
     * constant (its {@link #write} never mutates the source or the target beyond the copied bytes).
     * The server's internal pooling reuse lives on the private {@code PooledByteArrayContent}
     * wrapper, so a user-constructed constant carries no mutators.
     *
     * <p>Encoding once at startup and sharing across every connection and worker is shown under
     * <em>8. Saying it without allocating</em> in the {@link HttpServer} manual.
     */
    public static class ByteArrayContent implements ByteContent {
        public static ByteArrayContent ascii(final String value) {
            return new ByteArrayContent(value.getBytes(StandardCharsets.US_ASCII));
        }

        public static ByteArrayContent utf8(final String value) {
            return new ByteArrayContent(value.getBytes(StandardCharsets.UTF_8));
        }

        private byte[] data;
        private int offset;
        private int length;

        /** Empty, re-pointable view for {@link PooledByteArrayContent}; fields set via reset before use. */
        ByteArrayContent() {
        }

        /** Immutable, shareable constant over the whole array. */
        public ByteArrayContent(final byte[] data) {
            this(data, 0, data.length);
        }

        /** Immutable, shareable constant over {@code [offset, offset + length)}. */
        public ByteArrayContent(final byte[] data,
                                final int offset,
                                final int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public int write(final int sourceOffset, final ByteBuffer target) {
            final int n = Math.min(target.remaining(), length - sourceOffset);
            if (n <= 0) {
                return 0;               // no room in target (sourceOffset < length by precondition)
            }
            // Bulk relative copy: the source byte[] is only read (shareable); the target is
            // connection-owned, so advancing its position via a relative put is race-free.
            target.put(data, offset + sourceOffset, n);
            return n;
        }
    }

    /**
     * Server-owned, re-pointable body/header content pooled by {@link DefaultHttpResponse}. It wraps (not
     * extends) a {@link ByteArrayContent}, keeping the mutators here so a caller-constructed constant
     * can never be re-pointed or cleared by the framework. {@link #clear()} nulls the backing array so
     * it can be GC'd once the response no longer needs it.
     */
    private static final class PooledByteArrayContent implements ByteContent {
        private final ByteArrayContent delegate = new ByteArrayContent();

        private void reset(final byte[] data, final int offset, final int length) {
            delegate.data = data;
            delegate.offset = offset;
            delegate.length = length;
        }

        private void clear() {
            delegate.data = null;
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public int write(final int sourceOffset, final ByteBuffer target) {
            return delegate.write(sourceOffset, target);
        }
    }

    /**
     * Per-{@link Connection} gzip compressor: reads a fixed response's recorded body parts and produces
     * a single gzip stream (RFC 1952) in a growable buffer, then serves it back as a {@link ByteContent}
     * for the fixed body driver. Reused across requests via {@link #reset()} (the {@link Deflater} and
     * {@link CRC32} are reset, the buffer retained), so steady state is allocation-free. Connection-owned
     * and worker-thread-confined -- no locking. The native {@link Deflater} is released by {@link #end()}
     * at connection teardown.
     */
    private static final class GzipEncoder implements ByteContent {
        // Fixed 10-byte gzip member header: magic (0x1f 0x8b), CM=deflate(8), FLG=0, MTIME=0 (4B),
        // XFL=0, OS=255 (unknown). We write raw DEFLATE via Deflater(nowrap) and frame it ourselves.
        private static final byte[] GZIP_HEADER = {
            0x1f, (byte) 0x8b, 8, 0, 0, 0, 0, 0, 0, (byte) 0xff
        };

        private final Deflater deflater;
        private final CRC32 crc = new CRC32();
        private final byte[] staging;                            // pulls raw body bytes out of a ByteContent
        private final ByteBuffer stagingBuf;
        private final byte[] deflateScratch;                     // deflate output before it is appended
        private byte[] out;                                      // the assembled gzip stream (grows)
        private int length;                                      // bytes of 'out' in use
        private long isize;                                      // uncompressed byte count (for the trailer)

        // Streaming (chunked) mode: 'out' accumulates the stream and is handed to the chunk driver as
        // spans cut per addChunk or by size; 'cutStart' marks the start of the not-yet-recorded span.
        // In plain mode (streamGzip=false) input is copied through unchanged, so the same span machinery
        // frames a size-coalesced plain chunked body. Distinct from the batch add/finish path (fixed).
        private int cutStart;
        private boolean streamGzip;

        GzipEncoder(final int level, final int stagingBytes, final int scratchBytes,
                    final int initialOutBytes) {
            deflater = new Deflater(level, true);   // nowrap: raw DEFLATE, gzip framing done here
            staging = new byte[stagingBytes];
            stagingBuf = ByteBuffer.wrap(staging);
            deflateScratch = new byte[scratchBytes];
            out = new byte[initialOutBytes];
        }

        // Begin a fresh member: reset the deflater/crc, emit the 10-byte header.
        private void reset() {
            deflater.reset();
            crc.reset();
            length = 0;
            isize = 0;
            append(GZIP_HEADER, 0, GZIP_HEADER.length);
        }

        // Feed one recorded body part through the deflater (and CRC), appending compressed output.
        private void add(final ByteContent content) {
            final long len = content.length();
            int sourceOffset = 0;
            while (sourceOffset < len) {
                stagingBuf.clear();
                final int n = content.write(sourceOffset, stagingBuf);
                if (n == 0) {   // staging was just cleared, so a well-behaved ByteContent always fits >= 1
                    throw new IllegalStateException(
                            "ByteContent wrote no bytes with room available; length() over-promised");
                }
                sourceOffset += n;
                isize += n;
                crc.update(staging, 0, n);
                deflater.setInput(staging, 0, n);
                while (!deflater.needsInput()) {
                    drain();
                }
            }
        }

        // Finish the member: flush the deflater, append the 8-byte trailer (CRC32 + ISIZE, little-endian).
        // Returns the total gzip stream length (the Content-Length).
        private int finish() {
            deflater.finish();
            while (!deflater.finished()) {
                drain();
            }
            writeIntLe((int) crc.getValue());
            writeIntLe((int) (isize & 0xffffffffL));
            return length;
        }

        private void drain() {
            final int produced = deflater.deflate(deflateScratch, 0, deflateScratch.length);
            if (produced > 0) {
                append(deflateScratch, 0, produced);
            }
        }

        // Begin a fresh stream. For gzip, reset the deflater/CRC and emit the 10-byte header as the
        // leading bytes of the first cut; for plain, 'out' just accumulates raw bytes.
        private void beginStream(final boolean gzip) {
            streamGzip = gzip;
            length = 0;
            cutStart = 0;
            isize = 0;
            if (gzip) {
                deflater.reset();
                crc.reset();
                append(GZIP_HEADER, 0, GZIP_HEADER.length);
            }
        }

        // Feed one chunk's content. gzip: deflate it, SYNC_FLUSHing when 'flush' so the bytes emitted so
        // far form an independently inflatable unit. plain: copy it through.
        private void streamAdd(final ByteContent content, final boolean flush) {
            final long len = content.length();
            int sourceOffset = 0;
            while (sourceOffset < len) {
                stagingBuf.clear();
                final int n = content.write(sourceOffset, stagingBuf);
                if (n == 0) {
                    throw new IllegalStateException(
                            "ByteContent wrote no bytes with room available; length() over-promised");
                }
                sourceOffset += n;
                if (streamGzip) {
                    isize += n;
                    crc.update(staging, 0, n);
                    deflater.setInput(staging, 0, n);
                    while (!deflater.needsInput()) {
                        drain();
                    }
                } else {
                    append(staging, 0, n);
                }
            }
            if (flush && streamGzip) {
                syncFlush();
            }
        }

        // Complete the current deflate block to a byte boundary (a no-op in plain mode).
        private void streamFlush() {
            if (streamGzip) {
                syncFlush();
            }
        }

        // Commit: finish the gzip member (final block + CRC/ISIZE trailer). Plain: nothing to append.
        private void streamFinish() {
            if (!streamGzip) {
                return;
            }
            deflater.finish();
            while (!deflater.finished()) {
                drain();
            }
            writeIntLe((int) crc.getValue());
            writeIntLe((int) (isize & 0xffffffffL));
        }

        private void syncFlush() {
            int n;
            do {
                n = deflater.deflate(deflateScratch, 0, deflateScratch.length, Deflater.SYNC_FLUSH);
                if (n > 0) {
                    append(deflateScratch, 0, n);
                }
            } while (n == deflateScratch.length);
        }

        // Bytes written since the last recorded cut (compressed bytes in gzip mode, raw in plain).
        private int streamPending() {
            return length - cutStart;
        }

        private int streamCutStart() {
            return cutStart;
        }

        // Record the pending span as taken; the next span starts where this one ended.
        private void streamAdvanceCut() {
            cutStart = length;
        }

        // The backing buffer of the stream. A span recorded over it captures this array reference; the
        // encoder only appends (and only resets at the next beginStream), and ensureCapacity's copy keeps
        // earlier arrays intact, so a captured span stays valid until its entry is recycled.
        private byte[] streamBuffer() {
            return out;
        }

        private void append(final byte[] src, final int off, final int n) {
            ensureCapacity(length + n);
            System.arraycopy(src, off, out, length, n);
            length += n;
        }

        private void writeIntLe(final int v) {
            ensureCapacity(length + 4);
            out[length] = (byte) v;
            out[length + 1] = (byte) (v >>> 8);
            out[length + 2] = (byte) (v >>> 16);
            out[length + 3] = (byte) (v >>> 24);
            length += 4;
        }

        private void ensureCapacity(final int needed) {
            if (out.length < needed) {
                int cap = out.length;
                while (cap < needed) {
                    cap <<= 1;
                }
                out = Arrays.copyOf(out, cap);
            }
        }

        // Release native deflater resources at connection teardown.
        private void end() {
            deflater.end();
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public int write(final int sourceOffset, final ByteBuffer target) {
            final int n = Math.min(target.remaining(), length - sourceOffset);
            if (n <= 0) {
                return 0;               // no room in target (sourceOffset < length by precondition)
            }
            target.put(out, sourceOffset, n);
            return n;
        }
    }

    /**
     * Built-in {@link ByteContent} that UTF-8 encodes a {@link CharSequence} directly into the output
     * buffer -- no intermediate {@code byte[]}. Resumable at a char index and never splits a character
     * across a flush. {@link #length()} gives the exact Content-Length. Pooled by
     * {@link DefaultHttpResponse}; {@link #clear()} nulls the sequence.
     */
    private static final class Utf8Content implements ByteContent {
        private CharSequence cs;
        private int charCount;

        private void reset(final CharSequence cs) {
            this.cs = cs;
            this.charCount = cs.length();
        }

        private void clear() {
            this.cs = null;
        }

        /** Exact UTF-8 byte length of the whole sequence (surrogate-pair aware). */
        @Override
        public long length() {
            long bytes = 0;
            int i = 0;
            while (i < charCount) {
                final char c = cs.charAt(i);
                if (c < 0x80) {
                    bytes += 1;
                    i += 1;
                } else if (c < 0x800) {
                    bytes += 2;
                    i += 1;
                } else if (Character.isHighSurrogate(c) && i + 1 < charCount
                        && Character.isLowSurrogate(cs.charAt(i + 1))) {
                    bytes += 4;                 // valid surrogate pair -> 4-byte code point
                    i += 2;
                } else {
                    bytes += 3;                 // BMP char (incl. lone/unpaired surrogate)
                    i += 1;
                }
            }
            return bytes;
        }

        @Override
        public int write(final int sourceOffset, final ByteBuffer target) {
            final CharSequence s = cs;
            final int n = charCount;
            // Locate the char at byte offset 'sourceOffset'. Offsets always land on a char boundary
            // (a char is never split across a flush), so this walk is exact. O(chars-before-offset);
            // for resumed large sequences prefer a custom streaming content or chunked framing.
            // sourceOffset < length() (precondition) => i < n here.
            int i = 0;
            int consumed = 0;
            while (consumed < sourceOffset && i < n) {
                final char c = s.charAt(i);
                if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                    consumed += 4;              // surrogate pair -> 4-byte code point
                    i += 2;
                } else {
                    consumed += utf8Bytes(c);   // BMP char (incl. lone/unpaired surrogate)
                    i += 1;
                }
            }
            int written = 0;
            while (i < n) {
                final char c = s.charAt(i);
                final int codePoint;
                final int advance;
                if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                    codePoint = Character.toCodePoint(c, s.charAt(i + 1));
                    advance = 2;
                } else {
                    codePoint = c;              // BMP char, or a lone surrogate encoded as-is
                    advance = 1;
                }
                final int need = utf8Bytes(codePoint);
                if (need > target.remaining()) {
                    return written;             // never split a char: stop, resume on the next call
                }
                encode(target, codePoint, need);   // relative puts, advancing target.position()
                written += need;
                i += advance;
            }
            return written;
        }

        private static int utf8Bytes(final int cp) {
            if (cp < 0x80) {
                return 1;
            }
            if (cp < 0x800) {
                return 2;
            }
            if (cp < 0x10000) {
                return 3;
            }
            return 4;
        }

        private static void encode(final ByteBuffer target, final int cp, final int need) {
            switch (need) {
                case 1:
                    target.put((byte) cp);
                    break;
                case 2:
                    target.put((byte) (0xC0 | (cp >> 6)));
                    target.put((byte) (0x80 | (cp & 0x3F)));
                    break;
                case 3:
                    target.put((byte) (0xE0 | (cp >> 12)));
                    target.put((byte) (0x80 | ((cp >> 6) & 0x3F)));
                    target.put((byte) (0x80 | (cp & 0x3F)));
                    break;
                default:
                    target.put((byte) (0xF0 | (cp >> 18)));
                    target.put((byte) (0x80 | ((cp >> 12) & 0x3F)));
                    target.put((byte) (0x80 | ((cp >> 6) & 0x3F)));
                    target.put((byte) (0x80 | (cp & 0x3F)));
                    break;
            }
        }
    }

    /**
     * Built-in {@link ByteContent} that writes a {@link CharSequence} as ASCII -- one byte per char,
     * so the Content-Length is simply the character count ({@link #length()}). No scanning or
     * multi-byte encoding; the caller guarantees the characters are ASCII ({@code 0x00-0x7F}).
     * Resumable at a char index. Pooled by {@link DefaultHttpResponse}; {@link #clear()} nulls the sequence.
     */
    private static final class AsciiContent implements ByteContent {
        private CharSequence cs;
        private int count;

        private void reset(final CharSequence cs) {
            this.cs = cs;
            this.count = cs.length();
        }

        private void clear() {
            this.cs = null;
        }

        /** Byte length equals the character count (one byte per char). */
        @Override
        public long length() {
            return count;
        }

        @Override
        public int write(final int sourceOffset, final ByteBuffer target) {
            final int n = Math.min(target.remaining(), count - sourceOffset);
            if (n <= 0) {
                return 0;               // no room in target (sourceOffset < count by precondition)
            }
            for (int i = 0; i < n; i++) {                 // one byte per char; sourceOffset == char index
                target.put((byte) cs.charAt(sourceOffset + i));   // relative put, advances position
            }
            return n;
        }
    }

    /**
     * Zero-allocation response writer. One instance per {@link Connection} (thread-confined).
     *
     * <p>Emits pre-encoded header templates and encodes integers/dates in place into the bound
     * output buffer. Every writer returns {@code false} without partially writing when the
     * output buffer lacks room, so the caller can flush and retry. Framing header names are emitted
     * through the {@link HttpHeader} constants; {@link #contentType(ContentType)} adds the
     * {@code Content-Type: } prefix and the terminating CRLF around the {@link ContentType} value.
     */
    private static final class ResponseWriter {
        // Server-owned framing constants: immutable byte[] the writer bulk-puts directly, just like
        // the HttpHeader name+": " and ContentType value bytes. Shared across connections/threads
        // (each writes into its own connection-confined output buffer).
        private static final byte[] CRLF = ascii("\r\n");
        private static final byte[] HTTP11_SP = ascii("HTTP/1.1 ");
        // Framing header names (name + ": ") are taken from the HttpHeader constants; only the fixed
        // values the server owns are pre-encoded here.
        private static final byte[] V_CHUNKED = ascii("chunked\r\n");
        private static final byte[] V_KEEP_ALIVE = ascii("keep-alive\r\n");
        private static final byte[] V_CLOSE = ascii("close\r\n");
        private static final byte[] V_GZIP = ascii("gzip\r\n");
        private static final byte[] V_ACCEPT_ENCODING = ascii("Accept-Encoding\r\n");
        private static final byte[] LAST_CHUNK = ascii("0\r\n\r\n");

        private static final byte[][] STATUS_LINES = new byte[600][];

        static {
            putStatus(100, "Continue");
            putStatus(200, "OK");
            putStatus(201, "Created");
            putStatus(204, "No Content");
            putStatus(206, "Partial Content");
            putStatus(301, "Moved Permanently");
            putStatus(302, "Found");
            putStatus(304, "Not Modified");
            putStatus(400, "Bad Request");
            putStatus(401, "Unauthorized");
            putStatus(403, "Forbidden");
            putStatus(404, "Not Found");
            putStatus(405, "Method Not Allowed");
            putStatus(408, "Request Timeout");
            putStatus(413, "Payload Too Large");
            putStatus(431, "Request Header Fields Too Large");
            putStatus(500, "Internal Server Error");
            putStatus(501, "Not Implemented");
            putStatus(503, "Service Unavailable");
            putStatus(505, "HTTP Version Not Supported");
        }

        private static void putStatus(final int code, final String reason) {
            STATUS_LINES[code] = ascii("HTTP/1.1 " + code + " " + reason + "\r\n");
        }

        private static byte[] ascii(final String text) {
            return text.getBytes(StandardCharsets.US_ASCII);
        }

         // Per-instance (thread-confined) scratch -- no cross-thread sharing.
        private final byte[] digitsScratch = new byte[20];

        private ByteBuffer out;

        private void bind(final ByteBuffer outBuffer) {
            this.out = outBuffer;
        }

        private boolean statusLine(final int code) {
            final byte[] line = (code >= 0 && code < STATUS_LINES.length) ? STATUS_LINES[code] : null;
            if (line != null) {
                return put(line);
            }
            if (out.remaining() < HTTP11_SP.length + 8) {
                return false;
            }
            out.put(HTTP11_SP);
            putLong(code);
            out.put((byte) ' ');
            out.put(CRLF);
            return true;
        }

        private boolean contentType(final ContentType type) {
            final byte[] name = HttpHeader.CONTENT_TYPE.nameColonSpace();
            final byte[] value = type.value();
            if (out.remaining() < name.length + value.length + CRLF.length) {
                return false;
            }
            out.put(name);
            out.put(value);              // bare media type; the writer terminates the line
            out.put(CRLF);
            return true;
        }

        private boolean contentLength(final long length) {
            final byte[] name = HttpHeader.CONTENT_LENGTH.nameColonSpace();
            final int digitCount = digits(length);
            if (out.remaining() < name.length + digitCount + CRLF.length) {
                return false;
            }
            out.put(name);
            putLong(length);
            out.put(CRLF);
            return true;
        }

        private boolean connection(final boolean keepAlive) {
            final byte[] name = HttpHeader.CONNECTION.nameColonSpace();
            final byte[] value = keepAlive ? V_KEEP_ALIVE : V_CLOSE;
            if (out.remaining() < name.length + value.length) {
                return false;
            }
            out.put(name);
            out.put(value);
            return true;
        }

        private boolean transferEncodingChunked() {
            final byte[] name = HttpHeader.TRANSFER_ENCODING.nameColonSpace();
            if (out.remaining() < name.length + V_CHUNKED.length) {
                return false;
            }
            out.put(name);
            out.put(V_CHUNKED);
            return true;
        }

        private boolean contentEncodingGzip() {
            final byte[] name = HttpHeader.CONTENT_ENCODING.nameColonSpace();
            if (out.remaining() < name.length + V_GZIP.length) {
                return false;
            }
            out.put(name);
            out.put(V_GZIP);
            return true;
        }

        private boolean varyAcceptEncoding() {
            final byte[] name = HttpHeader.VARY.nameColonSpace();
            if (out.remaining() < name.length + V_ACCEPT_ENCODING.length) {
                return false;
            }
            out.put(name);
            out.put(V_ACCEPT_ENCODING);
            return true;
        }

        private boolean endHeaders() {
            return put(CRLF);
        }

        private boolean lastChunk() {
            return put(LAST_CHUNK);
        }

        /**
         * Emit a custom response header {@code "Name: " + value + CRLF}, pulling the bare value bytes
         * from {@code value} (relative writes, advancing the buffer position). Returns {@code false} if
         * the output buffer runs out of room before the whole line is written -- headers are not
         * resumable across flushes, so the caller must treat that as an overflow.
         */
        private boolean header(final byte[] nameColonSpace, final ByteContent value) {
            if (!put(nameColonSpace)) {
                return false;
            }
            final long len = value.length();
            int sourceOffset = 0;
            while (sourceOffset < len) {
                final int n = value.write(sourceOffset, out);
                if (n == 0) {
                    return false;                // no room for even one more unit -> overflow
                }
                sourceOffset += n;
            }
            return put(CRLF);
        }

        private boolean put(final byte[] bytes) {
            if (out.remaining() < bytes.length) {
                return false;
            }
            out.put(bytes);
            return true;
        }

        private void putLong(final long value) {
            if (value == 0) {
                out.put((byte) '0');
                return;
            }
            long remaining = value;
            if (remaining < 0) {
                out.put((byte) '-');
                remaining = -remaining;
            }
            int pos = 20;
            while (remaining > 0) {
                digitsScratch[--pos] = (byte) ('0' + (int) (remaining % 10));
                remaining /= 10;
            }
            out.put(digitsScratch, pos, 20 - pos);
        }

        private static int digits(final long value) {
            if (value < 0) {
                return 1 + digits(-value);
            }
            int count = 1;
            long remaining = value;
            while (remaining >= 10) {
                remaining /= 10;
                count++;
            }
            return count;
        }
    }

    /**
     * Worker = thread + Selector + event loop. Owns its connections for their whole life, so no
     * connection state is shared across threads. Newly accepted channels are handed over through a
     * concurrent queue and registered on the worker's own thread.
     */
    private static final class Worker implements Runnable {
        private static final long SELECT_TIMEOUT_MS = 1000; // shutdown join budget

        /**
         * Idle backoff: spin, then yield, then park for a period that doubles while nothing
         * happens. Spinning and yielding keep work that arrives <em>now</em> cheap; the park
         * ceiling is the most latency an idle worker can add.
         * <p>
         * The park is clamped to the wheel's next deadline ({@link #idle()}), so the wait is
         * derived from what is actually due rather than from a fixed tick. A fixed wait fires
         * every deadline inside its window in one sweep when it lands -- a burst -- where this
         * wakes once per deadline and spreads the same work.
         */
        private static final int IDLE_SPINS = 64;
        private static final int IDLE_YIELDS = 32;
        private static final long PARK_MIN_NANOS = 100_000L;        // 100us
        private static final long PARK_MAX_NANOS = 1_000_000L;      // 1ms

        /**
         * The ceiling while this worker is <b>quiescent</b>: no live connection and nothing armed
         * in the wheel. Only {@link #assign} and {@link #execute} can produce work in that state,
         * and both unpark, so waking a thousand times a second buys nothing -- an idle worker is
         * the common state for a server sized by {@code availableProcessors()}.
         * <p>
         * Bounded rather than an indefinite {@link LockSupport#park()}: if a wake source is ever
         * missed the worker recovers within a second instead of hanging.
         */
        private static final long QUIESCENT_PARK_NANOS = 1_000_000_000L;   // 1s

        private int idleRounds;
        private long parkNanos;

        private final int id;
        private final ConnectionInitializer initializer;
        private final AcceptHandler acceptHandler;
        private final HttpServerConfig config;
        private final Selector selector;
        private final Set<SelectionKey> selectedKeys;
        private final Thread thread;
        private final TimeoutWheel wheel = new TimeoutWheel();
        private final ConcurrentLinkedQueue<SocketChannel> pending = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        private int liveConns; // worker-thread confined: admission accounting
        private volatile boolean running = true;

        Worker(final int id, final ConnectionInitializer initializer,
                final AcceptHandler acceptHandler,
                final HttpServerConfig config,
                final ThreadFactory threadFactory) {
            this.id = id;
            this.acceptHandler = acceptHandler;
            this.initializer = initializer;
            this.config = config;
            try {
                this.selector = Selector.open();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            this.selectedKeys = selector.selectedKeys();
            this.thread = threadFactory.newThread(this);
        }

        private void start() {
            thread.start();
        }

        private TimeoutWheel wheel() {
            return wheel;
        }

        /**
         * True when the caller is running on this worker's own thread -- the guard the public
         * response/streaming/scheduling API uses to enforce that all connection I/O stays on the one
         * owning thread (rather than silently marshalling).
         */
        private boolean inEventLoop() {
            return Thread.currentThread() == thread;
        }

        /**
         * Arm a one-shot task in this worker's timing wheel, firing on the worker thread after
         * {@code delayMs}. Worker-thread-confined (touches the wheel). Returns a handle to cancel it.
         */
        private Cancelable schedule(final Runnable task, final long delayMs) {
            final ScheduledTask node = new ScheduledTask(this, task);
            wheel.schedule(node, nowMs() + Math.max(0L, delayMs));
            return node;
        }

        /**
         * Decrement the live-connection count. Called once per connection from
         * {@link Connection#close()} (which is idempotent), on the worker thread.
         */
        private void connectionClosed() {
            liveConns--;
        }

        private void assign(final SocketChannel channel) {
            pending.add(channel);
            LockSupport.unpark(thread);
        }

        /**
         * Run a task on this worker's own thread. Safe to call from any thread; used to resume a
         * paused connection's input without touching selector/connection state off-thread.
         */
        private void execute(final Runnable task) {
            tasks.add(task);
            LockSupport.unpark(thread);
        }

        /** @return how many tasks ran, so the loop can tell a busy pass from an idle one */
        private int drainTasks() {
            int ran = 0;
            Runnable task;
            while ((task = tasks.poll()) != null) {
                ran++;
                try {
                    task.run();
                } catch (final Throwable ignore) {
                    // best-effort: a resume task must not kill the worker loop
                }
            }
            return ran;
        }

        /**
         * Stop the worker: break the select loop (whose {@code finally} releases every live
         * connection via {@link Connection#close()}, firing {@code onClose}), join the thread,
         * then close the selector. Best-effort and idempotent.
         */
        private void stop() {
            running = false;
            LockSupport.unpark(thread);
            try {
                thread.join(SELECT_TIMEOUT_MS * 2);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                selector.close();
            } catch (final IOException ignore) {
                // best-effort
            }
        }

        /** @return how many channels were taken off the queue, registered or shed */
        private int drainPending() {
            int taken = 0;
            SocketChannel channel;
            while ((channel = pending.poll()) != null) {
                taken++;
                if (liveConns >= config.maxConnectionsPerWorker()) {
                    // Shed load before allocating any per-connection buffers.
                    try {
                        channel.close();
                    } catch (final IOException ignore) {
                        // best-effort
                    }
                    continue;
                }
                try {
                    final Connection connection = new Connection(channel, initializer, this, config);
                    final SelectionKey key = channel.register(selector, SelectionKey.OP_READ, connection);
                    connection.attach(key);
                    liveConns++;
                    connection.enterTimeoutState(Connection.TS_HEADERS); // start the header timeout at accept
                    connection.notifyOpen();                      // connection active: onOpen
                } catch (final IOException e) {
                    try {
                        channel.close();
                    } catch (final IOException ignore) {
                        // best-effort
                    }
                }
            }
            return taken;
        }

        /**
         * Nothing happened this pass: spin, then yield, then park no longer than the wheel's next
         * deadline. This thread is the only one that can fire a timer, so backing off cannot make
         * one late.
         * <p>
         * The ceiling on the doubling is {@link #PARK_MAX_NANOS}, or {@link #QUIESCENT_PARK_NANOS}
         * once there is neither a live connection nor an armed timer -- see that constant.
         */
        private void idle() {
            idleRounds++;
            if (idleRounds <= IDLE_SPINS) {
                Thread.onSpinWait();
                return;
            }
            if (idleRounds <= IDLE_SPINS + IDLE_YIELDS) {
                Thread.yield();
                return;
            }
            // One scan answers both questions: how long until the wheel can fire, and (when that
            // is never) whether the wheel is empty at all.
            final long nextDeadlineNanos = wheel.nanosToNextDeadline(nowMs());
            final long ceilingNanos = nextDeadlineNanos == Long.MAX_VALUE && liveConns == 0
                    ? QUIESCENT_PARK_NANOS
                    : PARK_MAX_NANOS;
            parkNanos = parkNanos == 0L ? PARK_MIN_NANOS : Math.min(ceilingNanos, parkNanos * 2L);
            LockSupport.parkNanos(Math.min(parkNanos, nextDeadlineNanos));
        }

        @Override
        public void run() {
            try {
                while (running) {
                    int work = 0;
                    // Never blocking. The wait belongs to idle(), which derives it from the next
                    // deadline; a blocking select would hold a fixed tick and then fire every
                    // timer inside it at once.
                    selector.selectNow();
                    if (!running) {
                        break;
                    }
                    work += drainPending();
                    work += drainTasks();

                    // isEmpty() first: the key set is a field, so an idle pass allocates no
                    // iterator, and an idle pass is the common one.
                    if (!selectedKeys.isEmpty()) {
                        final Iterator<SelectionKey> iterator = selectedKeys.iterator();
                        while (iterator.hasNext()) {
                            final SelectionKey key = iterator.next();
                            iterator.remove();
                            if (!key.isValid()) {
                                continue;
                            }
                            work++;
                            final Connection connection = (Connection) key.attachment();
                            try {
                                if (key.isReadable()) {
                                    connection.onReadable();
                                }
                                if (key.isValid() && key.isWritable()) {
                                    connection.onWritable();
                                }
                            } catch (final Throwable error) {
                                connection.handleException(error);
                            }
                        }
                    }
                    work += wheel.advance(nowMs());

                    if (work > 0) {
                        idleRounds = 0;
                        parkNanos = 0L;
                    } else {
                        idle();
                    }
                }
            } catch (final ClosedSelectorException e) {
                // stop() closed it under us: fall through to teardown.
            } catch (final IOException e) {
                if (running) {
                    acceptHandler.onError(null, e);
                }
            } finally {
                closeAllConnections();  // guarantee onClose for every live connection at shutdown
            }
        }

        /**
         * Release every connection still owned by this worker, on the worker's own thread (so
         * connection state stays thread-confined). Runs once as the run loop exits. {@link
         * Connection#close()} is idempotent and fires {@code onClose}; channels accepted but
         * never registered (still in {@code pending}) never fired {@code onOpen}, so they are just
         * closed.
         */
        private void closeAllConnections() {
            for (final SelectionKey key : selector.keys()) {
                final Object attachment = key.attachment();
                if (attachment instanceof Connection) {
                    ((Connection) attachment).close();
                }
            }
            SocketChannel channel;
            while ((channel = pending.poll()) != null) {
                try {
                    channel.close();
                } catch (final IOException ignore) {
                    // best-effort
                }
            }
        }
    }

    /**
     * A node in a {@link TimeoutWheel} bucket: an intrusive doubly-linked list element carrying its
     * absolute {@code deadlineMs}. Both a {@link Connection}'s timeout-state deadline and a one-shot
     * {@link ScheduledTask} are timer nodes, so the same wheel schedules connection timeouts and
     * arbitrary worker-thread tasks. {@link #onExpire(long)} runs on the owning worker thread when
     * the deadline elapses.
     */
    private abstract static class TimerNode {
        private TimerNode wheelPrev;
        private TimerNode wheelNext;
        private int wheelBucket = -1;
        private long deadlineMs;

        abstract void onExpire(long now);
    }

    /**
     * Handle for a task armed via {@link Connection#schedule(Response, Runnable, long)}: {@link #cancel()}
     * disarms it if it has not yet fired. Both scheduling and cancelling are worker-thread-confined
     * (they touch the wheel); cancel from another thread throws.
     *
     * <p>Giving a deferred response a deadline is shown under <em>6.3 Bounding the wait</em> in
     * the {@link HttpServer} manual.
     */
    public interface Cancelable {
        void cancel();
    }

    /**
     * A one-shot task armed in the worker's {@link TimeoutWheel}. Fires {@link Runnable#run()} on
     * the worker thread at its deadline. A self-rescheduling task (re-arming itself from within
     * {@code run()}) provides periodic execution without a dedicated primitive.
     */
    private static final class ScheduledTask extends TimerNode implements Cancelable {
        private final Worker worker;
        private Runnable task;
        private boolean done; // fired or cancelled

        ScheduledTask(final Worker worker, final Runnable task) {
            this.worker = worker;
            this.task = task;
        }

        @Override
        void onExpire(final long now) {
            if (done) {
                return;
            }
            done = true;
            final Runnable r = task;
            task = null;
            try {
                r.run();
            } catch (final Throwable ignore) {
                // best-effort: a scheduled task must not kill the worker loop
            }
        }

        @Override
        public void cancel() {
            if (!worker.inEventLoop()) {
                throw new IllegalStateException(
                        "ScheduledHandle.cancel() must run on the owning worker thread; use Connection.execute()");
            }
            if (done) {
                return;
            }
            done = true;
            task = null;
            worker.wheel().cancel(this);
        }
    }

    /**
     * Per-worker hashed timing wheel: arms every {@link TimerNode} (a connection timeout-state deadline or a
     * {@link ScheduledTask}) at O(1) schedule / cancel and O(bucket) expiry, replacing an O(N) full
     * scan. Buckets are intrusive doubly-linked lists of nodes (no per-entry allocation, no stale
     * entries). The span ({@link #SIZE} * {@link #TICK_MS}) exceeds the largest default timeout, so
     * a single rotation suffices; a deadline beyond the span is clamped and re-armed at expiry.
     * Confined to its owning worker thread.
     */
    private static final class TimeoutWheel {
        private static final long TICK_MS = 250L;
        private static final int SIZE = 256; // span = SIZE * TICK_MS = 64s, above the largest default timeout

        private final TimerNode[] buckets = new TimerNode[SIZE];
        private int currentTick;
        private long lastAdvanceMs = nowMs();

        /**
         * Arm (or re-arm) a node to fire at {@code deadlineMs}. Deadlines beyond the wheel span are
         * clamped to the last slot and re-armed by {@link #advance(long)} when they fire early.
         */
        private void schedule(final TimerNode node, final long deadlineMs) {
            cancel(node);
            node.deadlineMs = deadlineMs;
            final long delay = Math.max(0L, deadlineMs - nowMs());
            final int ticks = (int) Math.min(delay / TICK_MS, SIZE - 1L);
            final int slot = (currentTick + ticks + 1) % SIZE;
            node.wheelBucket = slot;
            node.wheelPrev = null;
            node.wheelNext = buckets[slot];
            if (buckets[slot] != null) {
                buckets[slot].wheelPrev = node;
            }
            buckets[slot] = node;
        }

        private void cancel(final TimerNode node) {
            final int bucket = node.wheelBucket;
            if (bucket < 0) {
                return;
            }
            if (node.wheelPrev != null) {
                node.wheelPrev.wheelNext = node.wheelNext;
            } else if (buckets[bucket] == node) {
                buckets[bucket] = node.wheelNext;
            }
            if (node.wheelNext != null) {
                node.wheelNext.wheelPrev = node.wheelPrev;
            }
            node.wheelPrev = null;
            node.wheelNext = null;
            node.wheelBucket = -1;
        }

        /**
         * Advance the wheel to {@code now}, firing every bucket whose tick has elapsed. Falling
         * behind by more than a full rotation collapses to a single sweep (every bucket is expired).
         * A node whose real deadline has not yet arrived (it was clamped into the span) is re-armed
         * rather than fired.
         */
        private int advance(final long now) {
            int steps = (int) ((now - lastAdvanceMs) / TICK_MS);
            if (steps <= 0) {
                return 0;
            }
            int fired = 0;
            if (steps >= SIZE) {
                steps = SIZE;
                lastAdvanceMs = now;
            } else {
                lastAdvanceMs += (long) steps * TICK_MS;
            }
            for (int i = 0; i < steps; i++) {
                currentTick = (currentTick + 1) % SIZE;
                TimerNode node = buckets[currentTick];
                buckets[currentTick] = null; // detach the whole list; onExpire may re-schedule
                while (node != null) {
                    final TimerNode next = node.wheelNext;
                    node.wheelPrev = null;
                    node.wheelNext = null;
                    node.wheelBucket = -1;
                    if (now < node.deadlineMs) {
                        schedule(node, node.deadlineMs); // clamped beyond span: re-arm for the remainder
                    } else {
                        fired++;
                        node.onExpire(now);
                    }
                    node = next;
                }
            }
            return fired;
        }

        /**
         * How long until this wheel could fire anything, so the loop's park can be clamped to it
         * rather than to a fixed tick. Scans forward for the first armed bucket -- at most
         * {@link #SIZE} array reads, and only on an idle pass.
         *
         * @return nanos until the next armed bucket is due, or {@link Long#MAX_VALUE} when the
         *         wheel holds nothing at all
         */
        private long nanosToNextDeadline(final long now) {
            for (int i = 1; i <= SIZE; i++) {
                if (buckets[(currentTick + i) % SIZE] != null) {
                    final long dueMs = lastAdvanceMs + (long) i * TICK_MS;
                    return Math.max(0L, (dueMs - now) * 1_000_000L);
                }
            }
            return Long.MAX_VALUE;
        }
    }

    /**
     * Zero-allocation incremental HTTP/1.1 parser (nginx-style state machine).
     *
     * <p>Consumes bytes from a read-mode {@link ByteBuffer}, exposing parsed request parts to a
     * {@link ConnectionHandler} as {@code (buffer, offset, length)} slices. Resumable: partial input
     * returns {@link #RESULT_NEED_MORE} and parsing continues from the same state on the next
     * call. The {@link Connection} argument is forwarded to the handler callbacks; the parser only
     * dereferences it to honor request-body backpressure ({@link Connection#inputPaused()}), and
     * tolerates a {@code null} connection (as in unit tests) by skipping that check.
     */
    private static final class HttpParser {
        private static final int RESULT_NEED_MORE = 0;
        private static final int RESULT_MESSAGE_COMPLETE = 1;
        private static final int RESULT_ERROR = 2;

        private static final int ST_METHOD = 0;
        private static final int ST_PATH = 1;
        private static final int ST_VERSION = 2;
        private static final int ST_HDR_NAME = 3;
        private static final int ST_HDR_VAL = 4;
        private static final int ST_HDR_END = 5;
        private static final int ST_BODY = 6;
        private static final int ST_CHUNK_SIZE = 7;
        private static final int ST_CHUNK_DATA = 8;
        private static final int ST_CHUNK_CRLF = 9;
        private static final int ST_TRAILERS = 10;   // after last chunk: skip trailer lines to the blank line
        private static final int ST_DONE = 11;

        private int state = ST_METHOD;
        private long bodyRemaining;
        private long chunkRemaining;

        // Optional decoded-body cap (0 => unlimited): total request body bytes across a fixed body or
        // the sum of chunk data. Exceeding it fails the request with 413 (see errorStatus).
        private final long maxRequestBodyBytes;
        private long bodyReceived;
        // Status the connection should surface when parse() returns RESULT_ERROR (400 by default,
        // 413 when the body cap is exceeded). Reset per request.
        private int errorStatus = 400;

        private HttpParser(final long maxRequestBodyBytes) {
            this.maxRequestBodyBytes = maxRequestBodyBytes;
        }

        private int state() {
            return state;
        }

        private int errorStatus() {
            return errorStatus;
        }

        private void reset() {
            state = ST_METHOD;
            bodyRemaining = 0;
            chunkRemaining = 0;
            bodyReceived = 0;
            errorStatus = 400;
        }

        private int parse(final ByteBuffer buffer, final HttpRequest request,
                  final Connection connection, final ConnectionHandler handler) {
            request.buffer = buffer;
            switch (state) {
                case ST_METHOD:
                    return parseMethod(buffer, request, connection, handler);
                case ST_PATH:
                    return parsePath(buffer, request, connection, handler);
                case ST_VERSION:
                    return parseVersion(buffer, request, connection, handler);
                case ST_HDR_NAME:
                case ST_HDR_VAL:
                case ST_HDR_END:
                    return parseHeaders(buffer, request, connection, handler);
                case ST_BODY:
                    return parseBody(buffer, request, connection, handler);
                case ST_CHUNK_SIZE:
                case ST_CHUNK_DATA:
                case ST_CHUNK_CRLF:
                case ST_TRAILERS:
                    return parseChunked(buffer, request, connection, handler);
                default:
                    return RESULT_ERROR;
            }
        }

        private int parseMethod(final ByteBuffer buffer, final HttpRequest request,
                                final Connection connection, final ConnectionHandler handler) {
            final int start = buffer.position();
            while (buffer.hasRemaining()) {
                final byte current = buffer.get();
                if (current == ' ') {
                    request.methodOffset = start;
                    request.methodLength = buffer.position() - 1 - start;
                    state = ST_PATH;
                    return parsePath(buffer, request, connection, handler);
                }
                if (current == '\r' || current == '\n') {
                    return RESULT_ERROR;
                }
            }
            buffer.position(start);
            return RESULT_NEED_MORE;
        }

        private int parsePath(final ByteBuffer buffer, final HttpRequest request,
                              final Connection connection, final ConnectionHandler handler) {
            final int start = buffer.position();
            while (buffer.hasRemaining()) {
                final byte current = buffer.get();
                if (current == ' ') {
                    setPath(request, buffer, start, buffer.position() - 1);
                    state = ST_VERSION;
                    return parseVersion(buffer, request, connection, handler);
                }
                if (current == '\r' || current == '\n') {
                    return RESULT_ERROR;
                }
            }
            buffer.position(start);
            return RESULT_NEED_MORE;
        }

        // Record the request path from the target [start, end). For absolute-form targets
        // (scheme://authority/path, used by proxies) the scheme+authority prefix is skipped so
        // pathString()/pathEquals() see the origin path. Origin-form ("/path"), authority-form and
        // asterisk-form ("*") are left as-is.
        private static void setPath(final HttpRequest request, final ByteBuffer buffer,
                                    final int start, final int end) {
            int pathStart = start;
            if (end > start && buffer.get(start) != '/' && buffer.get(start) != '*') {
                final int sep = indexOfSchemeSep(buffer, start, end);
                if (sep >= 0) {
                    int i = sep + 3;                       // past "://"
                    while (i < end && buffer.get(i) != '/') {
                        i++;                                // skip the authority
                    }
                    pathStart = i;                          // first '/' of the path, or end if none
                }
            }
            request.pathOffset = pathStart;
            request.pathLength = end - pathStart;
        }

        private static int indexOfSchemeSep(final ByteBuffer buffer, final int from, final int to) {
            for (int i = from; i + 2 < to; i++) {
                if (buffer.get(i) == ':' && buffer.get(i + 1) == '/' && buffer.get(i + 2) == '/') {
                    return i;
                }
            }
            return -1;
        }

        private int parseVersion(final ByteBuffer buffer, final HttpRequest request,
                                 final Connection connection, final ConnectionHandler handler) {
            final int start = buffer.position();
            final int end = findCrLf(buffer, start);
            if (end < 0) {
                buffer.position(start);
                return RESULT_NEED_MORE;
            }
            if (end - start >= 8 && buffer.get(start + 5) == '1') {
                request.versionMinor = (buffer.get(start + 7) == '1') ? 1 : 0;
                request.keepAlive = (request.versionMinor == 1);
            } else {
                // Unrecognized/malformed version token: default to closing the connection rather than
                // leaving the optimistic keep-alive default. An explicit "Connection: keep-alive"
                // header (parsed next) can still re-enable it.
                request.keepAlive = false;
            }
            buffer.position(end + 2);
            handler.onRequestLine(connection, request);
            state = ST_HDR_NAME;
            return parseHeaders(buffer, request, connection, handler);
        }

        private int parseHeaders(final ByteBuffer buffer, final HttpRequest request,
                                 final Connection connection, final ConnectionHandler handler) {
            while (true) {
                final int lineStart = buffer.position();
                final int lineEnd = findCrLf(buffer, lineStart);
                if (lineEnd < 0) {
                    buffer.position(lineStart);
                    return RESULT_NEED_MORE;
                }
                if (lineEnd == lineStart) {           // blank line -> headers done
                    buffer.position(lineEnd + 2);
                    handler.onHeadersComplete(connection, request);
                    return startBody(buffer, request, connection, handler);
                }
                int colon = -1;
                for (int i = lineStart; i < lineEnd; i++) {
                    if (buffer.get(i) == ':') {
                        colon = i;
                        break;
                    }
                }
                if (colon < 0) {
                    buffer.position(lineEnd + 2);
                    continue;
                }
                final int nameOffset = lineStart;
                final int nameLength = colon - lineStart;
                int valueOffset = colon + 1;
                int valueLength = lineEnd - valueOffset;
                while (valueLength > 0 && (buffer.get(valueOffset) == ' ' || buffer.get(valueOffset) == '\t')) {
                    valueOffset++;
                    valueLength--;
                }
                interpret(buffer, request, nameOffset, nameLength, valueOffset, valueLength);
                handler.onHeader(connection, request, buffer, nameOffset, nameLength, valueOffset, valueLength);
                buffer.position(lineEnd + 2);
            }
        }

        private void interpret(final ByteBuffer buffer, final HttpRequest request, final int nameOffset,
                               final int nameLength, final int valueOffset, final int valueLength) {
            if (nameEq(buffer, nameOffset, nameLength, "content-length")) {
                request.contentLength = parseLong(buffer, valueOffset, valueLength);
            } else if (nameEq(buffer, nameOffset, nameLength, "transfer-encoding")) {
                if (hasToken(buffer, valueOffset, valueLength, "chunked")) {
                    request.chunked = true;
                }
            } else if (nameEq(buffer, nameOffset, nameLength, "connection")) {
                if (hasToken(buffer, valueOffset, valueLength, "close")) {
                    request.keepAlive = false;
                } else if (hasToken(buffer, valueOffset, valueLength, "keep-alive")) {
                    request.keepAlive = true;
                }
            } else if (nameEq(buffer, nameOffset, nameLength, "expect")) {
                if (hasToken(buffer, valueOffset, valueLength, "100-continue")) {
                    request.expectContinue = true;
                }
            } else if (nameEq(buffer, nameOffset, nameLength, "accept-encoding")) {
                if (hasToken(buffer, valueOffset, valueLength, "gzip")) {
                    request.gzipAccepted = true;
                }
            }
        }

        private int startBody(final ByteBuffer buffer, final HttpRequest request,
                              final Connection connection, final ConnectionHandler handler) {
            // Both Transfer-Encoding: chunked and Content-Length present: a request-smuggling vector
            // (RFC 7230 section 3.3.3). contentLength stays -1 when the header is absent, so this is precise.
            if (request.chunked && request.contentLength >= 0) {
                errorStatus = 400;
                return RESULT_ERROR;
            }
            if (request.chunked) {
                state = ST_CHUNK_SIZE;
                return parseChunked(buffer, request, connection, handler);
            }
            if (request.contentLength > 0) {
                // Reject an over-cap fixed body up front (before streaming any of it).
                if (maxRequestBodyBytes > 0 && request.contentLength > maxRequestBodyBytes) {
                    errorStatus = 413;
                    return RESULT_ERROR;
                }
                bodyRemaining = request.contentLength;
                state = ST_BODY;
                return parseBody(buffer, request, connection, handler);
            }
            state = ST_DONE;
            return RESULT_MESSAGE_COMPLETE;
        }

        private int parseBody(final ByteBuffer buffer, final HttpRequest request,
                              final Connection connection, final ConnectionHandler handler) {
            if (bodyRemaining == 0) {
                state = ST_DONE;
                return RESULT_MESSAGE_COMPLETE;
            }
            if (!buffer.hasRemaining()) {
                return RESULT_NEED_MORE;
            }
            // Handler rejected in a header/life-cycle callback: stop feeding the body; the response
            // drains and the connection closes.
            if (connection != null && connection.isClosing()) {
                return RESULT_NEED_MORE;
            }
            final int take = (int) Math.min(buffer.remaining(), bodyRemaining);
            final int offset = buffer.position();
            handler.onBodyChunk(connection, request, buffer, offset, take);
            buffer.position(offset + take);
            bodyRemaining -= take;
            if (bodyRemaining == 0) {
                state = ST_DONE;
                return RESULT_MESSAGE_COMPLETE;
            }
            return RESULT_NEED_MORE;
        }

        private int parseChunked(final ByteBuffer buffer, final HttpRequest request,
                                 final Connection connection, final ConnectionHandler handler) {
            while (true) {
                switch (state) {
                    case ST_CHUNK_SIZE: {
                        final int start = buffer.position();
                        final int end = findCrLf(buffer, start);
                        if (end < 0) {
                            buffer.position(start);
                            return RESULT_NEED_MORE;
                        }
                        final long size = parseHex(buffer, start, end - start);
                        buffer.position(end + 2);
                        // parseHex returns < 0 when no hex digit was present (or the size overflowed):
                        // a malformed chunk-size line. Do NOT treat it as the terminating 0-chunk (that
                        // would truncate the body and mis-frame trailing bytes as a new request).
                        if (size < 0) {
                            errorStatus = 400;
                            return RESULT_ERROR;
                        }
                        if (size == 0) {                 // last chunk -> consume trailers + final CRLF
                            state = ST_TRAILERS;
                            break;
                        }
                        // Reject a single chunk that alone would push the decoded body over the cap.
                        if (maxRequestBodyBytes > 0 && size > maxRequestBodyBytes - bodyReceived) {
                            errorStatus = 413;
                            return RESULT_ERROR;
                        }
                        chunkRemaining = size;
                        state = ST_CHUNK_DATA;
                        break;
                    }
                    case ST_CHUNK_DATA: {
                        if (!buffer.hasRemaining()) {
                            return RESULT_NEED_MORE;
                        }
                        // Handler rejected in a header/life-cycle callback: stop feeding the body.
                        if (connection != null && connection.isClosing()) {
                            return RESULT_NEED_MORE;
                        }
                        final int take = (int) Math.min(buffer.remaining(), chunkRemaining);
                        final int offset = buffer.position();
                        handler.onBodyChunk(connection, request, buffer, offset, take);
                        buffer.position(offset + take);
                        chunkRemaining -= take;
                        bodyReceived += take;
                        if (chunkRemaining == 0) {
                            state = ST_CHUNK_CRLF;
                        } else {
                            return RESULT_NEED_MORE;
                        }
                        // Honor backpressure requested during the callback (stop before the next
                        // buffered chunk); resume re-enters at ST_CHUNK_CRLF.
                        if (connection != null && connection.inputPaused()) {
                            return RESULT_NEED_MORE;
                        }
                        break;
                    }
                    case ST_CHUNK_CRLF: {
                        if (buffer.remaining() < 2) {
                            return RESULT_NEED_MORE;
                        }
                        buffer.get();
                        buffer.get();
                        state = ST_CHUNK_SIZE;
                        break;
                    }
                    case ST_TRAILERS: {
                        // Skip any trailer header lines; the blank line terminates the message.
                        final int lineStart = buffer.position();
                        final int lineEnd = findCrLf(buffer, lineStart);
                        if (lineEnd < 0) {
                            buffer.position(lineStart);
                            return RESULT_NEED_MORE;
                        }
                        buffer.position(lineEnd + 2);
                        if (lineEnd == lineStart) {      // blank line -> done
                            state = ST_DONE;
                            return RESULT_MESSAGE_COMPLETE;
                        }
                        break;                            // ignore this trailer line, keep scanning
                    }
                    default:
                        return RESULT_ERROR;
                }
            }
        }

        private static int findCrLf(final ByteBuffer buffer, final int from) {
            final int limit = buffer.limit();
            for (int i = from; i + 1 < limit; i++) {
                if (buffer.get(i) == '\r' && buffer.get(i + 1) == '\n') {
                    return i;
                }
            }
            return -1;
        }

        private static boolean nameEq(final ByteBuffer buffer, final int offset, final int length, final String lower) {
            if (length != lower.length()) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (AsciiText.toLower(buffer.get(offset + i) & 0xFF) != lower.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean hasToken(final ByteBuffer buffer, final int offset, final int length,
                                        final String tokenLower) {
            final int tokenLength = tokenLower.length();
            outer:
            for (int i = offset; i + tokenLength <= offset + length; i++) {
                for (int j = 0; j < tokenLength; j++) {
                    if (AsciiText.toLower(buffer.get(i + j) & 0xFF) != tokenLower.charAt(j)) {
                        continue outer;
                    }
                }
                return true;
            }
            return false;
        }

        private static long parseLong(final ByteBuffer buffer, final int offset, final int length) {
            long value = 0;
            for (int i = 0; i < length; i++) {
                final int current = buffer.get(offset + i);
                if (current < '0' || current > '9') {
                    break;
                }
                value = value * 10 + (current - '0');
            }
            return value;
        }

        /**
         * Parse a chunk-size hex field. Returns the value, or {@code -1} when no hex digit was present
         * (a malformed chunk-size line) -- the caller must treat that as a protocol error, not as the
         * terminating zero chunk. Stops at the first non-hex byte (so chunk extensions like
         * {@code ";name=value"} are tolerated). An overflowing (>15-digit) size also surfaces as a
         * negative value and is rejected by the caller.
         */
        private static long parseHex(final ByteBuffer buffer, final int offset, final int length) {
            long value = 0;
            int digits = 0;
            for (int i = 0; i < length; i++) {
                final int current = buffer.get(offset + i) & 0xFF;
                final int digit;
                if (current >= '0' && current <= '9') {
                    digit = current - '0';
                } else if (current >= 'a' && current <= 'f') {
                    digit = current - 'a' + 10;
                } else if (current >= 'A' && current <= 'F') {
                    digit = current - 'A' + 10;
                } else {
                    break;
                }
                value = (value << 4) | digit;
                digits++;
            }
            if (digits == 0) {
                return -1;
            }
            return value < 0 ? -1 : value;   // overflow (>15 hex digits) -> malformed
        }
    }

    /**
     * Response body driver: resumable state machine for bodies larger than the output buffer.
     * Handles fixed-length and chunked framing, cooperating with backpressure, over the recorded
     * body entries of a {@link DefaultHttpResponse}.
     *
     * <p>For chunked framing the body is produced directly into the connection's output buffer:
     * a fixed-width hex size field (zero-padded) plus CRLF is reserved up front, the
     * {@link ByteContent} writes body bytes straight into {@code out}, and the reserved header is
     * then backfilled in place with the exact size. This is zero-copy (no scratch buffer) and
     * allocation-free in steady state. Leading zeros in the chunk-size are permitted by RFC 7230.
     *
     * <p>An entry's release callback is fired the moment its {@link ByteContent} is fully
     * consumed into the output buffer (its data is then copied and safe to reuse).
     */
    private static final class ResponseBodyDriver {
        // Driver lifecycle. A fixed body is written whole; a chunked body is incremental, so it walks
        // OPEN (chunks may still arrive) -> CLOSING (commit()ed, terminating last chunk still owed) ->
        // DONE (terminator written). DONE is distinct from CLOSING so a pump() re-entered to drain the
        // buffer tail after the terminator is idempotent (returns DONE, never re-emits "0\r\n\r\n").
        private static final int ST_IDLE = 0;            // no body in flight
        private static final int ST_FIXED = 1;           // fixed-length body
        private static final int ST_CHUNKED_OPEN = 2;    // chunked, not yet committed
        private static final int ST_CHUNKED_CLOSING = 3; // chunked, committed; last chunk not yet written
        private static final int ST_CHUNKED_DONE = 4;    // chunked, committed; last chunk written

        // pump() outcomes: the whole body is enqueued (DONE), the output buffer could not drain so
        // OP_WRITE backpressure applies (BLOCKED), or an open chunked response has drained the chunks
        // recorded so far but has not been committed and awaits the next addChunk()/commit() (AWAIT).
        private static final int PUMP_DONE = 0;
        private static final int PUMP_BLOCKED = 1;
        private static final int PUMP_AWAIT = 2;

        // Fixed width of the reserved hex size field. A single chunk can never carry more than
        // the output buffer holds, so hexDigits(outputBufferBytes) digits always suffice; derived
        // from the connection's configured output-buffer size so it stays correct for any cap.
        private final int hexWidth;

        private int state = ST_IDLE;
        private DefaultHttpResponse response;   // supplies the recorded body/chunk entries
        private long fixedRemaining;     // fixed mode: bytes still owed against Content-Length
        private int entryIndex;          // chunked mode: current chunk entry being streamed
        private int sourceOffset;        // bytes already pulled from the current entry's content

        private ResponseBodyDriver(final int outputBufferBytes) {
            this.hexWidth = hexDigits(outputBufferBytes);
        }

        private void reset() {
            state = ST_IDLE;
            response = null;
            fixedRemaining = 0;
            entryIndex = 0;
            sourceOffset = 0;
        }

        /** Fixed (buffered) body: the single {@code setBody} entry is written in full at commit(). */
        private void beginFixed(final DefaultHttpResponse response, final long contentLength) {
            state = ST_FIXED;
            this.response = response;
            this.fixedRemaining = contentLength;
            this.entryIndex = 0;
            this.sourceOffset = 0;
        }

        /** Chunked (incremental) body: chunk entries are appended and flushed as addChunk() is called. */
        private void beginChunked(final DefaultHttpResponse response) {
            state = ST_CHUNKED_OPEN;
            this.response = response;
            this.entryIndex = 0;
            this.sourceOffset = 0;
        }

        /** commit(): no more chunks -- the next pump emits the terminating last chunk. */
        private void markStreamClosed() {
            state = ST_CHUNKED_CLOSING;
        }

        /**
         * True when every chunk entry recorded so far has been fully written into the output buffer
         * (idle, or all recorded parts consumed). Used by {@link Connection#whenDrained} /
         * {@link Connection#signalDrainIfIdle} to decide the streaming producer can add the next chunk.
         */
        private boolean chunksDrained() {
            return response == null || entryIndex >= response.entryCount;
        }

        /**
         * Push as much body as fits into the connection's output buffer. Returns {@link #PUMP_DONE}
         * when the entire body (incl. terminator) has been enqueued, {@link #PUMP_BLOCKED} on
         * output-buffer backpressure, or {@link #PUMP_AWAIT} when an open chunked body has drained the
         * chunks recorded so far but has not been committed. Caller flushes 'out' between/after calls.
         */
        private int pump(final Connection connection) {
            if (state == ST_FIXED) {
                return pumpFixed(connection);
            }
            if (state == ST_CHUNKED_OPEN || state == ST_CHUNKED_CLOSING || state == ST_CHUNKED_DONE) {
                return pumpChunked(connection);
            }
            return PUMP_DONE;
        }

        private int pumpFixed(final Connection connection) {
            final ByteBuffer out = connection.out;
            final DefaultHttpResponse r = response;
            // A fixed body is one or more parts (setBody = a single part; addBody = several); write
            // each fully, in order, until the summed Content-Length is satisfied.
            while (fixedRemaining > 0) {
                final BodyEntry entry = r.entry(entryIndex);
                final ByteContent content = entry.content;
                final long entryLength = content.length();
                while (sourceOffset < entryLength) {
                    final int cap = (int) Math.min(out.remaining(),
                            Math.min(entryLength - sourceOffset, fixedRemaining));
                    if (cap == 0) {                          // out full -> flush and retry
                        if (!connection.tryFlush()) {
                            return PUMP_BLOCKED;
                        }
                        continue;
                    }
                    final int pos = out.position();
                    final int limit = out.limit();
                    out.limit(pos + cap);                    // never exceed the owed bytes / this part
                    final int n = content.write(sourceOffset, out);   // relative: advances out.position()
                    out.limit(limit);
                    if (n == 0) {                            // couldn't fit even one unit in 'cap'
                        if (pos == 0) {                       // ...with an empty buffer -> contract violation
                            throw new IllegalStateException(
                                    "ByteContent wrote no bytes with room available; length() over-promised");
                        }
                        if (!connection.tryFlush()) {        // flush to grow room, then retry
                            return PUMP_BLOCKED;
                        }
                        continue;
                    }
                    sourceOffset += n;
                    fixedRemaining -= n;
                }
                entry.fire();                                // part fully consumed -> the data is reusable
                entryIndex++;
                sourceOffset = 0;                            // rewind for the next part
            }
            return PUMP_DONE;
        }

        private int pumpChunked(final Connection connection) {
            final ByteBuffer out = connection.out;
            final ResponseWriter writer = connection.responseWriter;
            final DefaultHttpResponse r = response;

            while (entryIndex < r.entryCount) {
                final BodyEntry entry = r.entry(entryIndex);
                final ByteContent content = entry.content;
                final long entryLength = content.length();
                while (sourceOffset < entryLength) {
                    // Need room for the fixed-width header + CRLF, at least one data byte, and the
                    // trailing CRLF.
                    final int room = out.remaining();
                    if (room < hexWidth + 2 + 1 + 2) {
                        if (!connection.tryFlush()) {
                            return PUMP_BLOCKED;
                        }
                        continue;
                    }
                    // Reserve the fixed-width header + CRLF up front and the trailing CRLF at the end;
                    // the content writes body bytes directly into 'out' between them. We park the
                    // position at dataPos for the relative write, then either backfill the reserved
                    // header (n>0) or rewind to headerPos to discard the reservation (n==0).
                    final int dataRoom = room - hexWidth - 2 - 2;
                    final int headerPos = out.position();
                    final int dataPos = headerPos + hexWidth + 2;
                    final int limit = out.limit();
                    out.limit(dataPos + dataRoom);
                    out.position(dataPos);
                    final int n = content.write(sourceOffset, out);   // relative: advances to dataPos + n
                    out.limit(limit);
                    if (n == 0) {                         // couldn't fit a unit in dataRoom -> flush
                        out.position(headerPos);           // discard the reservation
                        if (!connection.tryFlush()) {
                            return PUMP_BLOCKED;
                        }
                        continue;
                    }
                    sourceOffset += n;

                    // Backfill "<zero-padded hex-size>\r\n" over the reserved slot (absolute puts, so
                    // the position stays at dataPos + n), then close with the trailing CRLF. n <=
                    // dataRoom <= outputBufferBytes, so hexWidth digits suffice.
                    writePaddedHex(out, headerPos, n);
                    out.put(headerPos + hexWidth, (byte) '\r');
                    out.put(headerPos + hexWidth + 1, (byte) '\n');
                    out.put((byte) '\r');
                    out.put((byte) '\n');
                }
                entry.fire();                             // entry fully consumed -> the data is reusable
                entryIndex++;
                sourceOffset = 0;                         // rewind for the next entry's content
            }

            // All recorded chunks drained. An open (not-yet-committed) chunked response awaits the
            // next addChunk()/commit() before emitting the terminating last chunk. Recycle the consumed
            // chunk entries now so a long-lived stream neither retains every chunk's backing data nor
            // grows the entries[] array without bound (the drained bytes are already on the wire).
            if (state == ST_CHUNKED_OPEN) {
                r.recycleBody();
                entryIndex = 0;
                return PUMP_AWAIT;
            }

            if (state == ST_CHUNKED_CLOSING) {
                if (!writer.lastChunk()) {
                    if (!connection.tryFlush()) {
                        return PUMP_BLOCKED;
                    }
                    if (!writer.lastChunk()) {
                        return PUMP_BLOCKED;
                    }
                }
                state = ST_CHUNKED_DONE;
            }
            // ST_CHUNKED_DONE: terminator already written; re-entry to drain the buffer tail is a
            // no-op DONE (never re-emits "0\r\n\r\n").
            return PUMP_DONE;
        }

        private static int hexDigits(final int value) {
            if (value == 0) {
                return 1;
            }
            int remaining = value;
            int count = 0;
            while (remaining > 0) {
                remaining >>>= 4;
                count++;
            }
            return count;
        }

        // Zero-padded, lowercase, big-endian hex of 'value' into hexWidth bytes at absolute
        // 'pos' (does not touch the buffer position). Caller guarantees value fits in hexWidth.
        private void writePaddedHex(final ByteBuffer out, final int pos, final int value) {
            int remaining = value;
            for (int i = hexWidth - 1; i >= 0; i--) {
                final int digit = remaining & 0xF;
                out.put(pos + i, (byte) (digit < 10 ? '0' + digit : 'a' + digit - 10));
                remaining >>>= 4;
            }
        }
    }
}
