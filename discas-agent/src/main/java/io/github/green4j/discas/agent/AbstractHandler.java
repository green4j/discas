/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasOperationException;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.http.server.HttpRequestHeader;
import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ContentType;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import io.github.green4j.discas.common.http.server.HttpServer.HttpResponse;
import io.github.green4j.discas.common.http.server.QueryPathParser;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * Per-connection routing handler base. Confined to one worker thread, so its mutable state needs
 * no synchronization. Subclasses implement {@link #handle} once the full request (line, headers,
 * body) has arrived; the request path is available decoded via {@link #query()} and the request
 * body via {@link #bodyBuffer()}.
 */
abstract class AbstractHandler implements ConnectionHandler {

    private static final byte[] EMPTY = new byte[0];
    private static final int INITIAL_BODY_CAPACITY = 256;

    // Set fresh from the holder at the start of each request, so a nodes-file reload that swaps
    // the underlying client takes effect on the next request. Confined to one worker thread.
    protected DisCasClient client;
    private final ReloadableClient clientHolder;
    private final Duration requestTimeout;

    private final QueryPathParser query = new QueryPathParser();
    private final HttpRequestHeader headers = new HttpRequestHeader();

    private byte[] body = EMPTY;
    private int bodyLen;
    private boolean bodyOverflow;

    // Request method, materialized during the request-line phase: the request line bytes are only
    // valid in the input buffer until a body arrives and compacts it, so method/path must be
    // cached now to be readable later in onRequestComplete.
    private String method;

    // Snapshotted at onHeadersComplete (header flyweights are only valid through that phase).
    private String headerLockToken;

    AbstractHandler(final ReloadableClient clientHolder, final Duration requestTimeout) {
        this.clientHolder = clientHolder;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Handle a fully-received request. Must begin (and eventually commit) exactly one response, or
     * throw {@link HttpErrorException} <em>before</em> beginning one to signal a client error (which
     * {@link #onRequestComplete} renders).
     */
    protected abstract void handle(Connection connection, HttpRequest request) throws HttpErrorException;

    @Override
    public final void onRequestLine(final Connection connection, final HttpRequest request) {
        headers.reset();
        bodyLen = 0;
        bodyOverflow = false;
        headerLockToken = null;
        // Force method/path into their per-request String caches while the request-line bytes are
        // still intact in the input buffer (a later body read compacts them away).
        method = request.methodString();
        request.pathString();
    }

    @Override
    public final void onHeader(final Connection connection, final HttpRequest request,
                               final ByteBuffer buffer, final int nameOffset, final int nameLength,
                               final int valueOffset, final int valueLength) {
        headers.add(buffer, nameOffset, nameLength, valueOffset, valueLength);
    }

    @Override
    public final void onHeadersComplete(final Connection connection, final HttpRequest request) {
        // Snapshot to String now: the flyweights window the live input buffer and are re-pointed
        // once the body arrives, so they must be materialized during the header phase.
        headerLockToken = headers.firstStringValue(AgentSupport.HEADER_LOCK_TOKEN);
    }

    @Override
    public final void onBodyChunk(final Connection connection, final HttpRequest request,
                                  final ByteBuffer buffer, final int offset, final int length) {
        if (bodyOverflow) {
            return;
        }
        if ((long) bodyLen + length > DisCasClient.MAX_VALUE_BYTES) {
            bodyOverflow = true;
            return;
        }
        ensureBodyCapacity(bodyLen + length);
        for (int i = 0; i < length; i++) {
            body[bodyLen + i] = buffer.get(offset + i);
        }
        bodyLen += length;
    }

    @Override
    public final void onRequestComplete(final Connection connection, final HttpRequest request) {
        // Resolve the client in effect for this request (may have been swapped by a reload).
        client = clientHolder.current();
        if (bodyOverflow) {
            error(connection, request, 413, "request body exceeds maximum value size "
                    + DisCasClient.MAX_VALUE_BYTES + " bytes");
            return;
        }
        query.parse(request.pathString());
        // Handlers parse and validate everything up front -- a bad request throws HttpErrorException
        // (missing/oversized key, bad ttl/wait/index/token, unknown method) *before* any response is
        // begun; only then is the single response begun (via bridge). So this catch always renders
        // the sole response: no half-built response can exist here to double-commit.
        try {
            handle(connection, request);
        } catch (final HttpErrorException e) {
            error(connection, request, e.status(), e.reason());
        }
    }

    /** The request method (e.g. {@code GET}), cached during the request-line phase. */
    protected final String method() {
        return method;
    }

    /**
     * Query parameter {@code name} as a {@code long}, or {@code defaultValue} when it is absent. A
     * value that is <em>present but not a number</em> is a {@code 400} ({@link HttpErrorException}),
     * never silently replaced by the default -- a parse error is a client error, not a default.
     */
    protected final long longParam(final String name, final long defaultValue)
            throws HttpErrorException {
        final String raw = query.firstStringValue(name);
        return raw == null ? defaultValue : parseLongParam(name, raw);
    }

    /** Query parameter {@code name} as a {@code long}; a missing or non-numeric value is a {@code 400}. */
    protected final long requiredLongParam(final String name) throws HttpErrorException {
        final String raw = query.firstStringValue(name);
        if (raw == null) {
            throw new HttpErrorException(400, "missing " + name);
        }
        return parseLongParam(name, raw);
    }

    private static long parseLongParam(final String name, final String raw) throws HttpErrorException {
        try {
            return Long.parseLong(raw.trim());
        } catch (final NumberFormatException e) {
            throw new HttpErrorException(400, "invalid " + name + ": not a number");
        }
    }

    protected final QueryPathParser query() {
        return query;
    }

    protected final String headerLockToken() {
        return headerLockToken;
    }

    /** The hard per-request timeout; also the ceiling for a blocking-query wait. */
    protected final Duration requestTimeout() {
        return requestTimeout;
    }

    /**
     * The accumulated request body as a fresh, independent buffer (empty if none). A copy: the
     * backing array is reused on the next request, while the client may retain the buffer across
     * retry attempts.
     */
    protected final ByteBuffer bodyBuffer() {
        return ByteBuffer.wrap(Arrays.copyOf(body, bodyLen));
    }

    /**
     * Guard {@code key} against the cluster's key-size limit before it is handed to a client op.
     * The client validates key size <em>synchronously</em> (throwing before it returns its future,
     * see {@code DisCasClient#checkKeySize}); since the response is begun before that call, an
     * over-limit key would otherwise unwind out of {@code handle} leaving the response
     * begun-but-uncommitted. Throws {@code 400} when too large (rendered by {@link #onRequestComplete}).
     */
    protected final void requireKeyWithinLimit(final String key) throws HttpErrorException {
        if (AgentSupport.utf8(key).length > DisCasClient.MAX_KEY_BYTES) {
            throw new HttpErrorException(400,
                    "key exceeds maximum key size " + DisCasClient.MAX_KEY_BYTES + " bytes");
        }
    }

    /**
     * The decoded request path with {@code prefix} stripped (leading slashes removed), e.g. for
     * prefix {@code "/v1/kv"} and path {@code "/v1/kv/foo/bar"} returns {@code "foo/bar"}. Returns
     * an empty string when the remainder is empty (a scope/listing request).
     */
    protected final String keyAfter(final String prefix) {
        final String path = query.pathString();
        if (!path.startsWith(prefix)) {
            return "";
        }
        int start = prefix.length();
        while (start < path.length() && path.charAt(start) == '/') {
            start++;
        }
        return path.substring(start);
    }

    /** Begin a buffered {@code application/json} response (body/status set later). */
    protected final HttpResponse beginJson(final Connection connection,
                                           final HttpRequest request) {
        return connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive());
    }

    /** Begin a buffered {@code application/octet-stream} response (body/status set later). */
    protected final HttpResponse beginRaw(final Connection connection,
                                          final HttpRequest request) {
        return connection.beginResponse(ContentType.OCTET_STREAM, request.keepAlive());
    }

    /**
     * Send a terminal error as a JSON {@code {"error":...}} envelope with {@code status}. Keep-alive
     * follows the request (an HTTP-level 4xx/5xx keeps the connection pooled); {@link HttpResponse#error}
     * reuses the begun {@code application/json} content type.
     */
    protected final void error(final Connection connection,
                               final HttpRequest request,
                               final int status,
                               final String message) {
        connection.beginResponse(ContentType.APPLICATION_JSON, request.keepAlive())
                .error(status, Json.error(message));
    }

    /**
     * Bridge a client {@link CompletableFuture} onto this connection's worker thread with a hard
     * per-request timeout. On success {@code onOk} renders the response; on failure a {@code 504}
     * (timeout), {@code 403} (authorization refused) or {@code 500} JSON error is sent. The
     * connection stays open (a deferred failure is a normal per-request outcome):
     * {@link HttpResponse#error} keeps alive per the begun keep-alive flag and reuses the begun
     * {@code application/json} content type. Call on the worker thread.
     */
    protected final <T> void bridge(final Connection connection,
                                    final HttpRequest request,
                                    final HttpResponse response,
                                    final CompletableFuture<T> future,
                                    final BiConsumer<HttpResponse, T> onOk) {
        response.async(future.orTimeout(requestTimeout.toMillis(), TimeUnit.MILLISECONDS))
                .onSuccess(value -> {
                    try {
                        onOk.accept(response, value);
                    } catch (final Throwable t) {
                        errorJson(response, 500, "response build failed: " + t.getMessage());
                    }
                })
                .onFailure(err -> {
                    final Throwable cause = unwrap(err);
                    if (cause instanceof TimeoutException) {
                        errorJson(response, 504, "request timed out after "
                                + requestTimeout.toMillis() + " ms");
                    } else {
                        errorJson(response, statusOf(cause), cause.getMessage() == null
                                ? cause.getClass().getSimpleName() : cause.getMessage());
                    }
                });
    }

    /**
     * The HTTP status for a failed client operation, from the node's
     * {@link ClientErrorCode}. This is the only place the cluster's failure classes are
     * translated to HTTP -- the client and node layers know nothing about status codes.
     * <p>
     * A failure that never reached a node (send failure, client shut down mid-request) is not
     * a {@link DisCasOperationException} and has no node verdict to report, so it stays 500.
     */
    private static int statusOf(final Throwable cause) {
        if (!(cause instanceof DisCasOperationException)) {
            return 500;
        }
        switch (((DisCasOperationException) cause).code()) {
            case ACCESS_DENIED:
                return 403;
            case INVALID_ARGUMENT:
                // Oversized key or value: the request is wrong and will fail identically on
                // every retry, so it must not be reported as a server fault.
                return 400;
            case STORE_FULL:
                // Determinate and not the caller's fault, so neither 400 nor 503: the write did not
                // happen and re-issuing it changes nothing until somebody deletes something. Every
                // replica holds the same keys, so this is the cluster's answer, not one node's.
                return 507;
            case NOT_READY:   // the node is still replaying its log
            case NO_QUORUM_AT_COORDINATOR: // that coordinator cannot see a majority right now
            case UNAVAILABLE: // no quorum, round timed out, or the proposer is saturated
            case BALLOT_LOST: // another proposer is writing the same key
            case PROPOSAL_EXPIRED: // the write outlived the coordinator's proposalExpiry
                // Every one of these is transient and worth re-issuing, which 503 says and 500
                // does not. The two determinate ones -- BALLOT_LOST and PROPOSAL_EXPIRED -- are
                // the safest of the set to re-issue, since the write provably did not happen.
                return 503;
            default:
                return 500;
        }
    }

    private static void errorJson(final HttpResponse response, final int status, final String message) {
        response.error(status, Json.error(message));
    }

    private static Throwable unwrap(final Throwable t) {
        Throwable cause = t;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private void ensureBodyCapacity(final int needed) {
        if (needed <= body.length) {
            return;
        }
        int cap = body.length == 0 ? INITIAL_BODY_CAPACITY : body.length;
        while (cap < needed) {
            cap <<= 1;
            if (cap < 0) { // overflow guard
                cap = needed;
                break;
            }
        }
        final byte[] grown = new byte[cap];
        System.arraycopy(body, 0, grown, 0, bodyLen);
        body = grown;
    }
}
