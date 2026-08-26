/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockInfo;
import io.github.green4j.discas.client.lock.LockToken;

import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;
import io.github.green4j.discas.common.http.server.HttpServer.HttpResponse;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * HTTP handler for the distributed-lock surface under {@code /v1/lock/...}. The lock key is the
 * decoded request path after the {@code /v1/lock} prefix. A lock token is carried by the
 * {@code X-DisCas-Lock-Token} header (or {@code ?token=}) as lowercase hex.
 *
 * <ul>
 *   <li>{@code PUT /v1/lock/{key}?ttl=<s>&owner=<id>[&wait=<s>]} -- acquire; {@code wait} present
 *       blocks up to that many seconds, otherwise a single try. Returns
 *       {@code {"status","acquired","token"|"info"}}.</li>
 *   <li>{@code PUT /v1/lock/{key}?recover&owner=<id>} -- hand back the lock already standing in
 *       that name, for an acquire whose response was lost. Answers like an acquire.</li>
 *   <li>{@code PUT /v1/lock/{key}?renew&ttl=<s>} (+token) -- extend the lease.</li>
 *   <li>{@code DELETE /v1/lock/{key}} (+token) -- release.</li>
 *   <li>{@code GET /v1/lock/{key}} -- current lock info.</li>
 * </ul>
 *
 * <p>{@code owner} is required on an acquire. Over HTTP the response to an acquire is exactly what
 * a dropped connection or a gateway timeout takes away, so the caller-chosen name in the record is
 * the only way back to a lock that was in fact taken -- which is what {@code ?recover} uses.
 */
final class LockHandler extends AbstractHandler {

    static final String PREFIX = "/v1/lock";

    LockHandler(final ReloadableClient clientHolder, final Duration requestTimeout) {
        super(clientHolder, requestTimeout);
    }

    @Override
    protected void handle(final Connection connection, final HttpRequest request) throws HttpErrorException {
        final String key = keyAfter(PREFIX);
        if (key.isEmpty()) {
            throw new HttpErrorException(400, "missing lock key");
        }
        requireKeyWithinLimit(key);
        final String method = method();
        if ("GET".equals(method)) {
            handleInfo(connection, request, key);
        } else if ("PUT".equals(method) || "POST".equals(method)) {
            if (query().contains("renew")) {
                handleRenew(connection, request, key);
            } else if (query().contains("recover")) {
                handleRecover(connection, request, key);
            } else {
                handleAcquire(connection, request, key);
            }
        } else if ("DELETE".equals(method)) {
            handleRelease(connection, request, key);
        } else {
            throw new HttpErrorException(405, "method not allowed: " + method);
        }
    }

    private void handleAcquire(final Connection connection, final HttpRequest request, final String key)
            throws HttpErrorException {
        final long ttlSeconds = requiredLongParam("ttl");
        if (ttlSeconds < 1) {
            throw new HttpErrorException(400, "invalid ttl: must be >= 1 second");
        }
        final Duration ttl = Duration.ofSeconds(ttlSeconds);
        final String owner = requiredOwner();

        // wait absent or <= 0 => a single try; wait > 0 => block up to that many seconds.
        final long waitSeconds = longParam("wait", 0);
        final CompletableFuture<LockAcquireResult> op =
                waitSeconds > 0
                        ? client.lock(key, ttl, Duration.ofSeconds(waitSeconds), owner)
                        : client.tryLock(key, ttl, owner);

        bridge(connection, request, beginJson(connection, request), op, LockHandler::writeAcquire);
    }

    /**
     * Hands back the lock that already stands in {@code owner}'s name: the recovery for an acquire
     * whose response was lost, which over HTTP is every acquire that ends in a dropped connection
     * or a gateway timeout. Answers exactly like an acquire, so a caller can retry into this
     * without a second shape to parse.
     */
    private void handleRecover(final Connection connection, final HttpRequest request, final String key)
            throws HttpErrorException {
        bridge(connection, request, beginJson(connection, request),
                client.recoverLock(key, requiredOwner()), LockHandler::writeAcquire);
    }

    private static void writeAcquire(final HttpResponse resp, final LockAcquireResult result) {
        final Json json = Json.object()
                .field("status", result.status().name())
                .field("acquired", result.acquired());
        if (result.acquired()) {
            json.field("token", AgentSupport.hex(result.lock().token().bytes()))
                    .field("generation", result.lock().fencingToken());
        } else {
            json.rawField("info", lockInfoJson(result.observed()));
        }
        resp.setBodyUtf8(json.end()).ok();
    }

    /**
     * The {@code owner} the acquire is made under. Required rather than generated: an id the
     * caller never chose cannot identify its own lock afterwards, which is the whole reason the
     * field is in the record.
     */
    private String requiredOwner() throws HttpErrorException {
        final String owner = query().firstStringValue("owner");
        if (owner == null || owner.isEmpty()) {
            throw new HttpErrorException(400,
                    "missing owner: name this holder with ?owner=<id>, unique among whatever may "
                            + "hold this key at once -- it is what identifies your own lock if the "
                            + "response to an acquire is lost");
        }
        return owner;
    }

    private void handleRenew(final Connection connection, final HttpRequest request, final String key)
            throws HttpErrorException {
        final LockToken token = requireToken();
        final long ttlSeconds = requiredLongParam("ttl");
        if (ttlSeconds < 1) {
            throw new HttpErrorException(400, "invalid ttl: must be >= 1 second");
        }
        bridge(connection, request, beginJson(connection, request),
                client.renewLock(key, token, Duration.ofSeconds(ttlSeconds)),
                (resp, renewed) -> resp.setBodyUtf8(Json.object()
                        .field("renewed", renewed.applied())
                        .field("status", renewed.status().name())
                        .end()).ok());
    }

    private void handleRelease(final Connection connection, final HttpRequest request, final String key)
            throws HttpErrorException {
        final LockToken token = requireToken();
        bridge(connection, request, beginJson(connection, request),
                client.release(key, token),
                (resp, released) -> resp.setBodyUtf8(Json.object()
                        .field("released", released.applied())
                        .field("status", released.status().name())
                        .end()).ok());
    }

    private void handleInfo(final Connection connection, final HttpRequest request, final String key) {
        bridge(connection, request, beginJson(connection, request),
                client.getLockInfo(key),
                (resp, result) -> resp.setBodyUtf8(Json.object()
                        .field("status", result.status().name())
                        .rawField("info", lockInfoJson(result.info()))
                        .end()).ok());
    }

    /** The lock token from the {@code X-DisCas-Lock-Token} header or {@code ?token=}; a missing or
     * malformed token is a {@code 400}. */
    private LockToken requireToken() throws HttpErrorException {
        String hex = headerLockToken();
        if (hex == null) {
            hex = query().firstStringValue("token");
        }
        final byte[] bytes = AgentSupport.fromHex(hex);
        if (bytes == null) {
            throw new HttpErrorException(400, "missing or invalid lock token");
        }
        return new LockToken(bytes);
    }

    /**
     * Describes a lock without its token. The token is what renew and release are conditioned on,
     * and a lock reading is answerable to anyone who can reach the agent, so printing it here would
     * hand every reader the means to end a lease it does not hold. A holder that lost its own token
     * asks for it back by name with {@code ?recover}.
     */
    private static String lockInfoJson(final LockInfo info) {
        if (info == null) {
            return "null";
        }
        return Json.object()
                .field("owner", info.ownerId())
                .field("generation", info.generation())
                .field("acquiredAtEpochMs", info.acquiredAtEpochMs())
                .field("leaseUntilEpochMs", info.leaseUntilEpochMs())
                .field("expired", info.expired())
                .end();
    }
}
