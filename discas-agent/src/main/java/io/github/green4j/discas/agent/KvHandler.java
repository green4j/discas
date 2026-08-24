/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.client.ScanCoverage;
import io.github.green4j.discas.client.ScanResult;
import io.github.green4j.discas.client.Version;

import io.github.green4j.discas.common.client.ReadConsistency;

import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.HttpHeader;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP handler for the KV surface under {@code /v1/kv/...}. Keys are hierarchical: the key is the
 * decoded request path after the {@code /v1/kv} prefix, so {@code /v1/kv/foo/bar} addresses key
 * {@code foo/bar}.
 *
 * <ul>
 *   <li>{@code GET /v1/kv/{key}} -- read; {@code 404} if absent. {@code ?raw} returns the raw value
 *       bytes, otherwise a JSON envelope {@code {"Key","Value":<base64>,"Flags":0}}. Every read
 *       carries the key's version as {@code X-DisCas-Version}, including the {@code 404}.</li>
 *   <li>{@code GET /v1/kv/{prefix}?keys} -- list all keys (via scan), filtered by {@code prefix}.
 *       {@code X-DisCas-Complete} says whether a majority answered, with
 *       {@code X-DisCas-Responded} / {@code X-DisCas-Cluster-Size} as the counts behind it.</li>
 *   <li>{@code PUT /v1/kv/{key}} -- body is the raw value. {@code ?cas=<version>} does a
 *       <em>version-fenced</em> compare-and-set against a version previously returned as
 *       {@code X-DisCas-Version} ({@code cas=0} = create-if-absent), returning
 *       {@code {"swapped":bool,"value":<base64|null>}} plus the resulting
 *       {@code X-DisCas-Version}. Otherwise a plain put. A bare {@code ?cas} with no version is a
 *       {@code 400}: there is no value-compared CAS.</li>
 *   <li>{@code DELETE /v1/kv/{key}} -- delete; {@code ?cas=<version>} fences it on the version the
 *       same way, and answers in the same shape as a fenced PUT.</li>
 * </ul>
 *
 * <p>Prefer the fenced forms for read-modify-write: they are the ones that stay safe when a
 * coordinator stops answering.
 */
final class KvHandler extends AbstractHandler {

    static final String PREFIX = "/v1/kv";

    private static final HttpHeader VERSION_HEADER = HttpHeader.of(AgentSupport.HEADER_VERSION);
    private static final HttpHeader CHANGED_HEADER = HttpHeader.of(AgentSupport.HEADER_CHANGED);
    private static final HttpHeader COMPLETE_HEADER = HttpHeader.of(AgentSupport.HEADER_COMPLETE);
    private static final HttpHeader RESPONDED_HEADER = HttpHeader.of(AgentSupport.HEADER_RESPONDED);
    private static final HttpHeader CLUSTER_SIZE_HEADER =
            HttpHeader.of(AgentSupport.HEADER_CLUSTER_SIZE);
    // A blocking-query wait must complete normally before the request-timeout fires (bridge() turns
    // that into a 504), so it is capped this far below it. The client re-polls on an unchanged reply.
    private static final long WATCH_WAIT_MARGIN_MS = 1_000L;

    KvHandler(final ReloadableClient clientHolder, final Duration requestTimeout) {
        super(clientHolder, requestTimeout);
    }

    @Override
    protected void handle(final Connection connection, final HttpRequest request) throws HttpErrorException {
        final String method = method();
        if ("GET".equals(method)) {
            handleGet(connection, request);
        } else if ("PUT".equals(method) || "POST".equals(method)) {
            handlePut(connection, request);
        } else if ("DELETE".equals(method)) {
            handleDelete(connection, request);
        } else {
            throw new HttpErrorException(405, "method not allowed: " + method);
        }
    }

    private void handleGet(final Connection connection, final HttpRequest request) throws HttpErrorException {
        final String key = keyAfter(PREFIX);
        if (query().contains("keys")) {
            handleList(connection, request, key);
            return;
        }
        if (key.isEmpty()) {
            throw new HttpErrorException(400, "missing key");
        }
        requireKeyWithinLimit(key);
        final boolean raw = query().contains("raw");
        // ?stale opts the read down to a serializable one: the coordinator answers from its own
        // committed state instead of running a Paxos round. Cheaper, and it still answers when the
        // cluster has lost quorum -- at the cost of possibly being behind. Same word as on the
        // listing above, and the same meaning ("a stale answer is acceptable"), applied to the
        // axis this endpoint has: freshness of one key rather than coverage of the key set.
        final ReadConsistency consistency = readConsistency();
        if (query().contains("version") || query().contains("wait")) {
            handleWatch(connection, request, key, raw, consistency);
            return;
        }
        // Choose the content type up front: a response's framing is fixed at begin.
        bridge(connection, request,
                raw ? beginRaw(connection, request) : beginJson(connection, request),
                client.get(key, consistency),
                (resp, read) -> {
                    // Every read carries the version, not just a blocking one: the version comes
                    // back on the wire regardless, and a caller that wants to watch this key next,
                    // or fence a ?cas= write on what it just read, would otherwise have to ask
                    // again with ?wait=0 purely to be told something this response already knew.
                    resp.header(VERSION_HEADER, encodeVersion(read.version()));
                    final ByteBuffer value = read.value();
                    if (value == null) {
                        // Match the begun content type: a raw miss is a bodyless 404, not a JSON
                        // error under application/octet-stream (mirrors the watch path).
                        if (raw) {
                            resp.commit(404);
                        } else {
                            resp.setBodyUtf8(Json.error("not found")).commit(404);
                        }
                        return;
                    }
                    final byte[] bytes = AgentSupport.toBytes(value);
                    if (raw) {
                        resp.setBody(bytes).ok();
                        return;
                    }
                    // Consul-compatible key names -- see the Json javadoc.
                    resp.setBodyUtf8(Json.object()
                            .field("Key", key)
                            .field("Value", AgentSupport.base64(bytes))
                            .field("Flags", 0L)
                            .end()).ok();
                });
    }

    /**
     * Blocking-query read. Returns the current value together with the {@code X-DisCas-Version} version
     * once the key changes past {@code ?version=} or the (capped) {@code ?wait=} elapses. On an
     * unchanged reply the version equals the one supplied, so the client detects "no change".
     */
    private void handleWatch(final Connection connection, final HttpRequest request,
                             final String key, final boolean raw,
                             final ReadConsistency consistency) throws HttpErrorException {
        final Version since = decodeVersion(query().firstStringValue("version"));
        final Duration wait = watchWait(query().firstStringValue("wait"));
        bridge(connection, request,
                raw ? beginRaw(connection, request) : beginJson(connection, request),
                client.watch(key, since, wait, consistency),
                (resp, result) -> {
                    resp.header(VERSION_HEADER, encodeVersion(result.version()))
                            .header(CHANGED_HEADER, result.changed() ? "true" : "false");
                    if (result.value() == null) {
                        // Absent or tombstoned: still hand back the version so the caller can long-poll
                        // for a (re)creation by re-issuing with this version.
                        if (raw) {
                            resp.commit(404);
                        } else {
                            resp.setBodyUtf8(Json.error("not found")).commit(404);
                        }
                        return;
                    }
                    final byte[] bytes = AgentSupport.toBytes(result.value());
                    if (raw) {
                        resp.setBody(bytes).ok();
                        return;
                    }
                    // Consul-compatible key names -- see the Json javadoc.
                    resp.setBodyUtf8(Json.object()
                            .field("Key", key)
                            .field("Value", AgentSupport.base64(bytes))
                            .field("Flags", 0L)
                            .end()).ok();
                });
    }

    /**
     * The consistency a read runs at. Reads are linearizable unless {@code ?stale} is given.
     * <p>
     * Not offered on the lock endpoints: a lock decision taken on a possibly-stale read is a lock
     * two holders can believe they own.
     */
    private ReadConsistency readConsistency() {
        return query().contains("stale") ? ReadConsistency.SERIALIZABLE : ReadConsistency.LINEARIZABLE;
    }

    /** Effective wait: the requested seconds (absent = the full budget), capped under the request
     * timeout so {@code watch} completes normally before {@code bridge()}'s hard timeout fires. A
     * present-but-malformed {@code wait} is a {@code 400} rather than silently the full budget. */
    private Duration watchWait(final String waitParam) throws HttpErrorException {
        final long capMs = Math.max(0L, requestTimeout().toMillis() - WATCH_WAIT_MARGIN_MS);
        if (waitParam == null) {
            return Duration.ofMillis(capMs); // absent: wait the full (capped) budget
        }
        final long secs;
        try {
            secs = Long.parseLong(waitParam.trim());
        } catch (final NumberFormatException e) {
            throw new HttpErrorException(400, "invalid wait: not a number");
        }
        final long requestedMs = secs > 0 ? secs * 1_000L : 0L;
        return Duration.ofMillis(Math.min(requestedMs, capMs));
    }

    /** Decode a {@code ?version=} value (hex of the version token) to a {@link Version}; an absent
     * one yields {@link Version#INITIAL} (fire on the first committed value), a present-but-malformed
     * one is a {@code 400} (a parse error is not silently reset, matching {@code ?wait}). */
    private static Version decodeVersion(final String versionParam) throws HttpErrorException {
        if (versionParam == null || versionParam.isEmpty()) {
            return Version.INITIAL;
        }
        final byte[] bytes = AgentSupport.fromHex(versionParam);
        if (bytes == null) {
            throw new HttpErrorException(400, "invalid version: not hex");
        }
        try {
            return Version.parse(new String(bytes, StandardCharsets.UTF_8));
        } catch (final RuntimeException e) {
            throw new HttpErrorException(400, "invalid version");
        }
    }

    private static String encodeVersion(final Version version) {
        return AgentSupport.hex(ByteBuffer.wrap(AgentSupport.utf8(version.token())));
    }

    private void handleList(final Connection connection, final HttpRequest request, final String prefix) {
        // The prefix is pushed down to the nodes, so each walks only the matching key range and
        // returns only matching keys.
        // ?stale opts into a best-effort listing: return whatever nodes answered, even below a
        // majority, instead of failing. The default keeps the completeness guarantee, since a
        // listing that silently omits committed keys is the failure mode worth avoiding here.
        final ScanCoverage coverage = query().contains("stale")
                ? ScanCoverage.ANY_AVAILABLE
                : ScanCoverage.QUORUM;
        bridge(connection, request, beginJson(connection, request),
                client.scan(prefix, coverage),
                (resp, page) -> {
                    // How far the listing can be trusted goes in headers, not in the body: the
                    // body is a bare JSON array of keys for Consul compatibility, and a listing
                    // that cannot say it is short is the failure mode ?stale would otherwise
                    // introduce silently.
                    resp.header(COMPLETE_HEADER, page.quorumReached() ? "true" : "false")
                            .header(RESPONDED_HEADER, Integer.toString(page.respondedNodes()))
                            .header(CLUSTER_SIZE_HEADER, Integer.toString(page.clusterSize()));
                    final List<ScanResult> list = page.results();
                    final List<String> keys = new ArrayList<>(list.size());
                    for (int i = 0; i < list.size(); i++) {
                        keys.add(new String(
                                AgentSupport.toBytes(list.get(i).key()), StandardCharsets.UTF_8));
                    }
                    resp.setBodyUtf8(Json.arrayOfStrings(keys)).ok();
                });
    }

    private void handlePut(final Connection connection, final HttpRequest request) throws HttpErrorException {
        final String key = keyAfter(PREFIX);
        if (key.isEmpty()) {
            throw new HttpErrorException(400, "missing key");
        }
        requireKeyWithinLimit(key);
        final ByteBuffer value = bodyBuffer();
        final String casVersion = query().firstStringValue("cas");
        if (casVersion != null && !casVersion.isEmpty()) {
            handleCas(connection, request, key, value, casVersion);
            return;
        }
        if (query().contains("cas")) {
            // There is no value-compared CAS: its failure is unhandleable under ABA. Refusing is
            // the point -- silently promoting this to an unconditional put would apply a write
            // whose precondition the caller believed was being checked.
            throw new HttpErrorException(400,
                    "?cas needs a version: ?cas=<X-DisCas-Version value>, or ?cas=0 for "
                            + "create-if-absent. There is no value-compared form.");
        }
        // An unfenced write commits at a version like any other, so it hands one back: the writer
        // can fence its next write, or start watching, on what it just wrote without reading first.
        bridge(connection, request, beginJson(connection, request),
                client.put(key, value),
                (resp, at) -> resp.header(VERSION_HEADER, encodeVersion(at))
                        .setBodyUtf8(Json.object().field("ok", true).end()).ok());
    }

    /**
     * Version-fenced write: {@code ?cas=<version>}, where the value is a version this agent handed out
     * as {@code X-DisCas-Version}. Fencing on the version rather than on the value is what makes the
     * write safe when a coordinator stops answering, so this is the form to use for
     * read-modify-write over HTTP.
     * <p>
     * The reply carries the resulting {@code X-DisCas-Version} whether or not the swap took, so a
     * caller that lost the compare can recompute from the version that won without a second read.
     */
    private void handleCas(final Connection connection, final HttpRequest request,
                                    final String key, final ByteBuffer value,
                                    final String casVersion) throws HttpErrorException {
        final Version expected = decodeCasVersion(casVersion);
        bridge(connection, request, beginJson(connection, request),
                client.cas(key, expected, value),
                (resp, result) -> {
                    resp.header(VERSION_HEADER, encodeVersion(result.version()));
                    resp.setBodyUtf8(Json.object()
                            .field("swapped", result.swapped())
                            .field("value", result.value() == null ? null
                                    : AgentSupport.base64(
                                            AgentSupport.toBytes(result.value())))
                            .end()).ok();
                });
    }

    /**
     * The {@code ?cas=} value as a {@link Version}. Accepts {@code 0} as well as an
     * {@code X-DisCas-Version} version: {@code cas=0} is how Consul spells create-if-absent, and it is
     * unambiguous here because a version is hex and {@code "0"} is not a hex pair.
     */
    private static Version decodeCasVersion(final String casVersion) throws HttpErrorException {
        if ("0".equals(casVersion.trim())) {
            return Version.INITIAL;
        }
        return decodeVersion(casVersion);
    }

    private void handleDelete(final Connection connection, final HttpRequest request) throws HttpErrorException {
        final String key = keyAfter(PREFIX);
        if (key.isEmpty()) {
            throw new HttpErrorException(400, "missing key");
        }
        requireKeyWithinLimit(key);
        final String casVersion = query().firstStringValue("cas");
        if (casVersion != null && !casVersion.isEmpty()) {
            // The fenced delete, for the same reason PUT has one: an unfenced delete left
            // indeterminate can land after a later writer has recreated the key.
            final Version expected = decodeCasVersion(casVersion);
            bridge(connection, request, beginJson(connection, request),
                    client.delete(key, expected),
                    (resp, result) -> {
                        resp.header(VERSION_HEADER, encodeVersion(result.version()));
                        resp.setBodyUtf8(Json.object()
                                .field("swapped", result.swapped())
                                .field("value", result.value() == null ? null
                                        : AgentSupport.base64(
                                                AgentSupport.toBytes(result.value())))
                                .end()).ok();
                    });
            return;
        }
        // As with the unfenced put: a tombstone is a commit and carries the version it committed
        // at, which is where a watcher of the deleted key resumes from.
        bridge(connection, request, beginJson(connection, request),
                client.delete(key),
                (resp, at) -> resp.header(VERSION_HEADER, encodeVersion(at))
                        .setBodyUtf8(Json.object().field("ok", true).end()).ok());
    }
}
