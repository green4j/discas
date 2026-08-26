/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.observability;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.io.ReloadReport;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.observability.ObservabilityConfig;
import io.github.green4j.discas.common.observability.ObservabilityServer;
import io.github.green4j.discas.node.MetricsNodeObserver;
import io.github.green4j.discas.node.NodeState;
import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.PeerStateObserver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The endpoints as a scraper and an orchestrator actually see them.
 * <p>
 * Driven through the observer seam rather than a live cluster: {@code peerHandshakeCompleted} and
 * {@code peerDisconnected} are exactly what the transports emit, so feeding them directly reproduces
 * a partition -- peers going away and coming back -- without the flakiness of arranging real socket
 * failures, and pins the part that matters, which is the status code an orchestrator will act on.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("Observability endpoints -- health, readiness and metrics")
class ObservabilityEndpointsTest {

    private static final NodeId SELF = NodeId.of("n1");

    private MutableHealth health;
    private PeerStateObserver peerState;
    private ObservabilityServer server;
    private HttpClient http;
    /** What {@code /reload} returns; a test sets it to the outcome it wants to see served. */
    private volatile ReloadReport report = new ReloadReport(List.of());

    @BeforeEach
    void setUp() throws IOException {
        health = new MutableHealth();
        final MetricRegistry registry = new MetricRegistry();
        peerState = new PeerStateObserver(3, new MetricsNodeObserver(registry, NodeObserver.NONE));
        peerState.registerMetrics(registry);
        server = ObservabilityServer.start(
                ObservabilityConfig.builder().bindAddress("127.0.0.1").port(0).build(),
                NodeEndpoints.router(SELF, health, peerState, registry, () -> report));
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    @DisplayName("Disabled config starts nothing at all")
    void disabledStartsNothing() throws IOException {
        final MetricRegistry registry = new MetricRegistry();
        assertEquals(null, ObservabilityServer.start(
                ObservabilityConfig.builder().enabled(false).build(),
                NodeEndpoints.router(SELF, health, peerState, registry, () -> report)));
    }

    @Test
    @DisplayName("/metrics serves the exposition format with the node's series")
    void metricsAreExposed() throws Exception {
        health.state = NodeState.SERVING;
        peerState.peerHandshakeCompleted(NodeId.of("n2"));

        final HttpResponse<String> response = get("/metrics");

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/plain"),
                "A scraper content-negotiates on the exposition media type");
        final String body = response.body();
        assertTrue(body.contains("# TYPE discas_node_peers_handshaked gauge"), body);
        assertTrue(body.contains("discas_node_peers_handshaked 1\n"), body);
        assertTrue(body.contains("discas_node_peer_handshaked{peer=\"n2\"} 1\n"), body);
        assertTrue(body.contains("# TYPE discas_node_rounds_committed_total counter"), body);
    }

    @Test
    @DisplayName("/health is 503 before recovery and 200 after, regardless of peers")
    void livenessTracksRecoveryOnly() throws Exception {
        assertEquals(503, get("/health").statusCode(), "A node that has not recovered is not live");

        health.state = NodeState.SERVING;
        final HttpResponse<String> live = get("/health");
        assertEquals(200, live.statusCode());
        assertTrue(live.body().contains("\"status\":\"ok\""), live.body());
        assertTrue(live.body().contains("\"state\":\"SERVING\""), live.body());
        // No peers have handshaked, so there is no quorum -- and liveness must not care. Wiring a
        // livenessProbe to a quorum-aware endpoint turns a partition into a crashloop.
        assertTrue(get("/ready").statusCode() == 503, "Precondition: this node has no quorum");
        assertEquals(200, get("/health").statusCode(), "Liveness must be independent of quorum");
    }

    @Test
    @DisplayName("/health reports 503 when the WAL degrades")
    void livenessFailsOnDegradedWal() throws Exception {
        health.state = NodeState.SERVING;
        health.degraded = true;
        health.reason = "disk full";

        final HttpResponse<String> response = get("/health");
        assertEquals(503, response.statusCode());
        // The status and the flag are the contract; the reason beside them is operator prose the
        // test itself planted five lines up.
        assertTrue(response.body().contains("\"walDegraded\":true"), response.body());
    }

    @Test
    @DisplayName("/ready follows quorum as peers come and go")
    void readinessFollowsQuorum() throws Exception {
        health.state = NodeState.SERVING;

        assertEquals(503, get("/ready").statusCode(), "Recovered but alone in a cluster of 3");

        peerState.peerHandshakeCompleted(NodeId.of("n2"));
        final HttpResponse<String> ready = get("/ready");
        assertEquals(200, ready.statusCode());
        assertTrue(ready.body().contains("\"status\":\"ready\""), ready.body());
        assertTrue(ready.body().contains("\"peersHandshaked\":1"), ready.body());
        assertTrue(ready.body().contains("\"quorumAvailable\":true"), ready.body());

        // The peer goes away, as it would in a partition.
        peerState.peerDisconnected(NodeId.of("n2"), "connection closed");
        final HttpResponse<String> lost = get("/ready");
        assertEquals(503, lost.statusCode());
        assertTrue(lost.body().contains("\"status\":\"not-ready\""), lost.body());
        // ...but the node can still answer stale reads, and says so rather than leaving the caller
        // to infer it from a bare 503.
        assertTrue(lost.body().contains("\"canServeStaleReads\":true"), lost.body());
        assertEquals(200, get("/health").statusCode(), "A partitioned node is drained, not dead");

        // Healing restores it.
        peerState.peerHandshakeCompleted(NodeId.of("n2"));
        assertEquals(200, get("/ready").statusCode());
    }

    @Test
    @DisplayName("/ready carries per-peer state and transition history")
    void readinessBodyCarriesPeerDetail() throws Exception {
        health.state = NodeState.SERVING;
        peerState.peerHandshakeCompleted(NodeId.of("n2"));
        peerState.peerDisconnected(NodeId.of("n2"), "closed");
        peerState.peerHandshakeCompleted(NodeId.of("n2"));

        final String body = get("/ready").body();
        assertTrue(body.contains("\"id\":\"n2\""), body);
        assertTrue(body.contains("\"handshaked\":true"), body);
        assertTrue(body.contains("\"ups\":2"), body);
        assertTrue(body.contains("\"downs\":1"), body);
    }

    @Test
    @DisplayName("/reload reports what each source did, and 200 when the set went in")
    void reloadReportsWhatWasApplied() throws Exception {
        report = new ReloadReport(List.of(
                new ReloadReport.Entry("/etc/discas/members.properties",
                        ReloadReport.Outcome.APPLIED, "3 members"),
                new ReloadReport.Entry("/etc/discas/acl.properties",
                        ReloadReport.Outcome.UNCHANGED, "byte-identical")));

        final HttpResponse<String> response = post("/reload");

        assertEquals(200, response.statusCode(), "Applied and unchanged are both acceptances");
        final String body = response.body();
        assertTrue(body.contains("\"status\":\"applied\""), body);
        assertTrue(body.contains("\"source\":\"/etc/discas/members.properties\""), body);
        assertTrue(body.contains("\"outcome\":\"applied\""), body);
        assertTrue(body.contains("\"outcome\":\"unchanged\""), body);
    }

    @Test
    @DisplayName("/reload is a 400 when one source was refused: nothing was applied")
    void reloadRefusedIsAClientError() throws Exception {
        // A 4xx rather than a 503 because retrying without editing the file changes nothing, and
        // an operator reading the entry can tell which file to go and fix.
        report = new ReloadReport(List.of(
                new ReloadReport.Entry("/etc/discas/acl.properties",
                        ReloadReport.Outcome.FAILED, "unknown operation 'writ'"),
                new ReloadReport.Entry("/etc/discas/members.properties",
                        ReloadReport.Outcome.NOT_APPLIED, "ready, and held back")));

        final HttpResponse<String> response = post("/reload");

        assertEquals(400, response.statusCode());
        final String body = response.body();
        assertTrue(body.contains("\"status\":\"rejected\""), body);
        assertTrue(body.contains("\"outcome\":\"failed\""), body);
        assertTrue(body.contains("\"outcome\":\"not-applied\""), body);
        assertTrue(body.contains("unknown operation 'writ'"), body);
    }

    @Test
    @DisplayName("A method other than the endpoint's own is refused")
    void theWrongMethodIsRefused() throws Exception {
        assertEquals(405, post("/metrics").statusCode());
        assertEquals(405, post("/health").statusCode());
        assertEquals(405, post("/ready").statusCode());
        // ...and the other way round for the one endpoint that changes something: a GET that
        // reloads is a GET something will eventually retry, prefetch or cache.
        assertEquals(405, get("/reload").statusCode());
    }

    @Test
    @DisplayName("An unrouted path is a 404")
    void unknownPathIs404() throws Exception {
        assertEquals(404, get("/nope").statusCode());
    }

    private HttpResponse<String> get(final String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(final String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.ofString("x")).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(final String path) {
        return URI.create("http://127.0.0.1:" + server.port() + path);
    }

    /** A {@link HealthSource} a test can move, standing in for the node's own state. */
    private static final class MutableHealth implements HealthSource {
        private volatile NodeState state = NodeState.REPLAYING;
        private volatile boolean degraded;
        private volatile String reason;

        @Override
        public NodeState state() {
            return state;
        }

        @Override
        public boolean walDegraded() {
            return degraded;
        }

        @Override
        public String walDegradedReason() {
            return reason;
        }
    }
}
