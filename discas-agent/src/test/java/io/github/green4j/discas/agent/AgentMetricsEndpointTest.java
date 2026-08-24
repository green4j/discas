/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.agent.starter.DisCasAgentConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The agent's metrics endpoint as a scraper sees it, and its separation from the KV surface.
 * <p>
 * No cluster is needed: the registry is populated by construction (the client's counters are
 * resolved when the observer chain is built), so an agent pointed at an unreachable node still
 * exposes a well-formed exposition -- which is exactly the case where a scrape matters most.
 */
@DisplayName("Agent -- metrics endpoint")
final class AgentMetricsEndpointTest {

    /** A node address nothing listens on: the agent must come up and serve metrics regardless. */
    private static final String UNREACHABLE_NODE = "1=127.0.0.1:1";

    @Test
    @DisplayName("/metrics serves the exposition on its own port, and the two surfaces stay apart")
    void metricsAreExposedOnTheirOwnPort() throws Exception {
        try (DisCasAgent agent = start()) {
            assertNotEquals(agent.port(), agent.metricsPort(),
                    "Metrics must not share the KV surface's port");

            final HttpResponse<String> response = get(agent.metricsPort(), "/metrics");

            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/plain"),
                    "A scraper content-negotiates on the exposition media type");
            final String body = response.body();
            assertTrue(body.contains("# TYPE discas_client_send_failures_total counter"), body);
            assertTrue(body.contains("discas_client_hellos_rejected_total"), body);

            assertEquals(404, get(agent.metricsPort(), "/v1/kv/anything").statusCode(),
                    "The metrics port is not a second way into the KV surface");
            assertEquals(404, get(agent.port(), "/metrics").statusCode(),
                    "Metrics live on their own port, so the KV surface must not answer for them");
        }
    }

    @Test
    @DisplayName("--observability-enabled=false starts no second listener")
    void disabledStartsNothing() throws Exception {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(new String[] {
                "--nodes", UNREACHABLE_NODE, "--http-bind", "127.0.0.1:0",
                "--observability-bind", "127.0.0.1:0", "--observability-enabled", "false"}, Map.of());
        try (DisCasAgent agent = DisCasAgent.start(cfg)) {
            assertEquals(-1, agent.metricsPort(), "Nothing bound, so there is no port to report");
        }
    }

    private static DisCasAgent start() throws Exception {
        return DisCasAgent.start(DisCasAgentConfig.resolve(new String[] {
                "--nodes", UNREACHABLE_NODE, "--http-bind", "127.0.0.1:0",
                "--observability-bind", "127.0.0.1:0"}, Map.of()));
    }

    private static HttpResponse<String> get(final int port, final String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
