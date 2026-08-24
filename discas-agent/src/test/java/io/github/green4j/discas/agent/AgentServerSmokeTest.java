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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Server/router smoke test with no cluster behind it: /v1/agent/health answers (needs no cluster),
 * and an unknown path 404s. Isolates the HTTP wiring from cluster readiness.
 */
@DisplayName("DisCasAgent -- HTTP surface smoke test")
final class AgentServerSmokeTest {

    @Test
    void healthAndFallback() throws Exception {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--nodes", "1=127.0.0.1:1", "--http-bind", "127.0.0.1:0",
                        "--observability-bind", "127.0.0.1:0"}, Map.of());
        try (DisCasAgent agent = DisCasAgent.start(cfg)) {
            final String base = "http://127.0.0.1:" + agent.port();
            final HttpClient http = HttpClient.newHttpClient();

            final HttpResponse<String> health = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/v1/agent/health")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"status\":\"ok\""), health.body());

            final HttpResponse<String> missing = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/nope")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(404, missing.statusCode());

            // Deferred/async path with an unreachable node: must return a JSON error status,
            // not close the connection. (500 all-peers-exhausted, or 504 timeout.)
            final HttpResponse<String> kv = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/v1/kv/foo")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertTrue(kv.statusCode() >= 500, "status=" + kv.statusCode() + " body=" + kv.body());
            assertTrue(kv.body().contains("\"error\":"), kv.body());

            // Deferred PUT carrying a request body (the shape the integration test starts with).
            final HttpResponse<String> putResp = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/v1/kv/foo"))
                            .PUT(HttpRequest.BodyPublishers.ofString("1")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertTrue(putResp.statusCode() >= 500,
                    "status=" + putResp.statusCode() + " body=" + putResp.body());
        }
    }
}
