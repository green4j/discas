/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.client.DisCasClientConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fitting the client's deadlines inside the agent's HTTP request budget.
 * <p>
 * The defect this closes: the client's scan timeout and the agent's request timeout were both ten
 * seconds, so a scan waiting on a reachable-but-silent peer finished at the exact moment
 * {@code bridge()} gave up. The client's answer -- which for {@code ?stale} is a labelled partial
 * listing, a genuinely useful result -- was thrown away in favour of a {@code 504}. The two
 * deadlines cannot be equal; the client's has to be strictly shorter.
 */
@DisplayName("Agent -- client deadlines fit inside the request budget")
class AgentClientDeadlineTest {

    @Test
    @DisplayName("At the default budget the scan settles before the request times out")
    void scanSettlesBeforeTheDefaultBudget() {
        final Duration budget = Duration.ofSeconds(10);
        final DisCasClientConfig cfg = DisCasAgent.clientConfigFor(budget);

        assertTrue(cfg.scanTimeout().compareTo(budget) < 0,
                "The scan must settle strictly before the 504, but scanTimeout was "
                        + cfg.scanTimeout() + " against a budget of " + budget);
        // The default client scan timeout is 10s -- exactly the budget -- so this only holds
        // because the agent shortens it.
        assertEquals(Duration.ofSeconds(9), cfg.scanTimeout());
    }

    @Test
    @DisplayName("A shortened budget shortens both deadlines")
    void shortBudgetShortensBothDeadlines() {
        final Duration budget = Duration.ofSeconds(3);
        final DisCasClientConfig cfg = DisCasAgent.clientConfigFor(budget);

        assertEquals(Duration.ofSeconds(2), cfg.scanTimeout());
        // A single attempt must also fit: at the 5s default one attempt alone outlives a 3s budget,
        // so the request would always 504 before the client could fail over even once.
        assertEquals(Duration.ofSeconds(2), cfg.perAttemptTimeout());
    }

    @Test
    @DisplayName("A longer budget does not stretch the client past its own defaults")
    void longBudgetDoesNotInflateDefaults() {
        final DisCasClientConfig cfg = DisCasAgent.clientConfigFor(Duration.ofMinutes(5));
        final DisCasClientConfig defaults = DisCasClientConfig.defaults();

        // Only ever shrinks: a patient front-end is not a reason to make the client wait longer
        // than it would on its own.
        assertEquals(defaults.scanTimeout(), cfg.scanTimeout());
        assertEquals(defaults.perAttemptTimeout(), cfg.perAttemptTimeout());
    }

    @Test
    @DisplayName("An aggressively short budget still leaves a usable window")
    void tinyBudgetHitsTheFloor() {
        // Budget below the margin: the naive subtraction is zero or negative, which the config
        // rejects outright. The floor keeps the agent startable.
        final DisCasClientConfig cfg = DisCasAgent.clientConfigFor(Duration.ofMillis(200));

        assertTrue(cfg.scanTimeout().toMillis() > 0, "A zero-length deadline is not constructible");
        assertEquals(Duration.ofMillis(500), cfg.scanTimeout());
    }
}
