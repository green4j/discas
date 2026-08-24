/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.logging;

/**
 * The two levels discas emits. A diagnostic is either something an operator must act on
 * ({@link #ERROR}) or a lifecycle fact worth a line in the record ({@link #INFO}); anything
 * finer-grained is a metric, and lives in
 * {@link io.github.green4j.discas.common.metrics.MetricRegistry} where it can be aggregated instead
 * of grepped. With only two levels there is also no level check on the logging path.
 */
public enum LogLevel {

    /** A lifecycle fact: recovery finished, a peer came up, configuration reloaded. */
    INFO("INFO "),

    /** Something an operator should look at: a failed round handler, a degraded WAL, a rejected peer. */
    ERROR("ERROR");

    private final String padded;

    LogLevel(final String padded) {
        this.padded = padded;
    }

    /**
     * The level name padded to a fixed width, so the message column lines up across records without
     * the formatter having to pad at write time.
     */
    public String padded() {
        return padded;
    }
}
