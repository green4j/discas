/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigSupport;

/**
 * The options every command that talks to a running cluster declares: which nodes, under which
 * identity, with which token.
 *
 * <p>Declared in one place so {@code dump} and {@code load} spell them identically -- an operator
 * who has learnt {@code -N} for one should not find it means something else in the other. The
 * <em>values</em> are still each command's own: this only adds them to a schema, and each command
 * resolves what it needs.
 *
 * <p>{@code init} does not use this, and should not be made to: it never opens a connection.
 */
final class ClusterOptions {

    /** Identity a command connects under when the operator does not name one. */
    static final String DEFAULT_CLIENT_ID = "discas-admin";

    private ClusterOptions() {
    }

    static GetOpts declare(final GetOpts opts) {
        return opts
                .stringOpt("nodes", 'N', ConfigSupport.helpWithEnv("nodes",
                        "Cluster nodes, id=host:port,id2=host:port,... Give the whole "
                                + "membership.")).metavar("<list>")
                .stringOpt("client-id", 'c', ConfigSupport.helpWithEnv("client-id",
                        "Client id presented to the nodes, which their authorization rules see "
                                + "[default: " + DEFAULT_CLIENT_ID + "]."))
                .stringOpt("token", null, ConfigSupport.helpWithEnv("token",
                        "Client authentication token, when the nodes require one [default: none]."))
                        .metavar("<secret>");
    }
}
