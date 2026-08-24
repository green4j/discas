/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.common.identity.NodeId;

/**
 * A {@link ClientObserver} that writes diagnostics to {@code System.err}, for an embedding
 * application that wants them with a single wiring change; the default {@link ClientObserver#NONE}
 * stays silent. Only events worth a line are overridden -- pure metric points such as failover and
 * scan coverage stay no-ops, since a line per request is noise rather than a diagnostic.
 */
public class StderrClientObserver implements ClientObserver {

    /** Shared instance; the observer is stateless. */
    public static final StderrClientObserver INSTANCE = new StderrClientObserver();

    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        System.err.println(context + " error: " + error.getMessage());
        error.printStackTrace();
    }

    @Override
    public void serverRejectedHello(final NodeId server, final ClientHelloRespStatus status) {
        System.err.println("Server " + server + " rejected CLIENT_HELLO with status "
                + status + "; closing");
    }

    @Override
    public void serverReportedInvalidClusterSize(final NodeId server, final int reportedClusterSize) {
        System.err.println("Server " + server + " reported an invalid cluster size "
                + reportedClusterSize + "; closing");
    }

    @Override
    public void clusterSizeDisagreement(final NodeId server, final int reportedClusterSize,
                                        final int knownClusterSize) {
        System.err.println("Server " + server + " reported cluster size " + reportedClusterSize
                + " but " + knownClusterSize + " was already learned from another node; closing");
    }

    @Override
    public void transportAccountingUnderflow(final long estimatedBytes) {
        System.err.println("WARN: estimatedTransportBytes went negative ("
                + estimatedBytes + "), clamping to 0");
    }

    @Override
    public void clusterSizeMismatch(final int configuredNodes, final int reportedClusterSize) {
        System.err.println("WARN: client is configured with " + configuredNodes
                + " node(s) but the cluster reports " + reportedClusterSize
                + "; give the client the full membership. Reads and writes still work,"
                + " but failover is limited to the configured nodes"
                + (configuredNodes < reportedClusterSize / 2 + 1
                    ? " and scan cannot reach a quorum of " + (reportedClusterSize / 2 + 1) + "."
                    : "."));
    }
}
