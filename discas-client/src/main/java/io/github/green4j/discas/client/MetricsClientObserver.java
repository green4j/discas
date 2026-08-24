/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.metrics.Counter;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;

/**
 * Counts the client's events into a {@link MetricRegistry}, then forwards them.
 * <p>
 * Counters are resolved once in the constructor and held as fields, so the per-request points
 * ({@code sendFailed}, {@code requestFailedOver}) cost an increment and nothing else on the client's
 * event loop.
 * <p>
 * {@code requestFailedOver} carries the {@link ClientErrorCode} that caused the failover, and that
 * <em>is</em> used as a label: the enum is a closed set, so cardinality is bounded and knowing
 * <em>why</em> requests are moving between peers is most of the diagnostic value. The peer id is not
 * a label -- it would multiply the series by cluster size for little gain, since the interesting
 * question is the rate and the reason, not which peer.
 */
public class MetricsClientObserver extends DelegatingClientObserver {

    private final MetricRegistry registry;

    private final Counter eventLoopTaskFailures;
    private final Counter hellosRejected;
    private final Counter invalidClusterSizes;
    private final Counter clusterSizeDisagreements;
    private final Counter clusterSizeMismatches;
    private final Counter accountingUnderflows;
    private final Counter sendFailures;
    private final Counter scansIncomplete;
    private final Counter connectionsLost;
    private final Counter handshakesCompleted;
    /** Failover counters indexed by {@link ClientErrorCode#ordinal()}; filled lazily, see below. */
    private final Counter[] failovers = new Counter[ClientErrorCode.values().length];

    public MetricsClientObserver(final MetricRegistry registry, final ClientObserver delegate) {
        super(delegate);
        this.registry = registry;
        eventLoopTaskFailures = registry.counter("discas_client_event_loop_task_failures_total",
                "Tasks that threw on the client event loop.");
        hellosRejected = registry.counter("discas_client_hellos_rejected_total",
                "CLIENT_HELLO handshakes a server refused.");
        invalidClusterSizes = registry.counter("discas_client_invalid_cluster_sizes_total",
                "Servers that answered the handshake with a cluster size below 1.");
        clusterSizeDisagreements = registry.counter("discas_client_cluster_size_disagreements_total",
                "Servers reporting a cluster size different from the one already known.");
        clusterSizeMismatches = registry.counter("discas_client_cluster_size_mismatches_total",
                "Times the configured node count differed from the cluster's reported size.");
        accountingUnderflows = registry.counter("discas_client_transport_accounting_underflows_total",
                "Times the transport's estimated-bytes accounting went negative and was clamped.");
        sendFailures = registry.counter("discas_client_send_failures_total",
                "Request dispatches that threw, leaving that peer unable to answer.");
        scansIncomplete = registry.counter("discas_client_scans_incomplete_total",
                "ANY_AVAILABLE scans that returned below a majority.");
        // Neither of these was counted, because neither event reached this far: the delegating
        // decorator did not forward connectionLost, so the whole chain was blind to a node dropping
        // out. The pair matters more than either -- lost minus completed is how many are still gone.
        connectionsLost = registry.counter("discas_client_connections_lost_total",
                "Connections to a node that dropped unexpectedly, taking requests with them.");
        handshakesCompleted = registry.counter("discas_client_handshakes_completed_total",
                "CLIENT_HELLO handshakes a node accepted; a connection becoming usable.");
    }

    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        eventLoopTaskFailures.increment();
        super.eventLoopTaskFailed(context, error);
    }

    @Override
    public void serverRejectedHello(final NodeId server, final ClientHelloRespStatus status) {
        hellosRejected.increment();
        super.serverRejectedHello(server, status);
    }

    @Override
    public void serverReportedInvalidClusterSize(final NodeId server, final int reportedClusterSize) {
        invalidClusterSizes.increment();
        super.serverReportedInvalidClusterSize(server, reportedClusterSize);
    }

    @Override
    public void clusterSizeDisagreement(final NodeId server, final int reportedClusterSize,
                                        final int knownClusterSize) {
        clusterSizeDisagreements.increment();
        super.clusterSizeDisagreement(server, reportedClusterSize, knownClusterSize);
    }

    @Override
    public void clusterSizeMismatch(final int configuredNodes, final int reportedClusterSize) {
        clusterSizeMismatches.increment();
        super.clusterSizeMismatch(configuredNodes, reportedClusterSize);
    }

    @Override
    public void transportAccountingUnderflow(final long estimatedBytes) {
        accountingUnderflows.increment();
        super.transportAccountingUnderflow(estimatedBytes);
    }

    @Override
    public void sendFailed(final NodeId target, final Throwable error) {
        sendFailures.increment();
        super.sendFailed(target, error);
    }

    @Override
    public void requestFailedOver(final NodeId from, final ClientErrorCode code) {
        failoverCounter(code).increment();
        super.requestFailedOver(from, code);
    }

    @Override
    public void connectionLost(final NodeId peer, final int inFlight) {
        connectionsLost.increment();
        super.connectionLost(peer, inFlight);
    }

    @Override
    public void serverHandshakeCompleted(final NodeId peer) {
        handshakesCompleted.increment();
        super.serverHandshakeCompleted(peer);
    }

    @Override
    public void scanIncomplete(final int respondedNodes, final int clusterSize) {
        scansIncomplete.increment();
        super.scanIncomplete(respondedNodes, clusterSize);
    }

    /**
     * One counter per error code, resolved on first use and cached in the enum-indexed array.
     * <p>
     * Registering all of them up front would emit a series for every code a deployment never sees;
     * resolving lazily keeps the exposition to codes that actually occurred. The array is written
     * only from the client's event loop, which is also the only thread that calls this.
     */
    private Counter failoverCounter(final ClientErrorCode code) {
        final int ordinal = code.ordinal();
        Counter counter = failovers[ordinal];
        if (counter == null) {
            counter = registry.counter("discas_client_requests_failed_over_total",
                    "Requests re-dispatched to another peer after a server error.",
                    "code", code.name());
            failovers[ordinal] = counter;
        }
        return counter;
    }
}
