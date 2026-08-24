/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.logging.LogThrottle;
import io.github.green4j.discas.common.operator.OperatorAttention;
import io.github.green4j.discas.common.operator.OperatorState;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;

/**
 * Writes the client's operator-facing events to a {@link Log}, then forwards them.
 * <p>
 * The same split as on the node, for the same reason. A handshake or configuration event means this
 * client is talking to something other than what its operator intended, so it goes through
 * {@link OperatorAttention} and arrives with what to do about it. The per-request points
 * ({@code sendFailed}, {@code requestFailedOver}) are counted and not logged: they are expected
 * during a failover, and a line each would turn a brief node outage into a flood.
 * <p>
 * This is the agent's whole operator surface as well as the library's: an embedded client's
 * operator is the application's, and a standalone agent has no observer of its own.
 */
public class LoggingClientObserver extends DelegatingClientObserver {

    private final Log log;
    private final OperatorAttention attention;

    /**
     * The one site here paced by a retry loop rather than by a person: a node answering SERVER_BUSY
     * refuses as fast as this client can ask, so the line reporting it is exactly as fast as the
     * saturation it describes.
     */
    private final LogThrottle helloThrottle = new LogThrottle();

    public LoggingClientObserver(final Log log, final OperatorAttention attention,
                                 final ClientObserver delegate) {
        super(delegate);
        this.log = log;
        this.attention = attention;
    }

    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        attention.raise(OperatorState.UNHANDLED_ERROR, context, context + " threw", error);
        super.eventLoopTaskFailed(context, error);
    }

    /**
     * {@code ACCESS_DENIED} is the only status here with an operator's action behind it.
     * {@code SERVER_BUSY} is the node shedding load and says so by retrying; a protocol mismatch is
     * a rollout that has not finished, which is the same condition the peer mesh reports.
     */
    @Override
    public void serverRejectedHello(final NodeId server, final ClientHelloRespStatus status) {
        final String detail = "the node refused CLIENT_HELLO, status=" + status;
        switch (status) {
            case ACCESS_DENIED:
                attention.raise(OperatorState.ACCESS_DENIED, server.value(), detail);
                break;
            case PROTOCOL_MISMATCH:
                attention.raise(OperatorState.VERSION_MISMATCH, server.value(), detail);
                break;
            default:
                helloThrottle.info(log, detail + "; transient, the client retries");
                break;
        }
        super.serverRejectedHello(server, status);
    }

    @Override
    public void serverReportedInvalidClusterSize(final NodeId server, final int reportedClusterSize) {
        attention.raise(OperatorState.CLUSTER_SIZE_INVALID, server.value(),
                "the node reported a cluster size of " + reportedClusterSize);
        super.serverReportedInvalidClusterSize(server, reportedClusterSize);
    }

    @Override
    public void clusterSizeDisagreement(final NodeId server, final int reportedClusterSize,
                                        final int knownClusterSize) {
        attention.raise(OperatorState.FOREIGN_CLUSTER, server.value(),
                "the node reported cluster size " + reportedClusterSize + " but "
                        + knownClusterSize + " is already known from another node");
        super.clusterSizeDisagreement(server, reportedClusterSize, knownClusterSize);
    }

    /**
     * Not a condition: being given a subset of the members is a deliberate configuration in every
     * deployment that does it, and it is safe for every operation but {@code scan}. It stays a
     * lifecycle line, so an operator who did not intend it can still find out.
     */
    @Override
    public void clusterSizeMismatch(final int configuredNodes, final int reportedClusterSize) {
        log.info("Configured with " + configuredNodes + " of " + reportedClusterSize
                + " cluster nodes; failover is limited to the configured subset");
        super.clusterSizeMismatch(configuredNodes, reportedClusterSize);
    }

    @Override
    public void transportAccountingUnderflow(final long estimatedBytes) {
        attention.raise(OperatorState.UNHANDLED_ERROR, "transport accounting",
                "byte accounting went negative (" + estimatedBytes + ") and was clamped;"
                        + " back-pressure is looser than configured");
        super.transportAccountingUnderflow(estimatedBytes);
    }

    @Override
    public void connectionLost(final NodeId peer, final int inFlight) {
        attention.raise(OperatorState.NODE_UNREACHABLE, peer.value(),
                "the connection dropped with " + inFlight + " request(s) in flight");
        super.connectionLost(peer, inFlight);
    }

    /**
     * A usable connection ends every reason this node was not one, so it clears them by scope
     * rather than by name -- the same shape as a completed peer handshake on the node.
     */
    @Override
    public void serverHandshakeCompleted(final NodeId peer) {
        attention.clearScope(peer.value());
        super.serverHandshakeCompleted(peer);
    }
}
