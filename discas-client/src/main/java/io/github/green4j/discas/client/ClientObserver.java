/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.common.identity.NodeId;

/**
 * The one seam every handshake, failover and coverage event a client produces is reported through,
 * so a logging or metrics backend attaches without touching protocol code.
 * <p>
 * All methods are {@code default} no-ops: an implementation overrides only the events it cares
 * about, {@link #NONE} reports nothing, and {@link StderrClientObserver} prints to
 * {@code System.err}.
 * <p>
 * <b>Threading:</b> every callback fires on the client's {@code EventLoop} thread, so
 * implementations must not block. Several of these are per-request points
 * ({@link #sendFailed}, {@link #requestFailedOver}); implementations must avoid allocation and
 * string building in the body -- the arguments are passed as raw handles precisely so a no-op
 * default costs nothing.
 * <p>
 * This seam covers the client itself. Shared infrastructure that the client happens to use --
 * file watching, token stores, certificate rotation -- is used by nodes too and is not reported
 * here.
 */
public interface ClientObserver {

    /** No-op observer: the default. Reports nothing, allocates nothing. */
    ClientObserver NONE = new ClientObserver() {
    };

    /**
     * A task submitted to the client's event loop threw. The loop keeps running; {@code context}
     * names the phase ({@code "event loop"}, {@code "shutdown drain"}, {@code "timer task"}).
     */
    default void eventLoopTaskFailed(final String context, final Throwable error) {
    }

    /** A server refused CLIENT_HELLO; the connection is being closed. */
    default void serverRejectedHello(final NodeId server, final ClientHelloRespStatus status) {
    }

    /** A server answered the handshake with a cluster size below 1; the connection is refused. */
    default void serverReportedInvalidClusterSize(final NodeId server, final int reportedClusterSize) {
    }

    /**
     * Two servers reported different cluster sizes, which means this client is pointed at two
     * different clusters. The odd one out is refused rather than allowed to corrupt scan quorum.
     */
    default void clusterSizeDisagreement(final NodeId server, final int reportedClusterSize,
                                         final int knownClusterSize) {
    }

    /**
     * The client is configured with a different number of nodes than the cluster reports. Not
     * fatal -- the subset bootstrap is deliberate -- but failover is limited to the configured
     * nodes, and below a majority a {@link ScanCoverage#QUORUM} scan cannot succeed at all.
     * Reported once per transport, not once per reconnect.
     */
    default void clusterSizeMismatch(final int configuredNodes, final int reportedClusterSize) {
    }

    /**
     * The transport's estimated-bytes accounting went negative and was clamped to zero. An
     * internal invariant violation rather than an operational condition: the estimate feeds the
     * transport byte budget, so a drift here loosens back-pressure.
     */
    default void transportAccountingUnderflow(final long estimatedBytes) {
    }

    /**
     * Dispatching a request to {@code target} threw, so that peer will never answer it. The
     * request itself is retried elsewhere (or failed once the peers are exhausted); this reports
     * only the unreachable peer, which is otherwise invisible to the caller.
     */
    default void sendFailed(final NodeId target, final Throwable error) {
    }

    /**
     * A server answered {@code code} and the request was immediately re-dispatched to the next
     * peer instead of waiting out the per-attempt timeout. A rising rate here means peers are
     * spending time unable to serve.
     */
    default void requestFailedOver(final NodeId from, final ClientErrorCode code) {
    }

    /**
     * The connection to {@code peer} dropped unexpectedly, taking {@code inFlight} requests with
     * it. Those are re-dispatched elsewhere rather than waiting out their per-attempt timeouts.
     * <p>
     * Not reported for connections closed as part of shutting the client down.
     */
    default void connectionLost(final NodeId peer, final int inFlight) {
    }

    /**
     * {@code peer}'s CLIENT_HELLO was accepted and the connection is usable for requests.
     * <p>
     * The pair to {@link #connectionLost}, and the reason it exists: without it "this node is
     * unreachable" is a condition nothing can end. It also ends the refusals above -- a hello that
     * is now accepted answers whichever of them was raised.
     */
    default void serverHandshakeCompleted(final NodeId peer) {
    }

    /**
     * A {@link ScanCoverage#ANY_AVAILABLE} scan returned below a majority. The listing may omit
     * committed keys; the caller opted into that, but the shortfall is worth counting.
     */
    default void scanIncomplete(final int respondedNodes, final int clusterSize) {
    }

    /**
     * A handshake reported how far this client's clock sits from a coordinator's, in milliseconds
     * (positive: the cluster is ahead of us). The correction is applied to lock leases either way;
     * this is the number that says whether it was worth applying, which nothing could answer before
     * -- the clocks were never compared.
     *
     * @param roundTripMillis the handshake round trip the reading was taken across; the offset
     *                        cannot be more accurate than half of it, so a large one devalues a
     *                        small offset
     */
    default void clockOffsetMeasured(final NodeId node, final long offsetMillis,
                                     final long roundTripMillis) {
    }

    /**
     * This client's own wall clock stepped: it moved by {@code stepMillis} more (or less) than the
     * monotonic clock did over the same interval. The offset measured before the step described a
     * relationship that no longer holds, so it has been dropped and the next handshake re-measures.
     * <p>
     * Worth surfacing rather than absorbing silently: between the step and the next handshake, lock
     * leases are being written and judged on an uncorrected clock.
     */
    default void clientClockStepped(final long stepMillis) {
    }
}
