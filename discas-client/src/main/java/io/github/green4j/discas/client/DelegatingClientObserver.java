/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;

/**
 * Passes every client event through to a wrapped observer, so a subclass can override the handful it
 * cares about and leave the rest alone. The client-side counterpart of
 * {@code DelegatingNodeObserver}, and the reason logging and metrics can both be attached to one
 * client instead of being mutually exclusive.
 * <p>
 * Subclasses must call {@code super.x(...)} when overriding, or the rest of the chain stops seeing
 * that event.
 */
public abstract class DelegatingClientObserver implements ClientObserver {

    private final ClientObserver delegate;

    protected DelegatingClientObserver(final ClientObserver delegate) {
        this.delegate = delegate == null ? ClientObserver.NONE : delegate;
    }

    /** The next observer in the chain, for a subclass that needs to forward conditionally. */
    protected final ClientObserver delegate() {
        return delegate;
    }

    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        delegate.eventLoopTaskFailed(context, error);
    }

    @Override
    public void serverRejectedHello(final NodeId server, final ClientHelloRespStatus status) {
        delegate.serverRejectedHello(server, status);
    }

    @Override
    public void serverReportedInvalidClusterSize(final NodeId server, final int reportedClusterSize) {
        delegate.serverReportedInvalidClusterSize(server, reportedClusterSize);
    }

    @Override
    public void clusterSizeDisagreement(final NodeId server, final int reportedClusterSize,
                                        final int knownClusterSize) {
        delegate.clusterSizeDisagreement(server, reportedClusterSize, knownClusterSize);
    }

    @Override
    public void clusterSizeMismatch(final int configuredNodes, final int reportedClusterSize) {
        delegate.clusterSizeMismatch(configuredNodes, reportedClusterSize);
    }

    @Override
    public void transportAccountingUnderflow(final long estimatedBytes) {
        delegate.transportAccountingUnderflow(estimatedBytes);
    }

    @Override
    public void sendFailed(final NodeId target, final Throwable error) {
        delegate.sendFailed(target, error);
    }

    @Override
    public void requestFailedOver(final NodeId from, final ClientErrorCode code) {
        delegate.requestFailedOver(from, code);
    }

    @Override
    public void scanIncomplete(final int respondedNodes, final int clusterSize) {
        delegate.scanIncomplete(respondedNodes, clusterSize);
    }

    // The four below were missing, and missing here is not the same as unimplemented: a decorator
    // that does not forward an event ends it, because the interface default that runs instead is a
    // no-op that never reaches the delegate. So the whole chain was blind to a connection dropping
    // and to every clock reading -- not the outermost link's choice, but nobody's.

    @Override
    public void connectionLost(final NodeId peer, final int inFlight) {
        delegate.connectionLost(peer, inFlight);
    }

    @Override
    public void serverHandshakeCompleted(final NodeId peer) {
        delegate.serverHandshakeCompleted(peer);
    }

    @Override
    public void clockOffsetMeasured(final NodeId node, final long offsetMillis,
                                    final long roundTripMillis) {
        delegate.clockOffsetMeasured(node, offsetMillis, roundTripMillis);
    }

    @Override
    public void clientClockStepped(final long stepMillis) {
        delegate.clientClockStepped(stepMillis);
    }
}
