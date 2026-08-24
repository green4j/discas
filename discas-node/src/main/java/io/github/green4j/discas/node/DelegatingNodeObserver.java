/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerHelloRespStatus;

/**
 * Passes every event through to a wrapped observer, so a subclass can override the handful it cares
 * about and leave the rest alone.
 * <p>
 * This is what makes the observability backends composable rather than mutually exclusive. Without
 * it, a class that wants to both log and count either implements one concern and forfeits the other,
 * or grows into a single class doing both -- and then a deployment that wants counters but not
 * console output has nowhere to go. With it, each concern is one decorator and an operator picks the
 * chain:
 * <pre>
 * NodeObserver observer =
 *         new PeerStateObserver(clusterSize,
 *         new MetricsNodeObserver(registry,
 *         new LoggingNodeObserver(log, attention, NodeObserver.NONE)));
 * </pre>
 * <p>
 * Subclasses must call {@code super.x(...)} when overriding, or the rest of the chain stops seeing
 * that event. Each override here is a bare forward, so a decorator that overrides nothing costs one
 * virtual call per event.
 */
public abstract class DelegatingNodeObserver implements NodeObserver {

    private final NodeObserver delegate;

    protected DelegatingNodeObserver(final NodeObserver delegate) {
        this.delegate = delegate == null ? NodeObserver.NONE : delegate;
    }

    /** The next observer in the chain, for a subclass that needs to forward conditionally. */
    protected final NodeObserver delegate() {
        return delegate;
    }

    @Override
    public void nodeState(final NodeState from, final NodeState to,
                          final String detail, final Throwable cause) {
        delegate.nodeState(from, to, detail, cause);
    }

    @Override
    public void requestBeforeReady(final boolean peerMessage) {
        delegate.requestBeforeReady(peerMessage);
    }

    @Override
    public void snapshotStarted() {
        delegate.snapshotStarted();
    }

    @Override
    public void snapshotCompleted() {
        delegate.snapshotCompleted();
    }

    @Override
    public void snapshotFailed(final Throwable error, final boolean committed) {
        delegate.snapshotFailed(error, committed);
    }

    @Override
    public void walDegraded(final String reason) {
        delegate.walDegraded(reason);
    }

    @Override
    public void lifecycleResourceCloseFailed(final Throwable error) {
        delegate.lifecycleResourceCloseFailed(error);
    }

    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        delegate.eventLoopTaskFailed(context, error);
    }

    @Override
    public void roundCommitted(final HashedBytes key, final Ballot ballot) {
        delegate.roundCommitted(key, ballot);
    }

    @Override
    public void roundFailed(final HashedBytes key, final String reason) {
        delegate.roundFailed(key, reason);
    }

    @Override
    public void roundRefusedNoMajority(final HashedBytes key) {
        delegate.roundRefusedNoMajority(key);
    }

    @Override
    public void prepareRejected(final Ballot promised) {
        delegate.prepareRejected(promised);
    }

    @Override
    public void acceptRejected(final Ballot promised) {
        delegate.acceptRejected(promised);
    }

    @Override
    public void externalBallotObserved(final long counter) {
        delegate.externalBallotObserved(counter);
    }

    @Override
    public void serializableRead(final HashedBytes key) {
        delegate.serializableRead(key);
    }

    @Override
    public void prepareHandlerFailed(final HashedBytes key, final Throwable error) {
        delegate.prepareHandlerFailed(key, error);
    }

    @Override
    public void acceptHandlerFailed(final HashedBytes key, final Throwable error) {
        delegate.acceptHandlerFailed(key, error);
    }

    @Override
    public void scanFailed(final Throwable error) {
        delegate.scanFailed(error);
    }

    @Override
    public void ceilingRequestFailed(final NodeId peer, final Throwable error) {
        delegate.ceilingRequestFailed(peer, error);
    }

    @Override
    public void quorumAvailability(final boolean available, final int handshaked) {
        delegate.quorumAvailability(available, handshaked);
    }

    @Override
    public void peerIncarnationChanged(final NodeId peer, final String previousIncarnation,
                                       final String currentIncarnation) {
        delegate.peerIncarnationChanged(peer, previousIncarnation, currentIncarnation);
    }

    @Override
    public void repairCycleStarted() {
        delegate.repairCycleStarted();
    }

    @Override
    public void keyRepaired(final HashedBytes key) {
        delegate.keyRepaired(key);
    }

    @Override
    public void tombstoneSwept(final TombstoneSweep sweep) {
        delegate.tombstoneSwept(sweep);
    }

    @Override
    public void unaccountedKeyDropped(final HashedBytes key, final Ballot accepted) {
        delegate.unaccountedKeyDropped(key, accepted);
    }

    @Override
    public void digestRequestFailed(final NodeId peer, final Throwable error) {
        delegate.digestRequestFailed(peer, error);
    }

    @Override
    public void keysRequestFailed(final NodeId peer, final Throwable error) {
        delegate.keysRequestFailed(peer, error);
    }

    @Override
    public void writeRefusedNoCapacity(final HashedBytes key, final boolean coordinator) {
        delegate.writeRefusedNoCapacity(key, coordinator);
    }

    @Override
    public void storeCapacity(final boolean available, final long storedBytes,
                              final long capacityBytes) {
        delegate.storeCapacity(available, storedBytes, capacityBytes);
    }

    @Override
    public void membersReloadRejected(final String reason) {
        delegate.membersReloadRejected(reason);
    }

    @Override
    public void membersReloadAccepted(final int members) {
        delegate.membersReloadAccepted(members);
    }

    @Override
    public void peerHandshakeRejected(final NodeId peer, final PeerHelloRespStatus status,
                                      final String cause) {
        delegate.peerHandshakeRejected(peer, status, cause);
    }

    @Override
    public void peerHandshakeCompleted(final NodeId peer) {
        delegate.peerHandshakeCompleted(peer);
    }

    @Override
    public void peerDisconnected(final NodeId peer, final String reason) {
        delegate.peerDisconnected(peer, reason);
    }
}
