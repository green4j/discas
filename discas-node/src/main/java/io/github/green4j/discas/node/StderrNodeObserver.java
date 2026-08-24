/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerHelloRespStatus;

/**
 * A {@link NodeObserver} that writes diagnostics to {@code System.err}, for a deployment that wants
 * them with a single wiring change; the default {@link NodeObserver#NONE} stays silent. Only events
 * worth a line are overridden -- pure metric points, such as round outcomes and serializable reads,
 * stay no-ops.
 */
public class StderrNodeObserver implements NodeObserver {

    private final NodeId nodeId;

    public StderrNodeObserver(final NodeId nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void nodeState(final NodeState from, final NodeState to,
                          final String detail, final Throwable cause) {
        System.err.println("Node " + nodeId + " " + from + " -> " + to
                + (detail == null ? "" : " (" + detail + ")"));
        if (cause != null) {
            cause.printStackTrace();
        }
    }

    @Override
    public void snapshotFailed(final Throwable error, final boolean committed) {
        System.err.println("Snapshot write failed: " + error.getMessage());
        error.printStackTrace();
    }

    @Override
    public void walDegraded(final String reason) {
        System.err.println("WAL marked degraded: " + reason);
    }

    @Override
    public void eventLoopTaskFailed(final String context, final Throwable error) {
        System.err.println(context + " error: " + error.getMessage());
        error.printStackTrace();
    }

    @Override
    public void lifecycleResourceCloseFailed(final Throwable error) {
        System.err.println("Lifecycle resource close failed: " + error);
    }

    @Override
    public void prepareHandlerFailed(final HashedBytes key, final Throwable error) {
        System.err.println("Prepare handler failed for key " + key + ": " + error.getMessage());
    }

    @Override
    public void acceptHandlerFailed(final HashedBytes key, final Throwable error) {
        System.err.println("Accept handler failed for key " + key + ": " + error.getMessage());
    }

    @Override
    public void scanFailed(final Throwable error) {
        System.err.println("handleScan failed: " + error.getMessage());
    }

    @Override
    public void digestRequestFailed(final NodeId peer, final Throwable error) {
        System.err.println("handleDigestReq failed from peer " + peer + ": " + error.getMessage());
    }

    @Override
    public void keysRequestFailed(final NodeId peer, final Throwable error) {
        System.err.println("handleKeysReq failed from peer " + peer + ": " + error.getMessage());
    }

    @Override
    public void membersReloadRejected(final String reason) {
        System.err.println("Members reload rejected: " + reason
                + " -- ignoring the update (restart to change N; quorum is frozen at startup).");
    }

    @Override
    public void peerHandshakeRejected(final NodeId peer, final PeerHelloRespStatus status,
                                      final String cause) {
        System.err.println("Peer " + peer + " rejected/mismatched PEER_HELLO with status " + status
                + (cause == null || cause.isEmpty() ? "" : " (" + cause + ")") + "; closing");
    }
}
