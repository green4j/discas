/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.metrics.PrometheusTextFormat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The quorum arithmetic behind the readiness probe.
 * <p>
 * These drive the state machine through its transitions rather than asserting that any callback
 * fired -- the point is not that the observer was called, it is that {@code quorumAvailable()} flips
 * at exactly the right threshold, because that boolean decides whether a load balancer keeps sending
 * traffic to a node that can no longer commit anything.
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES)
@DisplayName("PeerStateObserver -- quorum tracking")
class PeerStateObserverTest {

    private static NodeId peer(final int id) {
        return NodeId.of("n" + id);
    }

    private static PeerStateObserver observer(final int clusterSize) {
        return new PeerStateObserver(clusterSize, NodeObserver.NONE);
    }

    @Test
    @DisplayName("A single-node cluster has a quorum with no peers at all")
    void singleNodeClusterIsAlwaysAvailable() {
        final PeerStateObserver state = observer(1);
        assertEquals(1, state.quorumSize());
        assertTrue(state.quorumAvailable(), "A node is always part of its own quorum");
    }

    @Test
    @DisplayName("N=3 needs one handshaked peer, and loses quorum when it goes")
    void threeNodeThreshold() {
        final PeerStateObserver state = observer(3);
        assertEquals(2, state.quorumSize());
        assertFalse(state.quorumAvailable(), "Alone, a node of 3 cannot reach a majority");

        state.peerHandshakeCompleted(peer(2));
        assertTrue(state.quorumAvailable(), "Self plus one of three is a majority");

        state.peerHandshakeCompleted(peer(3));
        assertTrue(state.quorumAvailable());

        state.peerDisconnected(peer(3), "closed");
        assertTrue(state.quorumAvailable(), "Still self plus one");

        state.peerDisconnected(peer(2), "closed");
        assertFalse(state.quorumAvailable(), "The last peer leaving takes the quorum with it");
    }

    @Test
    @DisplayName("N=5 needs two handshaked peers")
    void fiveNodeThreshold() {
        final PeerStateObserver state = observer(5);
        assertEquals(3, state.quorumSize());

        state.peerHandshakeCompleted(peer(2));
        assertFalse(state.quorumAvailable(), "Self plus one of five is not a majority");

        state.peerHandshakeCompleted(peer(3));
        assertTrue(state.quorumAvailable(), "Self plus two of five is");

        state.peerDisconnected(peer(2), "closed");
        assertFalse(state.quorumAvailable());
    }

    @Test
    @DisplayName("A repeated handshake for a live peer is not a second peer")
    void duplicateHandshakeIsIdempotent() {
        final PeerStateObserver state = observer(5);
        state.peerHandshakeCompleted(peer(2));
        state.peerHandshakeCompleted(peer(2));
        state.peerHandshakeCompleted(peer(2));

        assertEquals(1, state.handshakedPeerCount(),
                "Both ends of the single-dialer model can report the same peer; counting it twice "
                        + "would manufacture a quorum that does not exist");
        assertFalse(state.quorumAvailable());
        assertEquals(1, state.peerSnapshots().get(0).ups, "A repeat is not a new transition");
    }

    @Test
    @DisplayName("A disconnect for a peer never seen up is ignored and creates no entry")
    void disconnectForUnknownPeerIsANoOp() {
        final PeerStateObserver state = observer(3);
        state.peerDisconnected(peer(2), "closed");

        assertEquals(0, state.handshakedPeerCount());
        assertTrue(state.peerSnapshots().isEmpty(),
                "A peer that only ever failed its handshake must not appear as though it had been up");
    }

    @Test
    @DisplayName("A repeated disconnect does not drive the count negative")
    void duplicateDisconnectIsIdempotent() {
        final PeerStateObserver state = observer(3);
        state.peerHandshakeCompleted(peer(2));
        state.peerDisconnected(peer(2), "closed");
        state.peerDisconnected(peer(2), "closed again");

        assertEquals(0, state.handshakedPeerCount());
        assertEquals(1, state.peerSnapshots().get(0).downs);
    }

    @Test
    @DisplayName("Flapping is counted per direction and moves the change timestamp")
    void transitionsAreCounted() {
        final PeerStateObserver state = observer(3);
        final long before = System.currentTimeMillis();
        state.peerHandshakeCompleted(peer(2));
        state.peerDisconnected(peer(2), "closed");
        state.peerHandshakeCompleted(peer(2));

        final PeerStateObserver.PeerSnapshot snapshot = state.peerSnapshots().get(0);
        assertEquals(2, snapshot.ups);
        assertEquals(1, snapshot.downs);
        assertTrue(snapshot.handshaked);
        assertTrue(snapshot.changedAtMillis >= before,
                "The last transition must carry a current timestamp");
    }

    @Test
    @DisplayName("Snapshots are ordered by peer id so the probe body is stable")
    void snapshotsAreOrdered() {
        final PeerStateObserver state = observer(5);
        state.peerHandshakeCompleted(peer(4));
        state.peerHandshakeCompleted(peer(2));
        state.peerHandshakeCompleted(peer(3));

        final List<PeerStateObserver.PeerSnapshot> snapshots = state.peerSnapshots();
        assertEquals("n2", snapshots.get(0).peer.value());
        assertEquals("n3", snapshots.get(1).peer.value());
        assertEquals("n4", snapshots.get(2).peer.value());
    }

    @Test
    @DisplayName("Events reach the wrapped observer as well")
    void forwardsToTheDelegate() {
        final int[] seen = {0, 0};
        final PeerStateObserver state = new PeerStateObserver(3, new NodeObserver() {
            @Override
            public void peerHandshakeCompleted(final NodeId peer) {
                seen[0]++;
            }

            @Override
            public void peerDisconnected(final NodeId peer, final String reason) {
                seen[1]++;
            }
        });
        state.peerHandshakeCompleted(peer(2));
        state.peerDisconnected(peer(2), "closed");

        assertEquals(1, seen[0], "A decorator that swallows events breaks the rest of the chain");
        assertEquals(1, seen[1]);
    }

    @Test
    @DisplayName("Registered metrics report the peer series and quorum state")
    void metricsReflectState() {
        final MetricRegistry registry = new MetricRegistry();
        final PeerStateObserver state = observer(3);
        state.registerMetrics(registry);
        state.peerHandshakeCompleted(peer(2));
        state.peerDisconnected(peer(2), "closed");
        state.peerHandshakeCompleted(peer(3));

        final String text = PrometheusTextFormat.render(registry);
        assertTrue(text.contains("discas_node_cluster_size 3\n"), text);
        assertTrue(text.contains("discas_node_peers_handshaked 1\n"), text);
        assertTrue(text.contains("discas_node_quorum_available 1\n"), text);
        assertTrue(text.contains("discas_node_peer_handshaked{peer=\"n2\"} 0\n"), text);
        assertTrue(text.contains("discas_node_peer_handshaked{peer=\"n3\"} 1\n"), text);
        assertTrue(text.contains("discas_node_peer_transitions_total{peer=\"n2\",direction=\"up\"} 1\n"),
                text);
        assertTrue(text.contains("discas_node_peer_transitions_total{peer=\"n2\",direction=\"down\"} 1\n"),
                text);
    }
}
