/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which peers have completed their handshake, and derives from that whether this node can
 * still reach a quorum.
 * <p>
 * This is the source of truth for the readiness probe. It is a decorator rather than a poll of the
 * transport because the transport has no notion of live connectivity to poll: {@code
 * PeerTransport.peers()} is the <em>configured</em> membership, mutated only when the member list
 * reloads. The connectivity facts exist only as events, so this accumulates them.
 * <p>
 * <b>Handshaked, not connected.</b> A peer counts only after {@link #peerHandshakeCompleted}, which
 * fires once PEER_HELLO has succeeded -- and therefore, on an mTLS mesh, once the TLS handshake has
 * too. A peer whose socket is open but whose handshake is pending or rejected can carry no consensus
 * traffic and must not count towards a quorum.
 * <p>
 * <b>What this cannot tell you.</b> A completed handshake says the connection was usable, not that
 * the peer will answer the next round. There is no failure detector here -- no heartbeat, no
 * eviction -- so nothing notices a peer that is handshaked but wedged. Readiness derived
 * from it is the honest maximum: it catches a peer that is down, gone, partitioned away, or
 * refusing the handshake, and it does not pretend to catch one that is merely stuck.
 * <p>
 * <b>Threading.</b> Every peer event arrives on the node's event loop, so there is a single writer.
 * The map is concurrent and the derived count volatile because the HTTP scrape and probe threads
 * read them.
 */
public class PeerStateObserver extends DelegatingNodeObserver {

    private final int clusterSize;
    private final int quorumSize;
    private final Map<NodeId, PeerState> peers = new ConcurrentHashMap<>();

    /** Derived on every transition so readers never have to walk the map to answer the hot question. */
    private volatile int handshakedCount;

    /**
     * @param clusterSize N, the frozen quorum basis -- the same value the transport reports, not a
     *                    count of configured members, so a client-side subset cannot change it
     */
    public PeerStateObserver(final int clusterSize, final NodeObserver delegate) {
        super(delegate);
        if (clusterSize < 1) {
            throw new IllegalArgumentException("clusterSize must be >= 1, got " + clusterSize);
        }
        this.clusterSize = clusterSize;
        this.quorumSize = clusterSize / 2 + 1;
    }

    /** Peers whose handshake has completed and not since dropped. */
    public int handshakedPeerCount() {
        return handshakedCount;
    }

    /** N, the frozen quorum basis. */
    public int clusterSize() {
        return clusterSize;
    }

    /** The number of nodes, including this one, that a round needs. */
    public int quorumSize() {
        return quorumSize;
    }

    /**
     * Whether this node plus its handshaked peers can form a quorum.
     * <p>
     * Counts self: a node is always part of its own quorum, which is why a single-node cluster is
     * available with no peers at all, and why a 3-node cluster needs only one handshaked peer.
     */
    public boolean quorumAvailable() {
        return 1 + handshakedCount >= quorumSize;
    }

    /** An immutable snapshot of every peer this node has ever heard from, for the probe body. */
    public List<PeerSnapshot> peerSnapshots() {
        final List<PeerSnapshot> out = new ArrayList<>(peers.size());
        for (final Map.Entry<NodeId, PeerState> entry : peers.entrySet()) {
            final PeerState state = entry.getValue();
            out.add(new PeerSnapshot(entry.getKey(), state.handshaked,
                    state.changedAtMillis, state.ups, state.downs));
        }
        out.sort((a, b) -> a.peer.value().compareTo(b.peer.value()));
        return Collections.unmodifiableList(out);
    }

    /**
     * Registers the peer-state series. Called by whoever owns the registry; kept separate from the
     * constructor so that readiness works whether or not metrics are wired at all.
     */
    public void registerMetrics(final MetricRegistry registry) {
        registry.gauge("discas_node_peers_handshaked",
                "Peers whose PEER_HELLO handshake has completed and not since dropped.",
                this::handshakedPeerCount);
        registry.gauge("discas_node_cluster_size",
                "N, the frozen quorum basis.", () -> clusterSize);
        registry.gauge("discas_node_quorum_available",
                "1 when this node plus its handshaked peers can form a quorum, else 0.",
                () -> quorumAvailable() ? 1L : 0L);
        // A source rather than per-peer gauges: peers appear as they hand-shake, and registering
        // from the event loop would mean writing to the registry on a protocol path. This way
        // registration stays a startup activity and the sample set is enumerated per scrape.
        registry.register(sink -> {
            final List<PeerSnapshot> snapshots = peerSnapshots();
            for (int i = 0; i < snapshots.size(); i++) {
                final PeerSnapshot snapshot = snapshots.get(i);
                final String peer = snapshot.peer.value();
                sink.gauge("discas_node_peer_handshaked",
                        "1 when the peer's PEER_HELLO handshake has completed and the connection is "
                                + "live; TCP connectivity alone does not count.",
                        snapshot.handshaked ? 1L : 0L, "peer", peer);
                sink.gauge("discas_node_peer_state_changed_seconds",
                        "Unix seconds of the peer's last handshake/disconnect transition.",
                        snapshot.changedAtMillis / 1000L, "peer", peer);
                sink.counter("discas_node_peer_transitions_total",
                        "Peer handshake/disconnect transitions, by direction.",
                        snapshot.ups, "peer", peer, "direction", "up");
                sink.counter("discas_node_peer_transitions_total",
                        "Peer handshake/disconnect transitions, by direction.",
                        snapshot.downs, "peer", peer, "direction", "down");
            }
        });
    }

    @Override
    public void peerHandshakeCompleted(final NodeId peer) {
        final PeerState state = peers.computeIfAbsent(peer, ignored -> new PeerState());
        // Idempotent: a repeat handshake for a peer already up is not a transition. Both ends of the
        // single-dialer model can report the same peer, and a re-handshake on the same live session
        // would otherwise double-count the peer into a quorum it does not have.
        boolean quorumFlipped = false;
        if (!state.handshaked) {
            state.handshaked = true;
            state.ups++;
            state.changedAtMillis = System.currentTimeMillis();
            quorumFlipped = recount();
        }
        super.peerHandshakeCompleted(peer);
        // After the peer event, not before: "quorum available" only makes sense to a reader who has
        // just been told which peer arrived.
        if (quorumFlipped) {
            delegate().quorumAvailability(quorumAvailable(), handshakedCount);
        }
    }

    @Override
    public void peerDisconnected(final NodeId peer, final String reason) {
        final PeerState state = peers.get(peer);
        // A disconnect for a peer never seen up is not a transition -- and must not create an entry,
        // or a peer that only ever failed its handshake would appear in the probe body as though it
        // had once been available.
        boolean quorumFlipped = false;
        if (state != null && state.handshaked) {
            state.handshaked = false;
            state.downs++;
            state.changedAtMillis = System.currentTimeMillis();
            quorumFlipped = recount();
        }
        super.peerDisconnected(peer, reason);
        if (quorumFlipped) {
            delegate().quorumAvailability(quorumAvailable(), handshakedCount);
        }
    }

    /**
     * Recomputes the handshaked count from the map, walking it rather than incrementing a counter:
     * the map is the state, and a derived counter that drifted from it would make readiness lie in
     * the direction that matters most. The walk is over {@code N} entries and happens only on a
     * transition, not per request.
     *
     * @return whether this recount moved the node across the quorum line, in either direction
     */
    private boolean recount() {
        final boolean wasAvailable = quorumAvailable();
        int count = 0;
        for (final PeerState state : peers.values()) {
            if (state.handshaked) {
                count++;
            }
        }
        handshakedCount = count;
        // Reported by the caller, because this is where the count lives but the peer event has to
        // come first: the individual up/down events carry no arithmetic and cannot say which side
        // of the line the node is on.
        return quorumAvailable() != wasAvailable;
    }

    /**
     * Mutable per-peer state.
     * <p>
     * The fields are {@code volatile} for <em>visibility</em> to the scrape and probe threads, not
     * for mutual exclusion: {@code ups++} is a read-modify-write and would lose updates under
     * concurrent writers. It is safe here only because every peer event arrives on the node's event
     * loop, so there is exactly one writer. Anything that starts reporting peer events from another
     * thread must convert these to atomics first.
     * <p>
     * A scrape can observe one peer updated and another not; that is a sampling snapshot, not a
     * transaction, which is the right guarantee for a metrics read.
     */
    private static final class PeerState {
        private volatile boolean handshaked;
        private volatile long changedAtMillis;
        private volatile long ups;
        private volatile long downs;
    }

    /** An immutable view of one peer's state at a moment. */
    public static final class PeerSnapshot {
        public final NodeId peer;
        public final boolean handshaked;
        public final long changedAtMillis;
        public final long ups;
        public final long downs;

        PeerSnapshot(final NodeId peer, final boolean handshaked, final long changedAtMillis,
                     final long ups, final long downs) {
            this.peer = peer;
            this.handshaked = handshaked;
            this.changedAtMillis = changedAtMillis;
            this.ups = ups;
            this.downs = downs;
        }
    }
}
