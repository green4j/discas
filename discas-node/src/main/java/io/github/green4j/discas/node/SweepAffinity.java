/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.KeyHash;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerTransport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Which node sweeps which tombstone: <b>one member owns each key, and its owner is the one that
 * offers it.</b> A preference over the sweep queue, not a filter on it -- see the fallback below.
 *
 * <p>Without this every node sweeps the same key. The candidate is always the head of an age-ordered
 * queue, and those queues agree across members because they are filled in replication order -- so
 * all {@code N} nodes ask about one key per interval, one of them collects it, and the rest broadcast
 * an idempotent repeat. The messages were never the waste; the <b>rate</b> was. Ownership is what
 * turns one key per interval for the cluster into one per interval <em>per node</em>.
 *
 * <p><b>Ownership orders the queue; it does not shorten it.</b> A node that owns none of the
 * tombstones it holds still offers its oldest. That fallback is not politeness, it is what keeps an
 * unacknowledged {@code PURGE} self-healing: a member that missed the broadcast holds a tombstone
 * the rest of the cluster has dropped, and the only node that can ever offer it again is that
 * member -- its owner dropped the key and has nothing left to offer. Exclusive ownership would
 * leave such a tombstone in place forever.
 *
 * <p><b>It costs no availability</b>, and that follows from the collection rule rather than from
 * luck: a purge needs an answer from every member, so when the owner of a key is unreachable that
 * key could not have been collected by anybody -- the silent member is the owner. Not sweeping it is
 * the same outcome reached without the round trip.
 *
 * <p><b>The same hash as the client's routing, a different list.</b> {@link KeyHash} is shared so
 * that one function decides partitioning everywhere. The list it indexes is not: a client indexes
 * its own peer list, whose order is that client's business, while this has to be a fact every member
 * agrees on. So the members are sorted by {@link NodeId}, which every node can compute alone and
 * none can compute differently -- {@code N} is frozen, the handshake refuses a peer that disagrees
 * about it, and a membership reload is rejected unless it names exactly {@code N} members including
 * this one.
 *
 * <p>Read fresh on every sweep rather than cached, because a membership reload rewrites the peer
 * list in place: a cached order would sweep by a map of a cluster that has since changed, and the
 * failure -- a key whose owner has left being swept by nobody -- is silent. Sorting {@code N} ids
 * once per sweep interval is not a cost worth a cache.
 */
final class SweepAffinity {

    private final NodeId nodeId;
    private final PeerTransport transport;

    SweepAffinity(final NodeId nodeId, final PeerTransport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
    }

    /**
     * Whether this node is the one whose turn it is to offer {@code key}.
     * <p>
     * Total and disjoint by construction: every key maps to exactly one member of the list, so no
     * key is ownerless -- which would leave it uncollected forever with no fault to point at -- and
     * none is owned twice, which would put the duplicate sweeps back. Answering {@code false} is a
     * preference and not a veto; {@code LocalStore.tombstoneCandidate} says what happens to a key
     * nobody present owns.
     */
    boolean ownedByMe(final HashedBytes key) {
        final List<NodeId> members = sortedMembers();
        final int owner = Integer.remainderUnsigned(
                KeyHash.distributionHash(key.toBuffer()), members.size());
        return nodeId.equals(members.get(owner));
    }

    /** Every member including this node, in the one order all of them compute identically. */
    private List<NodeId> sortedMembers() {
        final List<NodeId> peers = transport.peers();
        final List<NodeId> members = new ArrayList<>(peers.size() + 1);
        members.addAll(peers);
        members.add(nodeId);
        Collections.sort(members);
        return members;
    }
}
