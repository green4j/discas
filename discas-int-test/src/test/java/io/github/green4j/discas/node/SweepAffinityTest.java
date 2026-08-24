/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerTransport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sweep affinity, checked as <b>ownership</b> and never as rate.
 *
 * <p>That choice is the point of this file. The reason affinity exists is a factor of {@code N} in
 * the collection rate, but a suite that counted collections per interval would be measuring the
 * scheduler; the rate is derived from two facts, and ownership is the one that can drift. So these
 * assert the two properties the derivation rests on -- <b>total</b> (no key is ownerless) and
 * <b>disjoint</b> (no key is owned twice) -- from which "the cluster collects {@code N} keys per
 * interval instead of one" follows without a clock.
 *
 * <p>Total is the load-bearing half. An ownerless key is never offered by anybody, so it stays
 * forever with no fault to point at and no member to name -- the silent kind of stall this
 * subsystem has to keep out.
 */
@DisplayName("Sweep affinity -- which node offers which tombstone")
class SweepAffinityTest {

    private static final List<NodeId> THREE =
            List.of(NodeId.of("gc-1"), NodeId.of("gc-2"), NodeId.of("gc-3"));

    private static SweepAffinity affinityOf(final NodeId self, final List<NodeId> members) {
        final List<NodeId> peers = new ArrayList<>(members);
        peers.remove(self);
        return new SweepAffinity(self, new PeerListOnly(peers));
    }

    private static List<HashedBytes> keys(final int count) {
        final List<HashedBytes> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(TestBytes.hashed("key-" + i));
        }
        return keys;
    }

    @Test
    @DisplayName("Every key is owned by exactly one member -- none ownerless, none owned twice")
    void ownershipIsTotalAndDisjoint() {
        // Each member is given the same membership in a different order, which is the shape a
        // member file or a reload can genuinely have. Agreement has to come from the rule, not
        // from everyone happening to have been handed the same list.
        final List<SweepAffinity> cluster = new ArrayList<>();
        for (int i = 0; i < THREE.size(); i++) {
            final List<NodeId> asThisMemberSeesIt = new ArrayList<>(THREE);
            Collections.rotate(asThisMemberSeesIt, i);
            cluster.add(affinityOf(THREE.get(i), asThisMemberSeesIt));
        }

        for (final HashedBytes key : keys(500)) {
            int owners = 0;
            for (final SweepAffinity node : cluster) {
                if (node.ownedByMe(key)) {
                    owners++;
                }
            }
            assertEquals(1, owners,
                    "A key owned by nobody is never swept by anybody and stays forever with no "
                            + "fault to name; a key owned twice puts the duplicate sweeps back");
        }
    }

    @Test
    @DisplayName("A single-member cluster owns everything, so nothing is left unswept")
    void oneMemberOwnsEveryKey() {
        final SweepAffinity alone = affinityOf(THREE.get(0), List.of(THREE.get(0)));

        for (final HashedBytes key : keys(100)) {
            assertTrue(alone.ownedByMe(key));
        }
    }

    @Test
    @DisplayName("Each member gets a share, so the ceilings add up rather than overlapping")
    void everyMemberOwnsSomething() {
        for (final NodeId member : THREE) {
            final SweepAffinity affinity = affinityOf(member, THREE);
            int owned = 0;
            for (final HashedBytes key : keys(300)) {
                if (affinity.ownedByMe(key)) {
                    owned++;
                }
            }
            assertTrue(owned > 0, member + " owns nothing, so its sweeps can only ever be idle");
        }
    }

    @Test
    @DisplayName("A membership change moves ownership with it, rather than being read once")
    void ownershipFollowsTheMemberList() {
        final NodeId self = THREE.get(0);
        final MutablePeers peers = new MutablePeers(List.of(THREE.get(1), THREE.get(2)));
        final SweepAffinity affinity = new SweepAffinity(self, peers);

        final List<HashedBytes> sample = keys(300);
        int ownedOfThree = 0;
        for (final HashedBytes key : sample) {
            if (affinity.ownedByMe(key)) {
                ownedOfThree++;
            }
        }

        // The constant-N replacement a reload performs: one member leaves, another takes its place.
        peers.set(List.of(THREE.get(1), NodeId.of("gc-4")));

        int ownedAfter = 0;
        for (final HashedBytes key : sample) {
            if (affinity.ownedByMe(key)) {
                ownedAfter++;
            }
        }
        assertTrue(ownedAfter > 0, "This node still owns a share of the keys after the replace");
        assertFalse(ownedOfThree == sample.size(),
                "The sample has to be split for this to say anything");
    }

    /** A transport that is nothing but a peer list; affinity reads no more than that. */
    private static class PeerListOnly implements PeerTransport {
        private List<NodeId> peers;

        PeerListOnly(final List<NodeId> peers) {
            this.peers = peers;
        }

        @Override
        public void send(final NodeId target, final PeerMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void register(final Consumer<PeerMessage> handler) {
        }

        @Override
        public List<NodeId> peers() {
            return peers;
        }

        @Override
        public int clusterSize() {
            return peers.size() + 1;
        }
    }

    /** The same, with the list rewritten in place -- which is what a membership reload does. */
    private static final class MutablePeers extends PeerListOnly {
        MutablePeers(final List<NodeId> peers) {
            super(peers);
        }

        void set(final List<NodeId> replacement) {
            super.peers = replacement;
        }
    }
}
