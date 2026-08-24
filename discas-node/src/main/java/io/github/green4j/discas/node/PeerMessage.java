/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.identity.NodeId;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Marker interface for the peer-to-peer protocol message family
 * (CASPaxos rounds + anti-entropy). The client-side protocol uses
 * the independent {@link io.github.green4j.discas.common.client.ClientMessage} hierarchy -- the two share no
 * common base.
 */
public interface PeerMessage {

    NodeId senderId();

    long correlationId();

    final class PrepareReq implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final HashedBytes key;
        private final Ballot ballot;

        public PrepareReq(final NodeId senderId, final long correlationId,
                          final HashedBytes key, final Ballot ballot) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = key;
            this.ballot = ballot;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public HashedBytes key() {
            return key;
        }

        public Ballot ballot() {
            return ballot;
        }

        @Override
        public String toString() {
            return "PrepareReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", key=" + key + ", ballot=" + ballot + "]";
        }
    }

    final class PrepareResp implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final boolean ok;
        private final Ballot promised;
        private final Ballot accepted;
        private final HashedBytes value;
        private final boolean tombstone;

        public PrepareResp(final NodeId senderId, final long correlationId,
                           final boolean ok, final Ballot promised,
                           final Ballot accepted, final HashedBytes value, final boolean tombstone) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.ok = ok;
            this.promised = promised;
            this.accepted = accepted;
            this.value = value;
            this.tombstone = tombstone;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public boolean ok() {
            return ok;
        }

        public Ballot promised() {
            return promised;
        }

        public Ballot accepted() {
            return accepted;
        }

        public HashedBytes value() {
            return value;
        }

        public boolean tombstone() {
            return tombstone;
        }

        @Override
        public String toString() {
            return "PrepareResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", ok=" + ok + ", promised=" + promised + ", accepted=" + accepted
                    + ", value=" + value + ", tombstone=" + tombstone + "]";
        }
    }

    final class AcceptReq implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final HashedBytes key;
        private final Ballot ballot;
        private final HashedBytes value;
        private final boolean tombstone;

        public AcceptReq(final NodeId senderId, final long correlationId,
                         final HashedBytes key, final Ballot ballot,
                         final HashedBytes value, final boolean tombstone) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = key;
            this.ballot = ballot;
            this.value = value;
            this.tombstone = tombstone;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public HashedBytes key() {
            return key;
        }

        public Ballot ballot() {
            return ballot;
        }

        public HashedBytes value() {
            return value;
        }

        public boolean tombstone() {
            return tombstone;
        }

        @Override
        public String toString() {
            return "AcceptReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", key=" + key + ", ballot=" + ballot + ", value=" + value
                    + ", tombstone=" + tombstone + "]";
        }
    }

    final class AcceptResp implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final boolean ok;
        private final Ballot promised;

        public AcceptResp(final NodeId senderId, final long correlationId,
                          final boolean ok, final Ballot promised) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.ok = ok;
            this.promised = promised;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public boolean ok() {
            return ok;
        }

        public Ballot promised() {
            return promised;
        }

        @Override
        public String toString() {
            return "AcceptResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", ok=" + ok + ", promised=" + promised + "]";
        }
    }

    final class DigestReq implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;

        public DigestReq(final NodeId senderId, final long correlationId) {
            this.senderId = senderId;
            this.correlationId = correlationId;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        @Override
        public String toString() {
            return "DigestReq[senderId=" + senderId + ", correlationId=" + correlationId + "]";
        }
    }

    final class DigestResp implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final Map<Integer, HashedBytes> rangeDigests;

        public DigestResp(final NodeId senderId, final long correlationId,
                          final Map<Integer, HashedBytes> rangeDigests) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.rangeDigests = rangeDigests;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        Map<Integer, HashedBytes> rangeDigests() {
            return rangeDigests;
        }

        @Override
        public String toString() {
            return "DigestResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", rangeDigests=" + rangeDigests + "]";
        }
    }

    final class KeysReq implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final int range;

        private final ByteBuffer startAfter;
        private final int limit;

        /**
         * @param startAfter exclusive lower bound within the range, or {@code null} for its first
         *                   key -- the page cursor
         * @param limit      maximum digests to return; the responder clamps it
         */
        public KeysReq(final NodeId senderId, final long correlationId, final int range,
                       final ByteBuffer startAfter, final int limit) {
            if (limit < 1) {
                throw new IllegalArgumentException("Keys limit must be >= 1, got " + limit);
            }
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.range = range;
            this.startAfter = ByteBuffers.readOnly(startAfter);
            this.limit = limit;
        }

        /** Exclusive lower bound, or {@code null} for "from the start of the range". */
        public ByteBuffer startAfter() {
            return ByteBuffers.duplicate(startAfter);
        }

        public int limit() {
            return limit;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public int range() {
            return range;
        }

        @Override
        public String toString() {
            return "KeysReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", range=" + range + ", startAfter=" + ByteBuffers.preview(startAfter) + ", limit=" + limit + "]";
        }
    }

    /**
     * "What is the highest ballot counter you have reserved the right to promise or to issue?"
     * <p>
     * Asked only by a node that started with no durable state, and only until it has an answer from
     * enough members to derive a promise floor.
     */
    final class CeilingReq implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;

        public CeilingReq(final NodeId senderId, final long correlationId) {
            this.senderId = senderId;
            this.correlationId = correlationId;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        @Override
        public String toString() {
            return "CeilingReq[senderId=" + senderId + ", correlationId=" + correlationId + "]";
        }
    }

    /**
     * The two bounds this node reserved <em>ahead of use</em>, and so cannot be short of after an
     * unclean shutdown: what it may promise, and what its proposer may issue. Plus whether it is
     * entitled to be believed at all.
     * <p>
     * Deliberately not {@code maxBallotSeen}, which is derived from Promise records -- those are not
     * forced, so a responder's copy can be missing the very tail the asker is worried about.
     */
    final class CeilingResp implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final long promisedUpTo;
        private final long reservedProposerBallot;
        private final CeilingEvidence evidence;

        public CeilingResp(final NodeId senderId, final long correlationId,
                           final long promisedUpTo, final long reservedProposerBallot,
                           final CeilingEvidence evidence) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.promisedUpTo = promisedUpTo;
            this.reservedProposerBallot = reservedProposerBallot;
            this.evidence = evidence;
        }

        /** What these bounds are worth to the asker: see {@link CeilingEvidence}. */
        public CeilingEvidence evidence() {
            return evidence;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public long promisedUpTo() {
            return promisedUpTo;
        }

        public long reservedProposerBallot() {
            return reservedProposerBallot;
        }

        /** The bound this response contributes: neither field may be exceeded without a forced write. */
        public long ceiling() {
            return Math.max(promisedUpTo, reservedProposerBallot);
        }

        @Override
        public String toString() {
            return "CeilingResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", promisedUpTo=" + promisedUpTo
                    + ", reservedProposerBallot=" + reservedProposerBallot
                    + ", evidence=" + evidence + "]";
        }
    }

    /**
     * "Can you still resurrect the value this tombstone suppresses?"
     * <p>
     * Asked of <em>every</em> member, not a quorum: the danger is precisely the replica outside the
     * quorum, which still holds the old value and would have anti-entropy copy it back once the
     * tombstone is gone.
     */
    final class PurgeCheckReq implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final HashedBytes key;
        private final Ballot tombstoneBallot;

        public PurgeCheckReq(final NodeId senderId, final long correlationId,
                             final HashedBytes key, final Ballot tombstoneBallot) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = key;
            this.tombstoneBallot = tombstoneBallot;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public HashedBytes key() {
            return key;
        }

        /** The ballot the tombstone was accepted at: the question is about that state and no other. */
        public Ballot tombstoneBallot() {
            return tombstoneBallot;
        }

        @Override
        public String toString() {
            return "PurgeCheckReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", key=" + key + ", tombstoneBallot=" + tombstoneBallot + "]";
        }
    }

    /**
     * One member's answer, which is durable in a way a reading is not: ballots only rise and a
     * tombstone once applied is never un-applied, so an answer stays true once true. That is what
     * makes collection an exchange of messages rather than a distributed snapshot.
     * <p>
     * The key and ballot are not echoed -- the correlation id identifies the question, and a
     * coordinator that has forgotten it has nothing to do with the answer anyway.
     */
    final class PurgeCheckResp implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final PurgeAnswer answer;

        public PurgeCheckResp(final NodeId senderId, final long correlationId,
                              final PurgeAnswer answer) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.answer = answer;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public PurgeAnswer answer() {
            return answer;
        }

        @Override
        public String toString() {
            return "PurgeCheckResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", answer=" + answer + "]";
        }
    }

    /**
     * The decision, broadcast to every member: this tombstone may go.
     * <p>
     * No response, and none is needed -- a member that misses it keeps the tombstone, which is the
     * state it was suppressing anyway, and the next sweep asks again. What it must not be is a
     * decision each member reaches on its own: one that had collected and one that had not would
     * differ, and anti-entropy would call that divergence and repair it by sending the tombstone
     * back, forever.
     */
    final class PurgeReq implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final HashedBytes key;
        private final Ballot tombstoneBallot;

        public PurgeReq(final NodeId senderId, final long correlationId,
                        final HashedBytes key, final Ballot tombstoneBallot) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = key;
            this.tombstoneBallot = tombstoneBallot;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public HashedBytes key() {
            return key;
        }

        public Ballot tombstoneBallot() {
            return tombstoneBallot;
        }

        @Override
        public String toString() {
            return "PurgeReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", key=" + key + ", tombstoneBallot=" + tombstoneBallot + "]";
        }
    }

    final class KeysResp implements PeerMessage {
        private final NodeId senderId;
        private final long correlationId;
        private final int range;
        private final List<KeyDigest> digests;

        private final boolean hasMore;

        /**
         * @param digests matching digests in ascending key order, at most the request's limit
         * @param hasMore whether this node holds further keys in the range after the last digest.
         *                A comparison can only be trusted up to the smallest last-key among
         *                responders reporting {@code true}: beyond that, a key absent from a
         *                peer's page may simply not have been sent, and treating it as missing
         *                would trigger a pointless repair round.
         */
        public KeysResp(final NodeId senderId, final long correlationId, final int range,
                        final List<KeyDigest> digests, final boolean hasMore) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.range = range;
            this.digests = digests;
            this.hasMore = hasMore;
        }

        public boolean hasMore() {
            return hasMore;
        }

        @Override
        public NodeId senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public int range() {
            return range;
        }

        public List<KeyDigest> digests() {
            return digests;
        }

        @Override
        public String toString() {
            return "KeysResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", range=" + range + ", digests=" + digests + ", hasMore=" + hasMore + "]";
        }
    }
}
