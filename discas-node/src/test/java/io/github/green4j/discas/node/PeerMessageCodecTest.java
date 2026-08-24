/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;


import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("PeerMessageCodec -- error paths")
class PeerMessageCodecTest {
    private static final NodeId N1 = NodeId.of("1");

    @Test
    void decodeRejectsTrailingBytes() {
        final PeerMessage.AcceptResp message = new PeerMessage.AcceptResp(N1, 9L, true, new Ballot(10L, N1));

        final ByteBuffer encoded = PeerMessageCodec.encode(message);
        final ByteBuffer withGarbage = ByteBuffer.allocate(encoded.remaining() + 1);
        withGarbage.put(encoded);
        withGarbage.put((byte) 42);
        withGarbage.flip();

        assertThrows(IllegalArgumentException.class, () -> PeerMessageCodec.decode(withGarbage));
    }

    @Test
    @DisplayName("The message layout is byte-for-byte stable and carries no protocol version")
    void encodingIsStableAndUnversioned() {
        // A pin on the wire layout, the peer-side twin of the one in ClientMessageCodecTest. The
        // protocol version is negotiated once, in PEER_HELLO, and must never reappear here:
        // paying four bytes per message to re-assert what the handshake already settled is
        // exactly the regression this test exists to catch. So the payload starts at the type
        // byte -- see TransportProtocol.PROTOCOL_VERSION.
        final ByteBuffer encoded = PeerMessageCodec.encode(new PeerMessage.PrepareReq(
                N1, 3L, new HashedBytes(new byte[] {9, 8}), new Ballot(5L, N1)));

        // type PREPARE_REQ | len+"1" | correlationId | len+key | ballot(counter, len+nodeId)
        assertEquals("01"
                        + "00000001" + "31"
                        + "0000000000000003"
                        + "00000002" + "0908"
                        + "0000000000000005" + "00000001" + "31",
                hex(encoded));
    }

    @Test
    @DisplayName("A ceiling answer keeps its two bounds and what they are worth")
    void ceilingRespRoundTrips() {
        // The evidence is the difference between an answer and a nothing: a member that started
        // empty reports honest zeros, and counting those as evidence is how a floor lands below a
        // promise somebody made. Pinned here because it is one byte and easy to drop in a codec.
        for (final CeilingEvidence evidence : CeilingEvidence.values()) {
            final PeerMessage.CeilingResp back = (PeerMessage.CeilingResp) PeerMessageCodec.decode(
                    PeerMessageCodec.encode(new PeerMessage.CeilingResp(N1, 7L, 0L, 0L, evidence)));
            assertEquals(evidence, back.evidence(), "Every kind of evidence must survive the wire");
        }

        final PeerMessage.CeilingResp witness = (PeerMessage.CeilingResp) PeerMessageCodec.decode(
                PeerMessageCodec.encode(new PeerMessage.CeilingResp(
                        N1, 7L, 4096L, 9001L, CeilingEvidence.WITNESS)));
        assertEquals(4096L, witness.promisedUpTo());
        assertEquals(9001L, witness.reservedProposerBallot());
        assertEquals(9001L, witness.ceiling(), "The bound is the higher of the two reservations");
    }

    @Test
    @DisplayName("A purge check keeps the key and the ballot the question is about")
    void purgeCheckRoundTrips() {
        final HashedBytes key = new HashedBytes(new byte[] {7, 7});
        final Ballot tombstone = new Ballot(11L, N1);

        final PeerMessage.PurgeCheckReq check = (PeerMessage.PurgeCheckReq) PeerMessageCodec.decode(
                PeerMessageCodec.encode(new PeerMessage.PurgeCheckReq(N1, 5L, key, tombstone)));
        assertEquals(key, check.key());
        // A ballot dropped or blurred here turns the question into "have you got anything for this
        // key", which a member that re-wrote the key above the tombstone would answer wrongly.
        assertEquals(tombstone, check.tombstoneBallot());

        final PeerMessage.PurgeReq purge = (PeerMessage.PurgeReq) PeerMessageCodec.decode(
                PeerMessageCodec.encode(new PeerMessage.PurgeReq(N1, 6L, key, tombstone)));
        assertEquals(key, purge.key());
        assertEquals(tombstone, purge.tombstoneBallot());
    }

    @Test
    @DisplayName("All three purge answers survive the wire, absent included")
    void purgeAnswersRoundTrip() {
        // ABSENT is the one worth pinning: it is a byte apart from RETAINED and means the opposite,
        // and a codec that collapsed the two would either block every collection in a cluster with
        // a replaced member (absent read as retained) or collect while a replica still holds the
        // value (retained read as absent).
        for (final PurgeAnswer answer : PurgeAnswer.values()) {
            final PeerMessage.PurgeCheckResp back = (PeerMessage.PurgeCheckResp) PeerMessageCodec.decode(
                    PeerMessageCodec.encode(new PeerMessage.PurgeCheckResp(N1, 7L, answer)));
            assertEquals(answer, back.answer());
            assertEquals(7L, back.correlationId(), "The correlation id is what identifies the question");
        }
    }

    @Test
    @DisplayName("The wire byte of an answer is fixed, not the position of its constant")
    void enumWireCodesArePinned() {
        // Both of these are read by nodes that may be older than the one that wrote them, so the
        // byte belongs to the protocol rather than to the source order. Pinned here because a
        // constant inserted to keep an enum in a meaningful order is a natural thing to do, and
        // with ordinals it silently renumbers everything after it.
        assertEquals((byte) 0, PurgeAnswer.RETAINED.code());
        assertEquals((byte) 1, PurgeAnswer.ABSENT.code());
        assertEquals((byte) 2, PurgeAnswer.HELD.code());

        assertEquals((byte) 0, CeilingEvidence.NONE.code());
        assertEquals((byte) 1, CeilingEvidence.WITNESS.code());
        assertEquals((byte) 2, CeilingEvidence.NO_HISTORY.code());
    }

    private static String hex(final ByteBuffer buffer) {
        final StringBuilder out = new StringBuilder();
        final ByteBuffer view = buffer.duplicate();
        while (view.hasRemaining()) {
            out.append(String.format("%02x", view.get()));
        }
        return out.toString();
    }
}
