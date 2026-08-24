/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@DisplayName("ClientMessageCodec -- error paths")
class ClientMessageCodecTest {

    @Test
    void getReqRoundTripsConsistency() {
        for (final ReadConsistency level : ReadConsistency.values()) {
            final ClientMessage.ClientGetReq req = new ClientMessage.ClientGetReq(
                    "c1", 7L, ByteBuffer.wrap(new byte[]{9, 8, 7}), level);
            final ClientMessage decoded = ClientMessageCodec.decode(ClientMessageCodec.encode(req));
            final ClientMessage.ClientGetReq out =
                    assertInstanceOf(ClientMessage.ClientGetReq.class, decoded);
            assertEquals(level, out.consistency());
        }
    }

    @Test
    @DisplayName("Versioned CAS round-trips its fence, including the create-if-absent ballot")
    void versionedCasReqRoundTrips() {
        // Ballot.ZERO is a legitimate expected version ("no committed value"), not a wildcard,
        // so it has to survive the wire like any other.
        for (final Ballot fence : new Ballot[]{Ballot.ZERO, new Ballot(42L, NodeId.of("n3"))}) {
            final ClientMessage.ClientCasReq req =
                    new ClientMessage.ClientCasReq(
                            "c1", 7L, ByteBuffer.wrap(new byte[]{1, 2}), fence,
                            ByteBuffer.wrap(new byte[]{3, 4, 5}));

            final ClientMessage.ClientCasReq out = assertInstanceOf(
                    ClientMessage.ClientCasReq.class,
                    ClientMessageCodec.decode(ClientMessageCodec.encode(req)));

            assertEquals(fence, out.expectedVersion());
            assertEquals(ByteBuffer.wrap(new byte[]{1, 2}), out.key());
            assertEquals(ByteBuffer.wrap(new byte[]{3, 4, 5}), out.desired());
        }
    }

    @Test
    @DisplayName("Versioned CAS carries a null desired -- a tombstone is a value like any other")
    void versionedCasReqRoundTripsTombstone() {
        final ClientMessage.ClientCasReq req = new ClientMessage.ClientCasReq(
                "c1", 7L, ByteBuffer.wrap(new byte[]{1}), new Ballot(3L, NodeId.of("n1")), null);

        final ClientMessage.ClientCasReq out = assertInstanceOf(
                ClientMessage.ClientCasReq.class,
                ClientMessageCodec.decode(ClientMessageCodec.encode(req)));

        assertNull(out.desired());
    }

    @Test
    @DisplayName("A failed versioned CAS reports the version it lost to, not just that it lost")
    void versionedCasRespCarriesCurrentVersion() {
        // The point of the response type: a caller that lost the compare can recompute from the
        // version in hand instead of paying a second round trip to discover it.
        final Ballot current = new Ballot(9L, NodeId.of("n2"));
        final ClientMessage.ClientCasResp resp =
                new ClientMessage.ClientCasResp(
                        "n2", 7L, true, false, ByteBuffer.wrap(new byte[]{7}), current,
                        null, ClientErrorCode.NONE);

        final ClientMessage.ClientCasResp out = assertInstanceOf(
                ClientMessage.ClientCasResp.class,
                ClientMessageCodec.decode(ClientMessageCodec.encode(resp)));

        assertEquals(current, out.version());
        assertEquals(ByteBuffer.wrap(new byte[]{7}), out.value());
        assertEquals(ClientErrorCode.NONE, out.errorCode());
        assertFalse(out.swapped());
    }

    @Test
    @DisplayName("A versioned CAS without a fence is rejected at construction, not on the wire")
    void versionedCasReqRequiresAnExpectedVersion() {
        assertThrows(IllegalArgumentException.class, () -> new ClientMessage.ClientCasReq(
                "c1", 7L, ByteBuffer.wrap(new byte[]{1}), null, ByteBuffer.wrap(new byte[]{2})));
    }

    @Test
    @DisplayName("A decoded message keeps its bytes after the frame it came from is reused")
    void decodedMessageOutlivesTheFrame() {
        // The "producer copies" half of the ownership rule on ClientMessage: a transport frame is
        // recycled the moment dispatch returns, but a decoded request survives a Paxos round and a
        // decoded response is handed to a future the caller resolves whenever it likes. Decoding
        // by reference would leave both reading whatever the next message wrote.
        final ByteBuffer frame = ClientMessageCodec.encode(new ClientMessage.ClientPutReq(
                "c1", 7L, ByteBuffer.wrap(new byte[]{1, 2, 3}), ByteBuffer.wrap(new byte[]{4, 5})));
        final ClientMessage.ClientPutReq decoded =
                (ClientMessage.ClientPutReq) ClientMessageCodec.decode(frame);

        // Recycle the frame under the decoded message, as the transport does.
        frame.clear();
        while (frame.hasRemaining()) {
            frame.put((byte) 0xFF);
        }

        assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3}), decoded.key());
        assertEquals(ByteBuffer.wrap(new byte[]{4, 5}), decoded.value());
    }

    @Test
    @DisplayName("Accessors hand out independent views, so one reader cannot consume another's")
    void accessorsAreIndependent() {
        final ClientMessage.ClientPutReq req = new ClientMessage.ClientPutReq(
                "c1", 7L, ByteBuffer.wrap(new byte[]{1, 2, 3}), ByteBuffer.wrap(new byte[]{4}));

        final ByteBuffer first = req.key();
        first.get();

        assertEquals(3, req.key().remaining(), "A drained view must not shrink the next one");
    }

    @Test
    void getReqDefaultsToLinearizable() {
        // No explicit consistency => LINEARIZABLE end to end.
        final ClientMessage.ClientGetReq req = new ClientMessage.ClientGetReq(
                "c1", 7L, ByteBuffer.wrap(new byte[]{1}));
        final ClientMessage.ClientGetReq out =
                (ClientMessage.ClientGetReq) ClientMessageCodec.decode(ClientMessageCodec.encode(req));
        assertEquals(ReadConsistency.LINEARIZABLE, out.consistency());
    }

    @Test
    void getRespRoundTripsVersion() {
        final Ballot version = new Ballot(42L, NodeId.of("nodeA"));
        final ClientMessage.ClientGetResp resp = new ClientMessage.ClientGetResp(
                "1", 9L, true, ByteBuffer.wrap(new byte[]{1, 2, 3}), null,
                ClientErrorCode.NONE, version);
        final ClientMessage.ClientGetResp out =
                assertInstanceOf(ClientMessage.ClientGetResp.class,
                        ClientMessageCodec.decode(ClientMessageCodec.encode(resp)));
        assertEquals(version, out.version());
        assertEquals(3, out.value().remaining());
    }

    static Stream<Arguments> truncatable() {
        return Stream.of(
                // A GET_REQ torn just short of its consistency byte. Defaulting that byte to
                // LINEARIZABLE, as this once did, turns a torn frame into a plausible request.
                Arguments.of("GET_REQ missing its consistency byte",
                        new ClientMessage.ClientGetReq("c1", 7L, ByteBuffer.wrap(new byte[]{4, 5}),
                                ReadConsistency.SERIALIZABLE)),
                // The more dangerous of the two: Ballot.ZERO on a successful read is the value a
                // caller would compare against for staleness.
                Arguments.of("GET_RESP with a torn version ballot",
                        new ClientMessage.ClientGetResp("1", 9L, true, ByteBuffer.wrap(new byte[]{7}),
                                null, ClientErrorCode.NONE, new Ballot(42L, NodeId.of("nodeA")))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("truncatable")
    @DisplayName("A message short of a mandatory field is rejected, never silently defaulted")
    void truncatedMessageIsRejected(final String name, final ClientMessage message) {
        // Every field of a message is mandatory: the handshake pins the protocol version, so a
        // short payload is corruption, never an older client.
        assertThrows(RuntimeException.class,
                () -> ClientMessageCodec.decode(truncateByOneByte(ClientMessageCodec.encode(message))));
    }

    @Test
    void decodeRejectsTrailingBytes() {
        final ClientMessage.ClientGetResp message = new ClientMessage.ClientGetResp(
                "1", 9L, true, ByteBuffer.wrap(new byte[]{1, 2, 3}), null,
                ClientErrorCode.NONE);

        final ByteBuffer encoded = ClientMessageCodec.encode(message);
        final ByteBuffer withGarbage = ByteBuffer.allocate(encoded.remaining() + 1);
        withGarbage.put(encoded);
        withGarbage.put((byte) 42);
        withGarbage.flip();

        assertThrows(IllegalArgumentException.class, () -> ClientMessageCodec.decode(withGarbage));
    }

    @Test
    @DisplayName("The message layout is byte-for-byte stable and carries no protocol version")
    void encodingIsStableAndUnversioned() {
        // A pin on the wire layout. The protocol version is negotiated once, in the hello, and
        // must never reappear here: paying four bytes per message to re-assert what the
        // handshake already settled is exactly the regression this test exists to catch.
        // The payload therefore starts at the type byte -- see TransportProtocol.PROTOCOL_VERSION.
        final ByteBuffer encoded = ClientMessageCodec.encode(new ClientMessage.ClientGetReq(
                "c1", 7L, ByteBuffer.wrap(new byte[]{9, 8}), ReadConsistency.SERIALIZABLE));

        // type CLIENT_GET_REQ | len+"c1" | correlationId | len+key | consistency
        assertEquals("01"
                        + "00000002" + "6331"
                        + "0000000000000007"
                        + "00000002" + "0908"
                        + "01",
                hex(encoded));
    }

    /** Drops the last byte, the cheapest way to tear the trailing field off an encoded message. */
    private static ByteBuffer truncateByOneByte(final ByteBuffer encoded) {
        final ByteBuffer truncated = ByteBuffer.allocate(encoded.remaining() - 1);
        final ByteBuffer src = encoded.duplicate();
        while (truncated.hasRemaining()) {
            truncated.put(src.get());
        }
        truncated.flip();
        return truncated;
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
