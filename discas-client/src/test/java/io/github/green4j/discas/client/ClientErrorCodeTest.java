/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ClientMessageCodec;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The failure code carried on client responses -- the thing consumers branch on instead of the
 * error text. Guards the wire round-trip and the caller-vs-node classification.
 */
@DisplayName("ClientErrorCode -- typed failure classification")
class ClientErrorCodeTest {
    private ClientMessage roundTrip(final ClientMessage message) {
        final ByteBuffer encoded = ClientMessageCodec.encode(message);
        return ClientMessageCodec.decode(encoded);
    }

    @Test
    @DisplayName("The code survives the wire on every failure-bearing response")
    void codeSurvivesRoundTrip() {
        final ClientMessage.ClientGetResp get = (ClientMessage.ClientGetResp) roundTrip(
                new ClientMessage.ClientGetResp("n", 1L, false, null, "denied",
                        ClientErrorCode.ACCESS_DENIED));
        assertEquals(ClientErrorCode.ACCESS_DENIED, get.errorCode());

        final ClientMessage.ClientPutResp put = (ClientMessage.ClientPutResp) roundTrip(
                new ClientMessage.ClientPutResp("n", 2L, false, "too big",
                        ClientErrorCode.INVALID_ARGUMENT));
        assertEquals(ClientErrorCode.INVALID_ARGUMENT, put.errorCode());

        final ClientMessage.ClientCasResp cas =
                (ClientMessage.ClientCasResp) roundTrip(
                        new ClientMessage.ClientCasResp("n", 3L, false, false, null,
                                Ballot.ZERO, "no quorum", ClientErrorCode.UNAVAILABLE));
        assertEquals(ClientErrorCode.UNAVAILABLE, cas.errorCode());

        final ClientMessage.ClientDeleteResp del = (ClientMessage.ClientDeleteResp) roundTrip(
                new ClientMessage.ClientDeleteResp("n", 4L, false, "boom",
                        ClientErrorCode.INTERNAL));
        assertEquals(ClientErrorCode.INTERNAL, del.errorCode());

        // The determinate neighbour of UNAVAILABLE: a duel lost before Accept, so nothing was
        // applied and the caller may re-issue without wondering whether it was.
        final ClientMessage.ClientPutResp lost = (ClientMessage.ClientPutResp) roundTrip(
                new ClientMessage.ClientPutResp("n", 5L, false, "lost the duel",
                        ClientErrorCode.BALLOT_LOST));
        assertEquals(ClientErrorCode.BALLOT_LOST, lost.errorCode());
    }

    @Test
    @DisplayName("Every code survives its own wire byte")
    void everyCodeRoundTripsItsByte() {
        // fromCode() is deliberately lenient -- an unknown byte lands on INTERNAL rather than
        // throwing -- which is exactly what would hide a duplicated code() value when a new code
        // is added. Pin it instead of trusting the enum to stay hand-checked.
        for (final ClientErrorCode code : ClientErrorCode.values()) {
            assertEquals(code, ClientErrorCode.fromCode(code.code()),
                    code + " does not survive its own wire byte");
        }
        // Codes are added without a protocol-version bump, so a newer node legitimately sends a
        // byte this build has never heard of. INTERNAL is the safe landing: still a failure, so it
        // cannot be misread as success, and not retryable, so it cannot send a client into a loop.
        assertEquals(ClientErrorCode.INTERNAL, ClientErrorCode.fromCode((byte) 99),
                "An unrecognised code must land on INTERNAL, never on a success");
        assertEquals(ClientErrorCode.INTERNAL, ClientErrorCode.fromCode((byte) -1),
                "Including a negative byte, which no code uses");
    }

    @Test
    @DisplayName("The version ballot still round-trips alongside the new code")
    void codeDoesNotDisplaceTheVersionBallot() {
        final Ballot version = new Ballot(7, NodeId.of("3"));
        final ClientMessage.ClientGetResp resp = (ClientMessage.ClientGetResp) roundTrip(
                new ClientMessage.ClientGetResp("n", 1L, true, ByteBuffer.wrap("v".getBytes()), null,
                        ClientErrorCode.NONE, version));

        assertEquals(version, resp.version());
        assertEquals(ClientErrorCode.NONE, resp.errorCode());
    }

    @Test
    @DisplayName("Caller errors are distinguished from node-side ones")
    void callerVersusNodeClassification() {
        assertTrue(new DisCasOperationException(ClientErrorCode.ACCESS_DENIED, "x").isCallerError());
        assertTrue(new DisCasOperationException(ClientErrorCode.INVALID_ARGUMENT, "x").isCallerError());
        assertFalse(new DisCasOperationException(ClientErrorCode.UNAVAILABLE, "x").isCallerError());
        assertFalse(new DisCasOperationException(
                ClientErrorCode.NO_QUORUM_AT_COORDINATOR, "x").isCallerError());
        assertFalse(new DisCasOperationException(ClientErrorCode.INTERNAL, "x").isCallerError());
    }

    @Test
    @DisplayName("A null code defaults to INTERNAL rather than NPE-ing or implying success")
    void nullCodeDefaultsToInternal() {
        assertEquals(ClientErrorCode.INTERNAL,
                new DisCasOperationException(null, "x").code());
    }
}
