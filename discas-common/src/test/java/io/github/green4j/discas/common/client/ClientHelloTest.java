/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.ClientDescription;
import io.github.green4j.discas.common.identity.ClientId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("ClientHello -- payload encode/decode round-trip")
class ClientHelloTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("payloads")
    @DisplayName("Round-trips version, clientId, credential and description")
    void roundTrip(final String name, final int version, final String clientId,
                   final String credential, final String description) {
        final ByteBuffer payload = ClientHello.encode(version, ClientId.of(clientId), credential,
                ClientDescription.ofNullable(description));

        final ClientHello.Decoded decoded = ClientHello.decode(payload);
        assertEquals(version, decoded.version);
        assertEquals(ClientId.of(clientId), decoded.clientId);
        assertEquals(credential, decoded.credential);
        assertEquals(ClientDescription.ofNullable(description), decoded.description);
    }

    static Stream<Arguments> payloads() {
        return Stream.of(
                Arguments.of("a token credential", 7, "web-1", "api-key-123", null),
                // The zero-length credential is the edge the length prefix has to carry.
                Arguments.of("an empty credential", 2, "dev", "", null),
                Arguments.of("a description", 1, "web-1", "api-key-123", "Jenkins euc1-blue"),
                Arguments.of("a description at the bound", 1, "web-1", null,
                        "d".repeat(KvLimits.MAX_CLIENT_DESCRIPTION_BYTES)));
    }

    @Test
    @DisplayName("Decode reads from a duplicate, leaving the caller's buffer position intact")
    void decodeDoesNotConsumeCallerBuffer() {
        final ByteBuffer payload = ClientHello.encode(2, ClientId.of("dev"), "", null);
        final int positionBefore = payload.position();

        ClientHello.decode(payload);

        assertEquals(positionBefore, payload.position());
    }
}
