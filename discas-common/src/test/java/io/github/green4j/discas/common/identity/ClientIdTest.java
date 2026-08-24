/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.identity;

import io.github.green4j.discas.common.KvLimits;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("ClientId -- explicit, NodeId-style client identity")
class ClientIdTest {

    static Stream<Arguments> rejected() {
        return Stream.of(
                Arguments.of("null", null),
                Arguments.of("empty", ""),
                Arguments.of("over the byte limit", "x".repeat(KvLimits.MAX_CLIENT_ID_BYTES + 1)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejected")
    void ofRejects(final String name, final String value) {
        assertThrows(IllegalArgumentException.class, () -> ClientId.of(value));
    }

    @Test
    void ofAcceptsMaxLength() {
        final String atLimit = "x".repeat(KvLimits.MAX_CLIENT_ID_BYTES);
        assertEquals(atLimit, ClientId.of(atLimit).value());
    }

    @Test
    void equalsAndHashCodeByValue() {
        final ClientId a = ClientId.of("same");
        final ClientId b = ClientId.of("same");
        final ClientId c = ClientId.of("other");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
