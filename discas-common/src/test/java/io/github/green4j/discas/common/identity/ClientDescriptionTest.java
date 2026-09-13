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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("ClientDescription -- bounded free text beside the client id")
class ClientDescriptionTest {

    @Test
    void acceptsTheLongestDescriptionThatFitsTheBound() {
        final String longest = "d".repeat(KvLimits.MAX_CLIENT_DESCRIPTION_BYTES);
        assertEquals(longest, ClientDescription.of(longest).value());
    }

    @Test
    void refusesOneByteMore() {
        final String tooLong = "d".repeat(KvLimits.MAX_CLIENT_DESCRIPTION_BYTES + 1);
        assertThrows(IllegalArgumentException.class, () -> ClientDescription.of(tooLong));
    }

    @Test
    @DisplayName("The bound is in bytes, not characters")
    void refusesMultiByteTextPastTheByteBound() {
        // Two bytes each in UTF-8, so half the bound plus one character is one byte too many.
        final String tooLong = "\u00e9".repeat(KvLimits.MAX_CLIENT_DESCRIPTION_BYTES / 2 + 1);
        assertThrows(IllegalArgumentException.class, () -> ClientDescription.of(tooLong));
    }

    @Test
    void refusesEmpty() {
        assertThrows(IllegalArgumentException.class, () -> ClientDescription.of(""));
        assertNull(ClientDescription.ofNullable(""));
        assertNull(ClientDescription.ofNullable(null));
    }

    @Test
    @DisplayName("An identity with neither id nor description is the shared unauthenticated one")
    void identityLabels() {
        assertEquals("web-1 (Jenkins euc1-blue)",
                ClientIdentity.of(ClientId.of("web-1"),
                        ClientDescription.of("Jenkins euc1-blue")).label());
        assertEquals("web-1", ClientIdentity.of(ClientId.of("web-1")).label());
        assertSame(ClientIdentity.UNAUTHENTICATED, ClientIdentity.of(null, null));
    }

    @Test
    void identityCopiesItsLabelBytes() {
        final ClientIdentity identity = ClientIdentity.of(ClientId.of("web-1"));
        final byte[] dst = new byte[16];
        assertEquals(5, identity.copyLabelTo(dst, 2));
        assertEquals('w', dst[2]);
        assertEquals(5, identity.labelLength());
    }
}
