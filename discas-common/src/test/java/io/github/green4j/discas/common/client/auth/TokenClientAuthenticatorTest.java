/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("TokenClientAuthenticator -- token verification, expiry, overlap rotation")
class TokenClientAuthenticatorTest {

    private static final ClientId WEB = ClientId.of("web-1");
    private static final long HOUR = 3_600_000L;

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedCredentials")
    @DisplayName("Anything but the right token for the right client is refused")
    void credentialRejected(final String name, final ClientId claimed, final String credential) {
        final ClientTokenStore store = InMemoryClientTokenStore.builder()
                .add(WEB, "good-token", 10_000L).build();
        final TokenClientAuthenticator auth = new TokenClientAuthenticator(store, () -> 0L);

        assertSame(ClientCredential.NONE, auth.authenticate(claimed, credential));
    }

    static Stream<Arguments> rejectedCredentials() {
        return Stream.of(
                Arguments.of("a wrong token", WEB, "bad-token"),
                // An AllowAll-style hello carries no credential at all.
                Arguments.of("an empty credential", WEB, ""),
                Arguments.of("an unknown client", ClientId.of("stranger"), "good-token"));
    }

    @Test
    @DisplayName("A valid, non-expired token authenticates the claimed client")
    void validTokenAccepted() {
        final ClientTokenStore store = InMemoryClientTokenStore.builder()
                .add(WEB, "good-token", 10_000L).build();
        final TokenClientAuthenticator auth = new TokenClientAuthenticator(store, () -> 0L);

        final ClientCredential cred = auth.authenticate(WEB, "good-token");
        assertTrue(cred.authenticated());
        assertEquals(WEB, cred.clientId());
    }

    @Test
    @DisplayName("An expired token is rejected")
    void expiredTokenRejected() {
        final AtomicLong now = new AtomicLong(0L);
        final ClientTokenStore store = InMemoryClientTokenStore.builder()
                .add(WEB, "good-token", HOUR).build();
        final TokenClientAuthenticator auth = new TokenClientAuthenticator(store, now::get);

        now.set(HOUR - 1);
        assertTrue(auth.authenticate(WEB, "good-token").authenticated());

        now.set(HOUR); // notAfter reached -> expired
        assertSame(ClientCredential.NONE, auth.authenticate(WEB, "good-token"));
    }

    @Test
    @DisplayName("Overlap rotation: old and new both valid, then old expires with no gap")
    void overlapRotation() {
        final AtomicLong now = new AtomicLong(0L);
        // old token expires at 1h; new token expires at 2h -- both active during the overlap.
        final ClientTokenStore store = InMemoryClientTokenStore.builder()
                .add(WEB, "old-token", HOUR)
                .add(WEB, "new-token", 2 * HOUR)
                .build();
        final TokenClientAuthenticator auth = new TokenClientAuthenticator(store, now::get);

        now.set(HOUR / 2); // during overlap: both work
        assertTrue(auth.authenticate(WEB, "old-token").authenticated());
        assertTrue(auth.authenticate(WEB, "new-token").authenticated());

        now.set(HOUR + 1); // old lapsed, new still valid -- zero-gap handover
        assertFalse(auth.authenticate(WEB, "old-token").authenticated());
        assertTrue(auth.authenticate(WEB, "new-token").authenticated());
    }
}
