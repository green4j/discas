/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;

/**
 * Server-wide policy that authenticates a connecting client from its CLIENT_HELLO.
 * A client server runs exactly one authenticator (single mode: AllowAll, Token, or
 * mTLS), selected by configuration and wired directly (no ServiceLoader) -- the client
 * analogue of {@code common.transport.security.PeerSecurityProvider}.
 */
@FunctionalInterface
public interface ClientAuthenticator {

    /**
     * @param claimed    the client identity claimed in the hello
     * @param credential the opaque hello credential (e.g. a token; empty when none)
     * @return an authenticated {@link ClientCredential}, or {@link ClientCredential#NONE}
     *         to reject the connection with {@code ACCESS_DENIED}
     */
    ClientCredential authenticate(ClientId claimed, String credential);
}
