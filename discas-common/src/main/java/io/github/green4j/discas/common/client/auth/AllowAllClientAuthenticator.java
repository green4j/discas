/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;

/**
 * Default authenticator: accepts every client, taking its claimed identity at face
 * value without checking any credential. For tests, development, and limited trusted
 * deployments -- the client analogue of {@code PlaintextPeerSecurity}.
 */
public final class AllowAllClientAuthenticator implements ClientAuthenticator {

    public static final AllowAllClientAuthenticator INSTANCE = new AllowAllClientAuthenticator();

    private AllowAllClientAuthenticator() {
    }

    @Override
    public ClientCredential authenticate(final ClientId claimed, final String credential) {
        return ClientCredential.of(claimed);
    }
}
