/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;

/**
 * Result of authenticating a CLIENT_HELLO: the established client identity, or
 * {@link #NONE} when the credential could not be authenticated. The client server
 * binds the authenticated {@link #clientId()} to the connection; authorization keys
 * off it, never off the self-declared {@code senderId} in each message.
 * <p>
 * The client analogue of {@code common.transport.security.PeerCredential}.
 */
public interface ClientCredential {

    /** Sentinel for a hello that failed authentication. */
    ClientCredential NONE = new ClientCredential() {
        @Override
        public String toString() {
            return "ClientCredential.NONE";
        }
    };

    /** True when a client identity was authenticated. */
    default boolean authenticated() {
        return false;
    }

    /** The authenticated client identity, or {@code null} when unauthenticated. */
    default ClientId clientId() {
        return null;
    }

    /** An authenticated credential for {@code clientId}. */
    static ClientCredential of(final ClientId clientId) {
        return new ClientCredential() {
            @Override
            public boolean authenticated() {
                return true;
            }

            @Override
            public ClientId clientId() {
                return clientId;
            }

            @Override
            public String toString() {
                return "ClientCredential[" + clientId + "]";
            }
        };
    }
}
