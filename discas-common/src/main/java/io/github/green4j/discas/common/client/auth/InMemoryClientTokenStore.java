/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Static, in-memory {@link ClientTokenStore} for tests, examples, and simple deployments.
 * Tokens are fixed at construction and never change. Mirrors {@code InMemoryMembers}.
 */
public final class InMemoryClientTokenStore implements ClientTokenStore {

    private final ClientTokens snapshot;

    public InMemoryClientTokenStore(final ClientTokens snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public ClientTokens snapshot() {
        return snapshot;
    }

    @Override
    public void addListener(final Consumer<ClientTokens> listener) {
        // Replay-on-subscribe for a uniform consumer path: deliver the fixed snapshot as
        // the first (and only) update. The token set never changes afterwards.
        listener.accept(snapshot);
    }

    /** A builder that PBKDF2-hashes plaintext tokens at provisioning time. */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<ClientId, List<TokenRecord>> byClient = new LinkedHashMap<>();

        /** Provision {@code token} for {@code clientId} expiring at {@code notAfterEpochMs}. */
        public Builder add(final ClientId clientId, final String token,
                           final long notAfterEpochMs) {
            final byte[] salt = Pbkdf2.newSalt();
            final byte[] hash = Pbkdf2.hash(token, salt, Pbkdf2.DEFAULT_ITERATIONS);
            byClient.computeIfAbsent(clientId, k -> new ArrayList<>())
                    .add(new TokenRecord(salt, Pbkdf2.DEFAULT_ITERATIONS, hash, notAfterEpochMs));
            return this;
        }

        public InMemoryClientTokenStore build() {
            return new InMemoryClientTokenStore(new ClientTokens(byClient));
        }
    }
}
