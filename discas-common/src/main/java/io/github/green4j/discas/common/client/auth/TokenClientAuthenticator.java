/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;

import java.util.List;
import java.util.function.LongSupplier;

/**
 * Authenticates a client by a shared-secret token (Consul-style API key). The token
 * arrives plaintext in the CLIENT_HELLO; this authenticator looks up the claimed client's
 * records in a {@link ClientTokenStore} and accepts if <b>any non-expired</b> record's
 * PBKDF2 hash matches (constant-time). Expired records are ignored, so an old token stops
 * working the moment it lapses while a freshly-added one already works -- zero-disruption
 * overlap rotation, driven by the store's hot reload.
 */
public final class TokenClientAuthenticator implements ClientAuthenticator {

    private final ClientTokenStore store;
    private final LongSupplier clock;

    public TokenClientAuthenticator(final ClientTokenStore store) {
        this(store, System::currentTimeMillis);
    }

    /** Test seam: inject a clock for deterministic expiry. */
    public TokenClientAuthenticator(final ClientTokenStore store, final LongSupplier clock) {
        this.store = store;
        this.clock = clock;
    }

    @Override
    public ClientCredential authenticate(final ClientId claimed, final String credential) {
        if (credential == null || credential.isEmpty()) {
            return ClientCredential.NONE;
        }
        final List<TokenRecord> records = store.snapshot().records(claimed);
        final long now = clock.getAsLong();
        for (int i = 0; i < records.size(); i++) {
            final TokenRecord record = records.get(i);
            if (!record.expired(now) && record.matches(credential)) {
                return ClientCredential.of(claimed);
            }
        }
        return ClientCredential.NONE;
    }
}
