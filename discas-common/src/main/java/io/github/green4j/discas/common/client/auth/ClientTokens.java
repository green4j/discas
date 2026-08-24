/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An immutable point-in-time view of provisioned client tokens, keyed by {@link ClientId}
 * (each client may have several records for overlap rotation). Value-based
 * {@link #equals(Object)} lets a file-backed store skip re-publishing an unchanged reload.
 * The token analogue of {@code MembersSnapshot}.
 */
public final class ClientTokens {

    private final Map<ClientId, List<TokenRecord>> byClient;

    public ClientTokens(final Map<ClientId, List<TokenRecord>> byClient) {
        final Map<ClientId, List<TokenRecord>> copy = new LinkedHashMap<>();
        for (final Map.Entry<ClientId, List<TokenRecord>> e : byClient.entrySet()) {
            copy.put(e.getKey(), List.copyOf(e.getValue()));
        }
        this.byClient = Collections.unmodifiableMap(copy);
    }

    /** The records for {@code clientId}, or an empty list if none. */
    public List<TokenRecord> records(final ClientId clientId) {
        final List<TokenRecord> records = byClient.get(clientId);
        return records == null ? List.of() : records;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientTokens)) {
            return false;
        }
        return byClient.equals(((ClientTokens) o).byClient);
    }

    @Override
    public int hashCode() {
        return byClient.hashCode();
    }
}
