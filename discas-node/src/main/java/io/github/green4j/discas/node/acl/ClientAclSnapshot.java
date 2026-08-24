/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.identity.ClientId;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An immutable point-in-time view of the client authorization table, keyed by
 * {@link ClientId}. Value-based {@link #equals(Object)} lets a file-backed source skip
 * re-publishing an unchanged reload.
 */
public final class ClientAclSnapshot {

    private final Map<ClientId, ClientPolicy> byClient;

    public ClientAclSnapshot(final Map<ClientId, ClientPolicy> byClient) {
        this.byClient = Collections.unmodifiableMap(new LinkedHashMap<>(byClient));
    }

    /** The policy for {@code clientId}, or {@code null} if the client has no grants. */
    public ClientPolicy policy(final ClientId clientId) {
        return byClient.get(clientId);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientAclSnapshot)) {
            return false;
        }
        return byClient.equals(((ClientAclSnapshot) o).byClient);
    }

    @Override
    public int hashCode() {
        return byClient.hashCode();
    }
}
