/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.identity.ClientId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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

    /**
     * One line for the reload report: every client and what it may touch. Sorted by client, because
     * the map's own order comes from {@code Properties}, which has none -- and a report whose order
     * wanders is one an operator cannot diff against the last one.
     *
     * <p>An ACL is a table of who may touch which prefixes. It holds no credential, so it is the
     * one thing here that can be named in full -- and it has to be, since "applied" without it
     * cannot tell an operator whether what landed is what they wrote.
     */
    public String summary() {
        if (byClient.isEmpty()) {
            return "no clients: every client is denied everything";
        }
        final List<ClientId> ids = new ArrayList<>(byClient.keySet());
        Collections.sort(ids);
        final StringBuilder sb = new StringBuilder();
        sb.append(ids.size()).append(ids.size() == 1 ? " client: " : " clients: ");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append("; ");
            }
            sb.append(ids.get(i).value()).append(" -> ").append(byClient.get(ids.get(i)).summary());
        }
        return sb.toString();
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
