/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.identity.ClientId;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Static, in-memory {@link ClientAcl} for tests, examples, and simple deployments. The
 * table is fixed at construction and never changes.
 */
public final class InMemoryClientAcl implements ClientAcl {

    private final ClientAclSnapshot snapshot;

    public InMemoryClientAcl(final ClientAclSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public ClientAclSnapshot snapshot() {
        return snapshot;
    }

    @Override
    public void addListener(final Consumer<ClientAclSnapshot> listener) {
        // Replay-on-subscribe: deliver the fixed snapshot as the first (and only) update.
        listener.accept(snapshot);
    }

    /** A fluent builder for assembling an ACL table in code. */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<ClientId, List<ClientPolicy.Grant>> byClient = new LinkedHashMap<>();

        /** Grant {@code ops} on keys under {@code prefix} to {@code clientId}. */
        public Builder grant(final ClientId clientId, final String prefix, final ClientOp... ops) {
            final EnumSet<ClientOp> opSet = EnumSet.noneOf(ClientOp.class);
            for (final ClientOp op : ops) {
                opSet.add(op);
            }
            byClient.computeIfAbsent(clientId, k -> new ArrayList<>())
                    .add(new ClientPolicy.Grant(prefix, opSet));
            return this;
        }

        public InMemoryClientAcl build() {
            final Map<ClientId, ClientPolicy> policies = new LinkedHashMap<>();
            for (final Map.Entry<ClientId, List<ClientPolicy.Grant>> e : byClient.entrySet()) {
                policies.put(e.getKey(), new ClientPolicy(e.getValue()));
            }
            return new InMemoryClientAcl(new ClientAclSnapshot(policies));
        }
    }
}
