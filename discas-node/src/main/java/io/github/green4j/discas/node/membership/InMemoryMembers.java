/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Static, in-memory {@link Members} list for tests, examples, and simple
 * deployments. Membership is fixed at construction and never changes.
 */
public final class InMemoryMembers<M extends MemberInfo> implements Members<M> {

    private final MembersSnapshot<M> snapshot;

    public InMemoryMembers(final Map<NodeId, M> members) {
        this.snapshot = new MembersSnapshot<>(members);
    }

    /**
     * TCP members from a {@code NodeId -> address} map. Addresses are stored
     * unresolved (host + port).
     */
    public static InMemoryMembers<TcpMemberInfo> ofTcp(
            final Map<NodeId, InetSocketAddress> addresses) {
        final Map<NodeId, TcpMemberInfo> members = new LinkedHashMap<>();
        for (final Map.Entry<NodeId, InetSocketAddress> e : addresses.entrySet()) {
            members.put(e.getKey(), new TcpMemberInfo(e.getKey(), e.getValue()));
        }
        return new InMemoryMembers<>(members);
    }

    /** Node-id-only members (no address) -- for the in-process transport. */
    public static InMemoryMembers<MemberInfo> ofNodes(final Collection<NodeId> ids) {
        final Map<NodeId, MemberInfo> members = new LinkedHashMap<>();
        for (final NodeId id : ids) {
            members.put(id, new MemberInfo(id));
        }
        return new InMemoryMembers<>(members);
    }

    @Override
    public MembersSnapshot<M> snapshot() {
        return snapshot;
    }

    @Override
    public void addListener(final Consumer<MembersSnapshot<M>> listener) {
        // Replay-on-subscribe for a uniform consumer path: deliver the fixed snapshot
        // as the first (and only) update. The list never changes afterwards.
        listener.accept(snapshot);
    }
}
