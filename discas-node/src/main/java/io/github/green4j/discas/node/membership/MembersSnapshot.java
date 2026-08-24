/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * An immutable point-in-time view of the cluster members, parameterized by the
 * transport's {@link MemberInfo} subtype ({@link TcpMemberInfo} for TCP, plain
 * {@link MemberInfo} for in-process).
 * <p>
 * Value-based {@link #equals(Object)} lets a reloader skip publishing when the
 * newly-parsed content is identical to the current snapshot.
 */
public final class MembersSnapshot<M extends MemberInfo> {

    private final Map<NodeId, M> byId;

    public MembersSnapshot(final Map<NodeId, M> members) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("members required");
        }
        this.byId = Collections.unmodifiableMap(new LinkedHashMap<>(members));
    }

    /** All members keyed by {@link NodeId}, including this node's own record. */
    public Map<NodeId, M> byId() {
        return byId;
    }

    public Set<NodeId> ids() {
        return byId.keySet();
    }

    public M get(final NodeId nodeId) {
        return byId.get(nodeId);
    }

    public boolean contains(final NodeId nodeId) {
        return byId.containsKey(nodeId);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MembersSnapshot)) {
            return false;
        }
        return byId.equals(((MembersSnapshot<?>) o).byId);
    }

    @Override
    public int hashCode() {
        return byId.hashCode();
    }

    @Override
    public String toString() {
        return "MembersSnapshot" + byId.values();
    }
}
