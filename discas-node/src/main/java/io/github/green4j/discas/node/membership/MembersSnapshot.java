/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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

    /**
     * One line for the reload report: every member and where it is. Sorted by id, because the map's
     * own order comes from {@code Properties} and has none, and a report an operator cannot compare
     * with the last one tells them nothing.
     *
     * <p>Addresses are cluster topology, not a secret -- and they are the whole point of reading
     * the report: a members file that applied with the wrong port is indistinguishable from one
     * that applied correctly until the node fails to reach a peer.
     */
    public String summary() {
        final List<NodeId> ids = new ArrayList<>(byId.keySet());
        Collections.sort(ids);
        final StringBuilder sb = new StringBuilder();
        sb.append(ids.size()).append(ids.size() == 1 ? " node: " : " nodes: ");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(ids.get(i).value()).append('=').append(byId.get(ids.get(i)).location());
        }
        return sb.toString();
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
