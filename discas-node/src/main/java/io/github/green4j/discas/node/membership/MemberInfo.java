/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;

/**
 * A single cluster member as seen by a {@link Members} list -- the
 * transport-agnostic base carrying just its {@link NodeId}. Transports that need a
 * dial address extend this (see {@link TcpMemberInfo}); an in-process transport
 * uses the base directly (node-id membership only, no host/port).
 */
public class MemberInfo {

    private final NodeId nodeId;

    public MemberInfo(final NodeId nodeId) {
        if (nodeId == null) {
            throw new IllegalArgumentException("nodeId required");
        }
        this.nodeId = nodeId;
    }

    public final NodeId nodeId() {
        return nodeId;
    }

    /**
     * Where this member is reached, for a reload report. The transport-agnostic base has nowhere to
     * point at, so a transport that dials overrides it.
     */
    public String location() {
        return "no address";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return nodeId.equals(((MemberInfo) o).nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "MemberInfo(" + nodeId + ")";
    }
}
