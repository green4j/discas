/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.node.membership.MemberInfo;
import io.github.green4j.discas.node.membership.Members;

/**
 * The in-process-transport wiring for a CAS node: just the cluster {@link Members}
 * (node-id-only members -- the in-process transport routes by {@code NodeId} via a
 * shared registry and needs no addresses). The transport-agnostic identity/size
 * lives in {@code NodeConfig}, passed to {@code DisCasNodeFactory} alongside this.
 */
public final class InProcessPeerBootstrap {
    public final Members<MemberInfo> members;

    public InProcessPeerBootstrap(final Members<MemberInfo> members) {
        if (members == null) {
            throw new IllegalArgumentException("members are required");
        }
        this.members = members;
    }
}
