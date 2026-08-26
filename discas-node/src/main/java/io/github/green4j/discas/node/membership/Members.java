/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.io.Reloadable;

/**
 * Locally-verifiable list of cluster members. Each peer consults its own
 * {@code Members} during the PEER_HELLO handshake -- there is no coordinator in
 * the auth path, preserving discas's leaderless, low-latency properties.
 * <p>
 * A member proves it belongs to this cluster by presenting a node id that
 * appears in the current {@link #snapshot()} (and, under mTLS, a certificate SAN
 * matching that id). The cluster identity itself is <b>not</b>
 * stored here -- each node receives its own {@code cluster_id} at startup (env /
 * system property) and checks the handshake against that.
 * <p>
 * Topology is semi-static (no gossip): members are added, decommissioned, or
 * re-addressed by editing the underlying source. It is a {@link Reloadable} of an
 * immutable {@link MembersSnapshot}: read the current list via {@link #snapshot()},
 * and/or subscribe via {@code addListener} for replay-on-subscribe plus every reload
 * (a {@code Consumer<MembersSnapshot>}).
 * <p>
 * Implementations: {@link InMemoryMembers} (static) and {@link FileMembers}
 * (file-backed, re-read on request).
 */
public interface Members<M extends MemberInfo> extends Reloadable<MembersSnapshot<M>> {
}
