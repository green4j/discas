/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.PeerMessage;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Asynchronous transport between cluster peers. An implementation runs on the node's event loop and
 * delivers best-effort: nothing here retries or times out, which is the caller's job.
 */
public interface PeerTransport extends AutoCloseable {

    /** Send to one peer. Non-blocking and fire-and-forget. */
    void send(NodeId targetNodeId, PeerMessage message);

    /** Register the handler every inbound message is delivered to, on the event loop. */
    void register(Consumer<PeerMessage> handler);

    /** Every other member's id. */
    List<NodeId> peers();

    /**
     * Total cluster size {@code N} including self -- the quorum basis.
     * <p>
     * Implementations MUST return a value frozen at startup. It must NOT be derived from
     * the live {@link #peers()} set: {@code Proposer} captures this once and computes
     * {@code N/2 + 1} from it, so a value that drifts with connectivity would silently
     * shrink the quorum and break the safety invariant. Abstract, because there is no safe default.
     */
    int clusterSize();

    /**
     * Bind the source of this node's forced promise ceiling, which the handshake carries so a peer
     * can check the one invariant a node cannot check about itself: <em>storage may never come back
     * holding less than it left with</em>.
     * <p>
     * Bound rather than passed to a constructor, like {@link #register}, and for the same reason:
     * the transport is built before the node that owns the store. Read on every handshake because
     * the ceiling rises. Until it is bound -- and while replay has not established one -- the
     * source reports {@code UNKNOWN} and no handshake is attempted or accepted, since a connection
     * is handshaked once and there is no second chance to ask.
     * <p>
     * Default no-op: an implementation with no handshake has nowhere to carry the claim, and a
     * member it connects is one it could not have checked anyway.
     */
    default void bindPromiseCeiling(final LongSupplier source) {
        // no-op
    }

    /**
     * Claim: this node has not finished replaying and has nothing to say about its storage yet.
     * <p>
     * <b>Refused</b>, not admitted. A connection is handshaked once, so admitting a peer that could
     * not be checked leaves it unchecked for that connection's life -- and there is no deadlock in
     * refusing, because replay always ends. The dialer's reconnect backoff is what waits.
     */
    long PROMISE_CEILING_REPLAYING = -1L;

    /**
     * Claim: this node has replayed, and its log does not run unbroken from where it starts, so the
     * ceiling it holds is not one it can prove.
     * <p>
     * <b>Admitted</b>, and this is the distinction that matters. Such a node is on its way to
     * asking a quorum for a promise floor and refuses to serve anything until it has one -- so it
     * asserts nothing, and refusing it would only stop it asking, leaving it stuck forever. Its
     * claim is neither compared nor remembered: a node that cannot prove its ceiling cannot be
     * evidence about it either.
     * <p>
     * The fault this guard exists for cannot hide here: a directory restored from a copy is
     * byte-continuous, so it replays cleanly and <em>does</em> prove a ceiling. It has to make a
     * real claim, which is what gets it caught.
     */
    long PROMISE_CEILING_UNPROVEN = -2L;

    /** Release transport resources. */
    @Override
    default void close() {
        // no-op
    }
}
