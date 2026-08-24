/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.transport.FrameCodec;

/**
 * Status of a {@link FrameCodec#TYPE_PEER_HELLO_RESP}. Each constant carries an
 * explicit wire {@link #code() byte code} set in the constructor -- the wire value
 * is <b>not</b> the enum {@code ordinal()}, so reordering or inserting
 * constants never shifts the protocol. The receiver writes {@link #code()} followed
 * by a length-prefixed {@code cause} string (see the {@code CAUSE_*} constants,
 * used to disambiguate the several {@link #IDENTITY_MISMATCH} situations).
 */
public enum PeerHelloRespStatus {
    OK((byte) 0),
    PROTOCOL_MISMATCH((byte) 1),
    CLUSTER_MISMATCH((byte) 2),
    /** Node id does not match the identity we expected / the peer claims. */
    IDENTITY_MISMATCH((byte) 3),
    UNKNOWN_PEER((byte) 4),
    /**
     * The {@code epochMs} in the HELLO is further from ours than the accepted window, in either
     * direction.
     * <p>
     * The window is a replay defence -- a captured HELLO cannot be re-sent later -- but the name
     * says what was observed rather than what it is taken to mean, because the common cause is the
     * other one: a peer whose clock is wrong trips it while doing nothing wrong, which is why it
     * raises {@code CONFIG/CLOCK_UNUSABLE} and not a security state. It is also why this is not
     * called {@code REPLAY} next to {@link #NOT_REPLAYED}, which is about replaying a log and has
     * nothing to do with it.
     */
    HELLO_TIMESTAMP_SKEW((byte) 5),
    // Code 6 is retired (it was SERVER_BUSY, never sent). The gap stays: codes are explicit
    // so that removing a constant never shifts the ones after it.
    /** The peer's cluster size (quorum basis) differs from ours -- reconfiguration guard. */
    CLUSTER_SIZE_MISMATCH((byte) 7),
    /**
     * <b>Retired: never sent.</b> A {@code node_id} presenting a different {@code incarnation_id}
     * than last time has had its durable state replaced -- a disk swap or a wipe, which look
     * identical on the wire.
     * <p>
     * Not a refusal: the promises are unrecoverable but the <em>bound</em> on them is not, and a
     * member starting without state asks a quorum for that bound before it serves anything.
     * Refusing would only keep it from asking, and would refuse a deliberate disk replacement with
     * no way to lift it -- {@code node_id} names a role meant to outlive the hardware under it.
     * <p>
     * The constant stays so that an older node's rejection still decodes, and so the code is never
     * reused for something else.
     */
    INCARNATION_CHANGED((byte) 8),
    /**
     * Two member identities presenting the same {@code incarnation_id} -- one node's data directory
     * copied onto another.
     * <p>
     * The opposite error to {@link #INCARNATION_CHANGED}, and the one that is still refused: that
     * one means state was lost, which recovery covers, while this one means state was duplicated,
     * which it cannot. A clone is not a blank node being seeded; it arrives holding the original's
     * promises and its reserved ballot range, and asserts them as its own. Anti-entropy is how a
     * replacement gets state.
     */
    INCARNATION_DUPLICATED((byte) 9),
    /**
     * One of the two ends cannot yet state its promise ceiling, because it has not finished
     * replaying its own log.
     * <p>
     * Transient, and the only refusal here that resolves itself: the dialer's reconnect backoff
     * retries, and the handshake succeeds once replay has established the ceiling. It exists
     * because a connection is handshaked <em>once</em> -- admitting a peer that could not be
     * checked would leave it unchecked for the life of that connection, and replay runs in batches
     * on the same loop that dials, so the overlap is ordinary rather than rare.
     */
    NOT_REPLAYED((byte) 10),
    /**
     * The same storage has come back holding <em>less</em> than it left with: this member claims a
     * promise ceiling lower than one it already proved under the same {@code incarnation_id}.
     * <p>
     * A ceiling is forced before it is promised against and only ever rises within a run, so this
     * cannot happen to a member that merely restarted, lost an unforced tail, or was legitimately
     * wiped (that one arrives with a new incarnation and is measured against nothing). What it does
     * catch is the fault nothing local can see -- a volume restored from a copy, byte-continuous and
     * older, whose own replay reports success.
     * <p>
     * Refused rather than reported, and unlike {@link #INCARNATION_CHANGED} this one cannot be
     * softened by recovery: such a member has not forgotten promises it can ask the cluster to
     * bound, it is holding <em>stale state it will assert as current</em>. Once tombstones are
     * collected there is nothing left to out-vote it, and what an operator does next is never to
     * lift this.
     */
    CEILING_ROLLED_BACK((byte) 11);

    /** {@link #IDENTITY_MISMATCH} cause: the peer claimed our own node id. */
    public static final String CAUSE_SELF_CLAIM = "self-claim";
    /** {@link #IDENTITY_MISMATCH} cause: outbound expected peer X but HELLO said Y. */
    public static final String CAUSE_EXPECTED_PEER = "expected-peer";
    /** {@link #IDENTITY_MISMATCH} cause: already-authenticated channel now claims a different id. */
    public static final String CAUSE_AUTHENTICATED_PEER = "authenticated-peer";
    /** {@link #IDENTITY_MISMATCH} cause: cert SAN node/cluster id != HELLO values. */
    public static final String CAUSE_CERT_SAN = "cert-san";

    /** {@link #NOT_REPLAYED} cause: the refusing node is the one that has not replayed. */
    public static final String CAUSE_OURS = "ours";
    /** {@link #NOT_REPLAYED} cause: the peer could not state a ceiling. */
    public static final String CAUSE_THEIRS = "theirs";

    private final byte code;

    PeerHelloRespStatus(final byte code) {
        this.code = code;
    }

    /** The wire byte for this status (not the ordinal). */
    public byte code() {
        return code;
    }

    /** Resolve a wire byte back to its status, or throw if unknown. */
    public static PeerHelloRespStatus fromCode(final byte code) {
        final PeerHelloRespStatus[] all = values();
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown PEER_HELLO_RESP status code: " + code);
    }
}
