/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

/**
 * Why a client operation failed, carried on every failure response alongside the human-readable
 * {@code error} text. The text is for people and may be rephrased at any time; this code is what
 * callers branch on.
 * <p>
 * Two distinctions matter. <em>Whose fault, and can retrying help</em>: {@link #ACCESS_DENIED} and
 * {@link #INVALID_ARGUMENT} are caller errors that will fail identically forever, while the rest
 * are node-side and may succeed later; of those, {@link #NOT_READY} and
 * {@link #NO_QUORUM_AT_COORDINATOR} are properties of <em>one node</em> and so are worth re-asking
 * elsewhere at once.
 * <p>
 * <em>Is the outcome known?</em> Every code is determinate -- the operation did not happen --
 * except {@link #UNAVAILABLE}, where the write may still be applied by a proposal already in
 * flight. That is the code a caller cannot resolve by re-reading, and the reason version-fenced
 * writes exist. {@link #PROPOSAL_EXPIRED} bounds it: past that horizon a proposal is no longer
 * driven, so an indeterminate answer has an end.
 * <p>
 * Wire form is the one-byte {@link #code()}; unknown values decode to {@link #INTERNAL} so a
 * newer node adding a code cannot make an older client misread a failure as a success. The byte is
 * declared rather than derived from {@code ordinal()}, so reordering the constants cannot change
 * what travels.
 */
public enum ClientErrorCode {

    /** No failure. Present on successful responses. */
    NONE((byte) 0),

    /** Authorization refused the operation for this client and key. Retrying will not help. */
    ACCESS_DENIED((byte) 1),

    /** The request itself is malformed -- currently a key or value over its {@code KvLimits} cap. */
    INVALID_ARGUMENT((byte) 2),

    /** The node failed to complete the operation (exception on the node side). */
    INTERNAL((byte) 3),

    /**
     * <b>The outcome is unknown.</b> The round got as far as Accept and did not complete in time,
     * or the coordinator stopped answering after taking the request. Some acceptors may hold the
     * proposal, and a later proposer can adopt and complete it -- so this is not "the write did
     * not happen", it is "the write may still happen, without you".
     * <p>
     * This is the one code a caller cannot resolve by itself. For a write fenced on a
     * {@code Version} it does not have to: a duplicate carries an overtaken ballot and is
     * rejected, so the client re-sends until it gets a definite answer or runs out of deadline.
     * For an unfenced write -- {@code put} and the unfenced {@code delete} -- there is no safe
     * automatic recovery, and the caller is told so rather than being handed a silent retry.
     * <p>
     * Not failed over automatically for unfenced writes: each further attempt costs another
     * proposer round timeout, and a stalled accept says nothing about <em>this</em> coordinator's
     * connectivity, so the next one is as likely to stall.
     */
    UNAVAILABLE((byte) 4),

    /**
     * This node in particular cannot serve requests yet -- it is still replaying its log. Says
     * nothing about the cluster, and the refusal is immediate and cheap, so a client should try
     * another node at once rather than wait out a timeout against this one.
     */
    NOT_READY((byte) 5),

    /**
     * This coordinator could not reach a majority: its prepare timed out without a quorum of
     * promises. A property of <em>this node's</em> connectivity, not of the cluster -- under an
     * asymmetric partition a client that can reach the healthy majority is served perfectly well by
     * a different coordinator.
     * <p>
     * Worth switching coordinators on at once, unlike {@link #UNAVAILABLE}: the round is abandoned
     * as soon as the promises fail to arrive, so the attempt cost nothing like a round timeout.
     */
    NO_QUORUM_AT_COORDINATOR((byte) 6),

    /**
     * The round lost a ballot duel and exhausted its retries: another proposer was writing the
     * same key and kept promising above it. <b>Nothing was accepted</b> -- a NACKed prepare never
     * reaches Accept -- so unlike {@link #UNAVAILABLE} the outcome here is determinate, and a
     * caller may re-issue without wondering whether its first attempt is still in flight.
     * <p>
     * <em>Not</em> failed over automatically: contention is a property of the key,
     * not of the coordinator, and moving to another node adds a competitor for the same register
     * rather than removing one -- while costing a full round to find out.
     */
    BALLOT_LOST((byte) 7),

    /**
     * The operation outlived the coordinator's {@code proposalExpiry} and was abandoned before
     * anything was proposed. <b>Determinate: the write did not happen</b> -- the expiry is checked
     * between prepare and the Accept broadcast, so no acceptor ever saw the value.
     * <p>
     * This code is the visible half of the bound on {@link #UNAVAILABLE}. An indeterminate answer
     * only means something if "may still be applied" has an end; {@code proposalExpiry} is that
     * end, and a caller that sees this code knows the deadline was reached with the write firmly on
     * the "did not happen" side of it, rather than being left to guess.
     * <p>
     * Not failed over automatically for unfenced writes. The expiry says the operation was slow,
     * which is not evidence about <em>this</em> coordinator, and another one would cost a full
     * round to reach the same verdict. A version-fenced write does keep walking coordinators,
     * because a duplicate provably cannot apply.
     */
    PROPOSAL_EXPIRED((byte) 8),

    /**
     * A node has no room for the pair: storing it would take its key/value bytes past the share of
     * the heap it was given. Refusing is how a member avoids being killed by the JVM, which is the
     * one failure this store cannot survive gracefully -- a member that dies takes its promises with
     * it.
     * <p>
     * <b>Determinate when it comes from the coordinator</b>, which is the usual case: the check runs
     * before the round, so nothing was proposed and no acceptor saw the value. A peer running out of
     * room mid-round does not produce this code -- it NACKs, the round fails, and the caller is told
     * {@link #UNAVAILABLE}, because by then the outcome genuinely is unknown.
     * <p>
     * <b>Not failed over.</b> Every replica holds the same keys, so a node that is full is a cluster
     * that is full; walking coordinators would spend the deadline discovering that. Retrying helps
     * only after something is deleted, and a delete is never refused for want of room -- otherwise a
     * full store would have no way back.
     */
    STORE_FULL((byte) 9);

    private final byte code;

    ClientErrorCode(final byte code) {
        this.code = code;
    }

    /** The wire byte. */
    public byte code() {
        return code;
    }

    /**
     * Resolve a wire byte back to its code, mapping anything unrecognised to {@link #INTERNAL}.
     * <p>
     * Lenient, unlike the enums gated by the handshake's protocol version: codes are added without
     * a version bump, so a newer node legitimately sends a byte an older client has never heard of,
     * and throwing would drop the connection over a failure response it merely could not name.
     * {@link #INTERNAL} is the safe landing -- still a failure, so it cannot be misread as success,
     * and non-retryable, so an unknown code cannot send a client into a loop.
     */
    public static ClientErrorCode fromCode(final byte code) {
        final ClientErrorCode[] all = values();
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        return INTERNAL;
    }
}
