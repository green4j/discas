/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

/**
 * What a member's answer to a ceiling request is worth to the node asking it.
 *
 * <p>Ordered by strength. Each constant carries an explicit wire {@link #code() byte code},
 * <b>not</b> the {@code ordinal()}, so a constant inserted to keep that order never shifts the
 * protocol.
 *
 * @see CeilingRecovery
 */
public enum CeilingEvidence {

    /**
     * Honest numbers that prove nothing. The member holds no state and has not yet recovered a
     * floor, so its zeros are what it does not know rather than what did not happen. Counting them
     * as evidence is how a floor ends up below a promise somebody made.
     */
    NONE((byte) 0),

    /**
     * The member's bounds run unbroken from before the asker lost its state -- it either never lost
     * its own, or it adopted a floor derived from members that had not. Its ceiling is therefore at
     * or above any ballot it ever promised at, which is what makes it useful in a quorum
     * intersection.
     */
    WITNESS((byte) 1),

    /**
     * A witness that additionally established, at a moment when every member had already answered
     * it, that <b>no member anywhere held any bound</b>. That fact is about the world rather than
     * about the asker, and it is transferable: every member that answered that sweep was by then in
     * its current life, so nothing any of them may have promised before it can ever be completed.
     *
     * <p>Without this, a cluster starting from nothing deadlocks in the ordinary case: whichever
     * member reaches the conclusion first begins serving, its ballots carry its ceiling above zero,
     * and the rest are left with one witness where two are needed and a "nothing anywhere" test
     * that no longer holds.
     */
    NO_HISTORY((byte) 2);

    private final byte code;

    CeilingEvidence(final byte code) {
        this.code = code;
    }

    /** The wire byte for this evidence (not the ordinal). */
    public byte code() {
        return code;
    }

    /** Resolve a wire byte back to its evidence, or throw if unknown. */
    public static CeilingEvidence fromCode(final byte code) {
        final CeilingEvidence[] all = values();
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown ceiling evidence code: " + code);
    }

    /** Whether an answer of this kind can stand in a quorum intersection. */
    boolean isWitness() {
        return this != NONE;
    }
}
