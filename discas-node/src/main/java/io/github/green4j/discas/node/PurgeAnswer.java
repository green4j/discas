/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

/**
 * What a member's storage means for a proposed collection: can this member still resurrect the
 * value the tombstone suppresses?
 *
 * <p>Two answers say it cannot, for the two different reasons a member can be harmless, and the
 * third covers everything else. The distinction between the first two is what keeps one replaced
 * member from blocking every collection in the cluster forever -- the same separation
 * {@link CeilingEvidence} draws between <em>survived somewhere</em> and <em>survived nowhere</em>.
 *
 * <p>Each constant carries an explicit wire {@link #code() byte code}, <b>not</b> the
 * {@code ordinal()}, so reordering or inserting constants never shifts the protocol.
 */
public enum PurgeAnswer {

    /**
     * This member holds state the decision was not about -- a value with no tombstone, a tombstone
     * below the ballot asked about, or a state written above it -- or holds the tombstone in memory
     * without being able to prove it reached the disk.
     *
     * <p>They are one answer because they mean one thing to the coordinator: not now. A member that
     * is merely behind is exactly the replica anti-entropy would copy the old value back off, so it
     * must block, and waiting costs nothing.
     */
    RETAINED((byte) 0),

    /**
     * This member holds no state for the key at all, so it has nothing to resurrect.
     *
     * <p>Not the same as {@link #RETAINED}: a member whose storage was replaced answers this, and
     * without it a single replaced member would block every collection in the cluster forever.
     */
    ABSENT((byte) 1),

    /**
     * This member holds the tombstone at or above the ballot asked about, and forced its log
     * before saying so.
     *
     * <p>The one answer in the protocol that is a claim about a disk. It is made after a force
     * because a member answering off its in-memory state and then losing power comes back holding
     * the <em>value</em> the tombstone suppressed, while everyone else has purged -- and by then
     * there is no tombstone left anywhere to out-vote it.
     */
    HELD((byte) 2);

    private final byte code;

    PurgeAnswer(final byte code) {
        this.code = code;
    }

    /** The wire byte for this answer (not the ordinal). */
    public byte code() {
        return code;
    }

    /** Resolve a wire byte back to an answer, or throw if unknown. */
    public static PurgeAnswer fromCode(final byte code) {
        final PurgeAnswer[] all = values();
        for (int i = 0; i < all.length; i++) {
            if (all[i].code == code) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown purge answer code: " + code);
    }

    /** Whether this answer lets the collection go ahead, as far as one member can say. */
    public boolean permitsCollection() {
        return this != RETAINED;
    }
}
