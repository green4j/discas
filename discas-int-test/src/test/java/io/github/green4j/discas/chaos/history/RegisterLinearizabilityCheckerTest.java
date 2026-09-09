/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos.history;

import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.node.HashedBytes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the checker itself: it must accept genuinely linearizable histories (including
 * concurrent and indeterminate ones) and reject genuinely non-linearizable ones. Without
 * these, a checker that always returns "linearizable" would silently pass the chaos test.
 */
@DisplayName("RegisterLinearizabilityChecker")
class RegisterLinearizabilityCheckerTest {

    private static final HashedBytes KEY = new HashedBytes(new byte[]{1});
    private static final HashedBytes A = new HashedBytes(new byte[]{'a'});
    private static final HashedBytes B = new HashedBytes(new byte[]{'b'});

    private final RegisterLinearizabilityChecker checker = new RegisterLinearizabilityChecker();

    /** A version on the one node these histories pretend to have run against. */
    private static Version v(final int counter) {
        return Version.parse(counter + ":n1");
    }

    // The versions the store would have assigned are not what most of these cases are about, so
    // they are derived from the invoke time: distinct, and ordered the way a sequential run orders
    // them. The cases that are about versions name them instead.

    private static OpRecord put(final HashedBytes value, final long inv, final long ret) {
        return OpRecord.put(KEY, value, v((int) inv + 1), OpRecord.Status.OK, inv, ret);
    }

    private static OpRecord get(final HashedBytes value, final long inv, final long ret) {
        return OpRecord.get(KEY, value, v((int) inv + 1), OpRecord.Status.OK, inv, ret);
    }

    private static OpRecord unknownPut(final HashedBytes value, final long inv, final long ret) {
        return OpRecord.put(KEY, value, null, OpRecord.Status.UNKNOWN, inv, ret);
    }

    private static OpRecord cas(final HashedBytes expected, final HashedBytes desired,
                                final boolean swapped, final long inv, final long ret) {
        return OpRecord.cas(KEY, expected, desired, null, swapped, null, v((int) inv + 1),
                OpRecord.Status.OK, inv, ret);
    }

    @Test
    void acceptsSequentialHistory() {
        assertTrue(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                get(A, 2, 3),
                cas(A, B, true, 4, 5),
                get(B, 6, 7))));
    }

    @Test
    void acceptsConcurrentHistoryWithValidLinearization() {
        // put(a) and put(b) overlap; a later get sees b -> linearize b last.
        assertTrue(checker.linearizableForKey(List.of(
                put(A, 0, 10),
                put(B, 1, 11),
                get(B, 20, 21))));
    }

    @Test
    void rejectsReadOfNeverWrittenValue() {
        // Nothing ever writes B, yet a read returns B after A was written.
        assertFalse(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                get(B, 2, 3))));
    }

    @Test
    void rejectsStaleReadAfterWrite() {
        // put(b) fully precedes a get, but the get still returns the older a.
        assertFalse(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                put(B, 2, 3),
                get(A, 4, 5))));
    }

    @Test
    void rejectsCasThatShouldHaveSwapped() {
        // Value is a with no concurrency, yet cas(expected=a) reports not-swapped.
        assertFalse(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                cas(A, B, false, 2, 3))));
    }

    @Test
    void rejectsCasThatShouldNotHaveSwapped() {
        // Value is b, yet cas(expected=a) claims it swapped.
        assertFalse(checker.linearizableForKey(List.of(
                put(B, 0, 1),
                cas(A, B, true, 2, 3))));
    }

    @Test
    void acceptsUnknownWriteThatExplainsLaterRead() {
        // An UNKNOWN put(b) (lost ack) may have taken effect; a later get sees b.
        assertTrue(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                unknownPut(B, 2, 8),
                get(B, 9, 10))));
    }

    @Test
    void acceptsUnknownWriteThatDidNotTakeEffect() {
        // An UNKNOWN put(b) that never applied; the value stays a.
        assertTrue(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                unknownPut(B, 2, 8),
                get(A, 9, 10))));
    }

    /**
     * The shape that took a chaos run down: a read after the indeterminate write's interval does
     * not see it, and a read after that one does. Legal, because the write is still pending -- the
     * coordinator the client gave up on is free to land it, or a later round is free to adopt the
     * one acceptor that took it. Giving the write a completion edge at the instant the client
     * stopped waiting forces it before both reads and calls this a violation.
     */
    @Test
    void acceptsUnknownWriteThatLandsAfterAReadHasMissedIt() {
        assertTrue(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                unknownPut(B, 2, 8),
                get(A, 9, 10),
                get(B, 11, 12))));
    }

    /**
     * The other side of that relaxation: a pending write explains one later value, not any value.
     * Without this, dropping the completion edge could be mistaken for dropping the constraint.
     */
    @Test
    void rejectsValueThatNoPendingWriteCanExplain() {
        // The UNKNOWN put(b) is spent explaining get(b); nothing is left to put a back.
        assertFalse(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                unknownPut(B, 2, 8),
                get(B, 9, 10),
                get(A, 11, 12))));
    }

    /**
     * What the fence is for. The pending CAS is fenced on v1, and by the time the value is a again
     * the register has moved to v2, so there is no instant left at which that CAS could apply --
     * even though the bytes it expected are back. A checker that matched the fence on bytes would
     * place it after the second put and call this linearizable.
     */
    @Test
    void rejectsUnknownFencedCasThatWouldHaveToCrossAVersion() {
        assertFalse(checker.linearizableForKey(List.of(
                OpRecord.put(KEY, A, v(1), OpRecord.Status.OK, 0, 1),
                OpRecord.cas(KEY, A, B, v(1), false, null, null, OpRecord.Status.UNKNOWN, 2, 8),
                OpRecord.put(KEY, A, v(2), OpRecord.Status.OK, 9, 10),
                OpRecord.get(KEY, B, v(3), OpRecord.Status.OK, 11, 12))));
    }

    /** The same pending CAS, against a register that never moved off its fence: it may still land. */
    @Test
    void acceptsUnknownFencedCasThatLandsWhileItsFenceStillHolds() {
        assertTrue(checker.linearizableForKey(List.of(
                OpRecord.put(KEY, A, v(1), OpRecord.Status.OK, 0, 1),
                OpRecord.cas(KEY, A, B, v(1), false, null, null, OpRecord.Status.UNKNOWN, 2, 8),
                OpRecord.get(KEY, A, v(1), OpRecord.Status.OK, 9, 10),
                OpRecord.get(KEY, B, v(4), OpRecord.Status.OK, 11, 12))));
    }

    /**
     * A definite read that sees a version the register has already left. The value alone says
     * nothing here -- it is the same bytes either way -- so only the version catches it.
     */
    @Test
    void rejectsReadOfAVersionTheRegisterHasLeft() {
        assertFalse(checker.linearizableForKey(List.of(
                OpRecord.put(KEY, A, v(1), OpRecord.Status.OK, 0, 1),
                OpRecord.put(KEY, A, v(5), OpRecord.Status.OK, 2, 3),
                OpRecord.get(KEY, A, v(1), OpRecord.Status.OK, 4, 5))));
    }

    /**
     * Collecting a tombstone removes the key outright, so a read of an absent key legitimately
     * reports {@link Version#INITIAL} after the register had reached a real version. That reset is
     * the one way a version goes down without anything being wrong.
     */
    @Test
    void acceptsAnAbsentKeyReadingAsInitialAfterItsTombstoneWasCollected() {
        assertTrue(checker.linearizableForKey(List.of(
                OpRecord.put(KEY, A, v(1), OpRecord.Status.OK, 0, 1),
                OpRecord.delete(KEY, v(2), OpRecord.Status.OK, 2, 3),
                OpRecord.get(KEY, null, Version.INITIAL, OpRecord.Status.OK, 4, 5))));
    }
}
