/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos.history;

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

    private static OpRecord put(final HashedBytes v, final long inv, final long ret) {
        return OpRecord.put(KEY, v, OpRecord.Status.OK, inv, ret);
    }

    private static OpRecord get(final HashedBytes v, final long inv, final long ret) {
        return OpRecord.get(KEY, v, OpRecord.Status.OK, inv, ret);
    }

    private static OpRecord cas(final HashedBytes exp, final HashedBytes des, final boolean swapped,
                                final long inv, final long ret) {
        return OpRecord.cas(KEY, exp, des, swapped, null, OpRecord.Status.OK, inv, ret);
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
                OpRecord.put(KEY, B, OpRecord.Status.UNKNOWN, 2, 8),
                get(B, 9, 10))));
    }

    @Test
    void acceptsUnknownWriteThatDidNotTakeEffect() {
        // An UNKNOWN put(b) that never applied; the value stays a.
        assertTrue(checker.linearizableForKey(List.of(
                put(A, 0, 1),
                OpRecord.put(KEY, B, OpRecord.Status.UNKNOWN, 2, 8),
                get(A, 9, 10))));
    }
}
