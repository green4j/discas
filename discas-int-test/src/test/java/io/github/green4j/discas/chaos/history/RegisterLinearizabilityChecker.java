/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos.history;

import io.github.green4j.discas.node.HashedBytes;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A history-based linearizability checker for independent per-key registers.
 * <p>
 * Because CASPaxos state is a set of independent single-key registers, a global history
 * is linearizable iff each per-key sub-history is linearizable against a register model
 * ({@code get}/{@code put}/{@code cas(expected,desired)}/{@code delete}). This is the
 * Wing&amp;Gong search: repeatedly pick a <i>minimal</i> operation (one that no other
 * pending operation must precede in real time), apply it to the model if the sequential
 * spec permits, and recurse; backtrack on failure. Visited (pending-set, model-value)
 * states are memoised so the search does not re-explore a dead end.
 * <p>
 * Indeterminate ({@link OpRecord.Status#UNKNOWN}) mutating operations -- e.g. a write
 * whose ack was lost -- are explored <i>both</i> ways (took effect / did not), so the
 * checker never reports a false violation for an operation whose fate is genuinely
 * unknown. Reads with no observed value impose no constraint and are dropped.
 */
public final class RegisterLinearizabilityChecker {

    /** Outcome of a check: linearizable, or the first offending key with a message. */
    public static final class Result {
        private final boolean linearizable;
        private final String message;

        private Result(final boolean linearizable, final String message) {
            this.linearizable = linearizable;
            this.message = message;
        }

        public boolean linearizable() {
            return linearizable;
        }

        public String message() {
            return message;
        }
    }

    private static final Result OK = new Result(true, "linearizable");

    /**
     * Checks a full multi-key history. Returns the first non-linearizable key (with a
     * witness message) or an OK result.
     */
    public Result check(final List<OpRecord> history) {
        final Map<HashedBytes, List<OpRecord>> byKey = new HashMap<>();
        for (final OpRecord op : history) {
            byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }
        for (final Map.Entry<HashedBytes, List<OpRecord>> e : byKey.entrySet()) {
            if (!linearizableForKey(e.getValue())) {
                return new Result(false,
                        "key " + e.getKey() + " is not linearizable; ops=" + e.getValue());
            }
        }
        return OK;
    }

    /** Checks a single key's sub-history (public so unit tests can target it directly). */
    public boolean linearizableForKey(final List<OpRecord> opsIn) {
        final OpRecord[] ops = opsIn.toArray(new OpRecord[0]);
        final int n = ops.length;
        if (n == 0) {
            return true;
        }
        return search(ops, new BitSet(n), n, null, new HashSet<>());
    }

    private boolean search(final OpRecord[] ops, final BitSet done, final int remaining,
                           final HashedBytes model, final Set<MemoKey> visited) {
        if (remaining == 0) {
            return true;
        }
        if (!visited.add(new MemoKey((BitSet) done.clone(), model))) {
            return false; // this (pending-set, value) state already failed
        }

        // Minimal candidates: op i whose invoke is <= the earliest return among all
        // still-pending ops (nothing pending must strictly precede it in real time).
        long minReturn = Long.MAX_VALUE;
        for (int i = 0; i < ops.length; i++) {
            if (!done.get(i) && ops[i].returnNanos() < minReturn) {
                minReturn = ops[i].returnNanos();
            }
        }

        for (int i = 0; i < ops.length; i++) {
            if (done.get(i) || ops[i].invokeNanos() > minReturn) {
                continue;
            }
            final OpRecord op = ops[i];
            done.set(i);
            if (op.status() == OpRecord.Status.UNKNOWN && op.mutating()) {
                // Branch 1: it took effect.
                if (search(ops, done, remaining - 1, effectIfHappened(op, model), visited)) {
                    done.clear(i);
                    return true;
                }
                // Branch 2: it did not.
                if (search(ops, done, remaining - 1, model, visited)) {
                    done.clear(i);
                    return true;
                }
            } else {
                final Apply applied = applyDefinite(op, model);
                if (applied.legal && search(ops, done, remaining - 1, applied.model, visited)) {
                    done.clear(i);
                    return true;
                }
            }
            done.clear(i);
        }
        return false;
    }

    /** The register value an UNKNOWN mutating op would leave behind if it took effect. */
    private static HashedBytes effectIfHappened(final OpRecord op, final HashedBytes model) {
        switch (op.kind()) {
            case PUT:
                return op.arg();
            case DELETE:
                return null;
            case CAS:
                return equalsNullable(model, op.arg()) ? op.desired() : model;
            default:
                return model;
        }
    }

    private static final class Apply {
        final boolean legal;
        final HashedBytes model;

        Apply(final boolean legal, final HashedBytes model) {
            this.legal = legal;
            this.model = model;
        }
    }

    private static Apply applyDefinite(final OpRecord op, final HashedBytes model) {
        // FAIL and UNKNOWN reads impose no constraint and leave the model unchanged.
        if (op.status() == OpRecord.Status.FAIL) {
            return new Apply(true, model);
        }
        switch (op.kind()) {
            case GET:
                if (op.status() == OpRecord.Status.UNKNOWN) {
                    return new Apply(true, model); // no observed value -> no constraint
                }
                return new Apply(equalsNullable(model, op.observed()), model);
            case PUT:
                return new Apply(true, op.arg());
            case DELETE:
                return new Apply(true, null);
            case CAS:
                if (op.swapped()) {
                    return new Apply(equalsNullable(model, op.arg()), op.desired());
                }
                return new Apply(!equalsNullable(model, op.arg()), model);
            default:
                return new Apply(true, model);
        }
    }

    private static boolean equalsNullable(final HashedBytes x, final HashedBytes y) {
        return x == null ? y == null : x.equals(y);
    }

    private static final class MemoKey {
        private final BitSet done;
        private final HashedBytes model;

        MemoKey(final BitSet done, final HashedBytes model) {
            this.done = done;
            this.model = model;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof MemoKey)) {
                return false;
            }
            final MemoKey other = (MemoKey) o;
            return done.equals(other.done) && Objects.equals(model, other.model);
        }

        @Override
        public int hashCode() {
            return done.hashCode() * 31 + Objects.hashCode(model);
        }
    }
}
