/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos.history;

import io.github.green4j.discas.client.Version;
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
 *
 * <h2>An indeterminate write has no return</h2>
 * The moment the client stopped waiting is not the moment the operation ended. A coordinator
 * the caller gave up on keeps driving its proposal, and one that dies having reached a single
 * acceptor leaves an accepted-but-unchosen value that any later round's prepare quorum may
 * adopt. So an {@code UNKNOWN} mutation can take effect arbitrarily far in the future, and
 * giving it a completion edge at {@link OpRecord#returnNanos()} would force it to linearize
 * before everything invoked afterwards -- reporting "the register went back in time" for the
 * ordinary case of a lost write landing late. It is treated as permanently pending instead,
 * which is what {@code :info} means in Jepsen and why it is modelled the same way here.
 * <p>
 * The store does bound one case: {@link io.github.green4j.discas.node.RoundFailure#PROPOSAL_EXPIRED}
 * is raised only between prepare and the accept broadcast, so nothing was proposed and the write
 * can never apply. {@link HistoryRecorder} cannot use that -- it collapses every exception into
 * {@code UNKNOWN} -- so the weaker model is the only sound one from a recorded history.
 *
 * <h2>The register is a value and a version</h2>
 * A {@code CAS} here fences on a {@link Version}, as the store's does, and the model carries the
 * version alongside the value. Matching a fence on bytes instead would be strictly weaker, and
 * weakest exactly where it matters: a pending {@code CAS} that may land at any time could otherwise
 * be placed after any later state whose bytes happen to match again, so
 * {@code put(a); CAS(a->b) UNKNOWN; put(a); get(b)} would pass although no fenced write could have
 * produced it. Versions also order commits, so a definite operation that observes a version below
 * the one already reached rules that ordering out.
 * <p>
 * A version is unknown only after an indeterminate write took effect: it committed at a ballot the
 * client never learned. The model then carries a null version, every fence is permitted against it,
 * and the next definite read pins it again.
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
        return search(ops, returnBounds(ops), new BitSet(n), n, State.UNWRITTEN, new HashSet<>());
    }

    /** The register as one linearization believes it to be. */
    private static final class State {
        /** A key no operation has committed to yet: no value, and the version before every commit. */
        static final State UNWRITTEN = new State(null, Version.INITIAL);

        final HashedBytes value;
        /** Null when an indeterminate write took effect: it committed at a ballot nobody recorded. */
        final Version version;

        State(final HashedBytes value, final Version version) {
            this.value = value;
            this.version = version;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof State)) {
                return false;
            }
            final State other = (State) o;
            return Objects.equals(value, other.value) && Objects.equals(version, other.version);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value) * 31 + Objects.hashCode(version);
        }
    }

    /**
     * The real-time instant by which each operation must have linearized. That is its return, except
     * for an indeterminate mutation, which has none: see the class javadoc on why the client giving
     * up does not end the operation.
     */
    private static long[] returnBounds(final OpRecord[] ops) {
        final long[] bounds = new long[ops.length];
        for (int i = 0; i < ops.length; i++) {
            final OpRecord op = ops[i];
            bounds[i] = op.status() == OpRecord.Status.UNKNOWN && op.mutating()
                    ? Long.MAX_VALUE : op.returnNanos();
        }
        return bounds;
    }

    private boolean search(final OpRecord[] ops, final long[] returnBounds,
                           final BitSet done, final int remaining,
                           final State model, final Set<MemoKey> visited) {
        if (remaining == 0) {
            return true;
        }
        if (!visited.add(new MemoKey((BitSet) done.clone(), model))) {
            return false; // this (pending-set, register state) already failed
        }

        // Minimal candidates: op i whose invoke is <= the earliest return among all
        // still-pending ops (nothing pending must strictly precede it in real time).
        long minReturn = Long.MAX_VALUE;
        for (int i = 0; i < ops.length; i++) {
            if (!done.get(i) && returnBounds[i] < minReturn) {
                minReturn = returnBounds[i];
            }
        }

        for (int i = 0; i < ops.length; i++) {
            if (done.get(i) || ops[i].invokeNanos() > minReturn) {
                continue;
            }
            final OpRecord op = ops[i];
            done.set(i);
            if (op.status() == OpRecord.Status.UNKNOWN && op.mutating()) {
                // Branch 1: it took effect -- where it could have. A fenced write could not have
                // applied to a register that was not at its fence, so that branch does not exist.
                final State applied = effectIfHappened(op, model);
                if (applied != null
                        && search(ops, returnBounds, done, remaining - 1, applied, visited)) {
                    done.clear(i);
                    return true;
                }
                // Branch 2: it did not.
                if (search(ops, returnBounds, done, remaining - 1, model, visited)) {
                    done.clear(i);
                    return true;
                }
            } else {
                final Apply applied = applyDefinite(op, model);
                if (applied.legal && search(ops, returnBounds, done, remaining - 1,
                        applied.model, visited)) {
                    done.clear(i);
                    return true;
                }
            }
            done.clear(i);
        }
        return false;
    }

    /**
     * The register an UNKNOWN mutating op would leave behind if it took effect, or null if it could
     * not have. It commits at a ballot the client never learned, so the version becomes unknown.
     */
    private static State effectIfHappened(final OpRecord op, final State model) {
        switch (op.kind()) {
            case PUT:
                return new State(op.arg(), null);
            case DELETE:
                return new State(null, null);
            case CAS:
                return fenceHolds(op, model) ? new State(op.desired(), null) : null;
            default:
                return model;
        }
    }

    private static final class Apply {
        final boolean legal;
        final State model;

        Apply(final boolean legal, final State model) {
            this.legal = legal;
            this.model = model;
        }
    }

    private static Apply applyDefinite(final OpRecord op, final State model) {
        // FAIL and UNKNOWN reads impose no constraint and leave the model unchanged.
        if (op.status() == OpRecord.Status.FAIL) {
            return new Apply(true, model);
        }
        switch (op.kind()) {
            case GET:
                if (op.status() == OpRecord.Status.UNKNOWN) {
                    return new Apply(true, model); // nothing observed -> no constraint
                }
                if (!equalsNullable(model.value, op.observed()) || readsBackwards(op, model)) {
                    return new Apply(false, model);
                }
                // A read is ground truth about the version, including after a purge has taken the
                // key back to INITIAL, so it replaces what the model believed rather than checking
                // it. The version can move with no operation of ours behind it: a lost fence that
                // committed what it found, a repair round, anti-entropy.
                return new Apply(true, new State(model.value, op.version()));
            case PUT:
                return commit(op, model, op.arg());
            case DELETE:
                return commit(op, model, null);
            case CAS:
                if (!op.swapped()) {
                    return new Apply(!fenceHolds(op, model), model);
                }
                return fenceHolds(op, model)
                        ? commit(op, model, op.desired()) : new Apply(false, model);
            default:
                return new Apply(true, model);
        }
    }

    /**
     * A definite commit, which cannot land below what the register had already reached.
     * <p>
     * Not <em>above</em>, though: a write whose value is the one already there commits nothing. The
     * round finds the state unchanged, short-circuits before the accept phase, and answers with the
     * version that was already in force -- so re-putting the same bytes, or deleting an already
     * tombstoned key, legitimately reports the version its predecessor did.
     */
    private static Apply commit(final OpRecord op, final State model, final HashedBytes value) {
        if (model.version != null && op.version() != null
                && op.version().compareTo(model.version) < 0) {
            return new Apply(false, model);
        }
        return new Apply(true, new State(value, op.version()));
    }

    /**
     * Whether a write fenced on {@code op.fencedOn()} could have applied here.
     * <p>
     * The fence is a version, so that is what is compared -- two writes of identical bytes are two
     * different states, and only one of them is the one the caller fenced on. The version is unknown
     * only where an indeterminate write took effect, and there the value is the best evidence left.
     */
    private static boolean fenceHolds(final OpRecord op, final State model) {
        if (op.fencedOn() == null || model.version == null) {
            return equalsNullable(model.value, op.arg());
        }
        return op.fencedOn().equals(model.version);
    }

    /**
     * A read of a version below the one already committed -- the register moving backwards.
     * <p>
     * Exempt: an absent key at {@link Version#INITIAL}. Collecting a tombstone removes the key
     * outright, which legitimately puts it back to "never written", and reporting that as a
     * violation would be a false alarm about the one case the store means to allow.
     */
    private static boolean readsBackwards(final OpRecord op, final State model) {
        if (model.version == null || op.version() == null) {
            return false;
        }
        if (op.observed() == null && Version.INITIAL.equals(op.version())) {
            return false;
        }
        return op.version().compareTo(model.version) < 0;
    }

    private static boolean equalsNullable(final HashedBytes x, final HashedBytes y) {
        return x == null ? y == null : x.equals(y);
    }

    private static final class MemoKey {
        private final BitSet done;
        private final State model;

        MemoKey(final BitSet done, final State model) {
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
