/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;
import io.github.green4j.discas.node.HashedBytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * A single client's authorization policy: a set of grants, each binding a key prefix to a set of
 * permitted {@link ClientOp}s. Evaluation is <b>default-deny</b> -- an operation is allowed only if
 * some grant's prefix matches the key and its op set includes the operation. Value-based equals
 * lets a snapshot skip re-publishing an unchanged reload, which is why the grants are held in a
 * canonical order: what the file cannot express, equals must not distinguish.
 */
public final class ClientPolicy {

    /** A prefix -&gt; permitted-operations grant. */
    public static final class Grant {
        private final String prefix;
        /**
         * Read-only and never handed out, so every {@link #matches} can test against it directly
         * with no per-call allocation.
         */
        private final ByteBuffer prefixBytes;
        private final EnumSet<ClientOp> ops;

        Grant(final String prefix, final EnumSet<ClientOp> ops) {
            this.prefix = prefix;
            this.prefixBytes = ByteBuffer.wrap(prefix.getBytes(StandardCharsets.UTF_8))
                    .asReadOnlyBuffer();
            this.ops = EnumSet.copyOf(ops);
        }

        /** The grant as the file writes it, {@code prefix:OPS}, for the reload report. */
        String text() {
            final StringBuilder sb = new StringBuilder(prefix).append(':');
            for (final ClientOp op : ops) { // EnumSet iterates in declaration order, so this is stable
                sb.append(op.code());
            }
            return sb.toString();
        }

        boolean matches(final ClientOp op, final HashedBytes key) {
            return ops.contains(op) && key.startsWith(prefixBytes);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Grant)) {
                return false;
            }
            final Grant other = (Grant) o;
            return prefix.equals(other.prefix) && ops.equals(other.ops);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, ops);
        }
    }

    private final List<Grant> grants;

    ClientPolicy(final List<Grant> grants) {
        // Held in a canonical order, with duplicates dropped. Evaluation is a plain OR over the
        // grants, so neither their order nor a repeat is observable in a decision -- but both are
        // observable in equals(), and that is what decides whether a reload counts as a change.
        // Without this, swapping two grants on one line would republish the whole table and read,
        // in the log and the metrics, as a policy change that never happened.
        final List<Grant> canonical = new ArrayList<>(new LinkedHashSet<>(grants));
        canonical.sort(Comparator.comparing((Grant g) -> g.prefix).thenComparing(Grant::text));
        this.grants = List.copyOf(canonical);
    }

    /** One line naming every grant, for the reload report. Prefixes and ops, which are not secret. */
    String summary() {
        if (grants.isEmpty()) {
            return "no grants";
        }
        final StringBuilder sb = new StringBuilder();
        for (final Grant grant : grants) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(grant.text());
        }
        return sb.toString();
    }

    /** True if some grant permits {@code op} on {@code key}. */
    boolean allows(final ClientOp op, final HashedBytes key) {
        for (final Grant grant : grants) {
            if (grant.matches(op, key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientPolicy)) {
            return false;
        }
        return grants.equals(((ClientPolicy) o).grants);
    }

    @Override
    public int hashCode() {
        return grants.hashCode();
    }
}
