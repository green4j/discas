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
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * A single client's authorization policy: an ordered list of grants, each binding a key
 * prefix to a set of permitted {@link ClientOp}s. Evaluation is <b>default-deny</b> -- an
 * operation is allowed only if some grant's prefix matches the key and its op set includes the
 * operation. Value-based equals lets a snapshot skip re-publishing an unchanged reload.
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
        this.grants = List.copyOf(grants);
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
