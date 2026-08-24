/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.node.HashedBytes;

/**
 * Evaluates per-operation, per-key authorization for a client. Holds a volatile
 * {@link ClientAclSnapshot} that a bound {@link ClientAcl} keeps current (replay-on-subscribe
 * plus every reload, marshalled onto the node event loop).
 *
 * <h2>Default when no ACL is bound</h2>
 * An unbound authorizer is <b>permissive</b> -- every operation on every key is allowed.
 * This preserves the AllowAll contract (no authentication, no restrictions) for tests and
 * trusted deployments. Binding an ACL activates default-deny, allow-only, per-client
 * prefix-grant evaluation.
 */
public final class ClientAuthorizer {

    private volatile ClientAclSnapshot snapshot; // null => permissive (AllowAll / unbound)

    /** Bind an ACL source; its snapshot is applied on {@code loop} and refreshed on reload. */
    public void bind(final ClientAcl acl, final EventLoop loop) {
        acl.addListener(s -> loop.execute(() -> this.snapshot = s));
    }

    /** True if {@code clientId} may perform {@code op} on {@code key}. */
    public boolean allow(final ClientId clientId, final ClientOp op, final HashedBytes key) {
        final ClientAclSnapshot current = snapshot;
        if (current == null) {
            return true; // AllowAll / unconfigured: no restrictions
        }
        if (clientId == null) {
            return false;
        }
        final ClientPolicy policy = current.policy(clientId);
        return policy != null && policy.allows(op, key);
    }
}
