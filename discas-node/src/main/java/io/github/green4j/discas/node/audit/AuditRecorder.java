/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.identity.ClientIdentity;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.node.acl.ClientOp;

import java.nio.ByteBuffer;

/**
 * The event-loop side of the audit: what the transport and the request path call. Every method is
 * a {@code default} no-op, so a node with no audit configured pays a call that returns -- no ring,
 * no thread, no buffer. Implementations run on the loop and must not block, bar the one policy
 * that deliberately does ({@link AuditOverflow#WAIT}).
 */
public interface AuditRecorder {

    AuditRecorder NONE = new AuditRecorder() {
    };

    default void sessionOpened(final ClientIdentity identity) {
    }

    /** The identity is the one claimed: a refusal is the node saying it could not establish it. */
    default void sessionRefused(final ClientIdentity identity, final ClientHelloRespStatus status) {
    }

    default void sessionClosed(final ClientIdentity identity) {
    }

    /**
     * @param key             the key, or a scan's prefix
     * @param value           the value a write carries, or {@code null}
     * @param expectedVersion the ballot counter a CAS is fenced on, or {@code 0}
     * @param serializable    whether a read asked to be answered from local state
     */
    default void requestReceived(final ClientIdentity identity, final ClientOp op,
                                 final long correlationId, final ByteBuffer key,
                                 final ByteBuffer value, final long expectedVersion,
                                 final boolean serializable) {
    }

    /**
     * How the operation with this {@code correlationId} ended. The key is not repeated: the request
     * record named it, and this says only what became of it.
     *
     * @param version the ballot the round committed or the read observed, or {@code null}
     * @param count   entries a scan returned; ignored otherwise
     */
    default void requestCompleted(final ClientIdentity identity, final ClientOp op,
                                  final long correlationId, final boolean ok,
                                  final ClientErrorCode errorCode, final Ballot version,
                                  final int count) {
    }
}
