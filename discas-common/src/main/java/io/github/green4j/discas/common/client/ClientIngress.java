/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.identity.ClientIdentity;

/**
 * Node-side entry point for a decoded client message, carrying the <b>trusted</b>
 * client identity bound to the originating connection at CLIENT_HELLO time (never the
 * self-declared {@code senderId} inside the message). The transport that created the
 * accompanying {@link ResponseSink} supplies this identity; authorization keys off its
 * {@link ClientIdentity#id()}.
 * <p>
 * Lives in {@code common.client} because both the node module (which consumes it) and
 * the in-process client transport (which invokes it) must reach it, and neither may
 * depend on the other.
 */
@FunctionalInterface
public interface ClientIngress {

    /**
     * @param identity the identity bound to the connection; never {@code null}, though its
     *        {@link ClientIdentity#id()} is {@code null} for an unauthenticated (e.g. in-process)
     *        connection
     */
    void accept(ClientIdentity identity, ClientMessage message, ResponseSink sink);
}
