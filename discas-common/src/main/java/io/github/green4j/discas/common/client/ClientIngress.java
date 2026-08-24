/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.identity.ClientId;

/**
 * Node-side entry point for a decoded client message, carrying the <b>trusted</b>
 * client identity bound to the originating connection at CLIENT_HELLO time (never the
 * self-declared {@code senderId} inside the message). The transport that created the
 * accompanying {@link ResponseSink} supplies this identity; authorization keys off it.
 * <p>
 * Lives in {@code common.client} because both the node module (which consumes it) and
 * the in-process client transport (which invokes it) must reach it, and neither may
 * depend on the other.
 */
@FunctionalInterface
public interface ClientIngress {

    /**
     * @param authenticatedClientId the identity authenticated for the connection, or
     *        {@code null} for an unauthenticated (e.g. AllowAll / in-process) connection
     */
    void accept(ClientId authenticatedClientId, ClientMessage message, ResponseSink sink);
}
