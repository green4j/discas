/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

/**
 * Sink for a single client-protocol response. A node's client-request
 * handler invokes {@link #send} once per request; the transport that
 * created the sink routes the response back to the originating client
 * (in-process or over TCP).
 * <p>
 * This is the one neutral seam between the outbound client and the node's
 * client-facing server, so it lives in {@code common.client} rather than in
 * either the client or the node module.
 */
@FunctionalInterface
public interface ResponseSink {
    void send(ClientMessage response);
}
