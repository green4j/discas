/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.acl.ClientAuthorizer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("ClientHandler -- rejects oversized keys/values at ingress (no round)")
class ClientHandlerSizeLimitTest {

    // proposer/store are unused on the oversized-input path (the request is rejected before any round).
    private final ClientHandler handler =
            new ClientHandler(NodeId.of("1"), null, null, new EventLoop("size-test"),
                    new ClientAuthorizer());

    private static final ClientId CLIENT = ClientId.of("c");
    private static final ByteBuffer SMALL_KEY = ByteBuffer.wrap(new byte[] {1, 2, 3});

    @Test
    void putWithOversizedValueRejected() {
        final ByteBuffer big = ByteBuffer.wrap(new byte[KvLimits.MAX_VALUE_BYTES + 1]);
        final AtomicReference<ClientMessage> captured = new AtomicReference<>();
        handler.handlePut(CLIENT, new ClientMessage.ClientPutReq("c", 7L, SMALL_KEY, big), captured::set);

        final ClientMessage.ClientPutResp resp = (ClientMessage.ClientPutResp) captured.get();
        assertNotNull(resp);
        assertFalse(resp.ok());
        assertEquals(ClientErrorCode.INVALID_ARGUMENT, resp.errorCode());
    }

    @Test
    void casWithOversizedKeyRejected() {
        final ByteBuffer bigKey = ByteBuffer.wrap(new byte[KvLimits.MAX_KEY_BYTES + 1]);
        final AtomicReference<ClientMessage> captured = new AtomicReference<>();
        handler.handleCas(CLIENT,
                new ClientMessage.ClientCasReq("c", 9L, bigKey, Ballot.ZERO, SMALL_KEY),
                captured::set);

        final ClientMessage.ClientCasResp resp = (ClientMessage.ClientCasResp) captured.get();
        assertNotNull(resp);
        assertFalse(resp.ok());
        assertEquals(ClientErrorCode.INVALID_ARGUMENT, resp.errorCode());
    }
}
