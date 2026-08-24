/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ResponseSink;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.acl.ClientAuthorizer;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A round whose peers never answer must reach the caller as
 * {@link ClientErrorCode#NO_QUORUM_AT_COORDINATOR} on every operation.
 * <p>
 * The code is the node-local one rather than {@link ClientErrorCode#UNAVAILABLE} because of what
 * this setup simulates: peers that never respond, so prepare cannot reach quorum. That is
 * {@code RoundFailure.INSUFFICIENT_RESPONDERS}, the one failure that says <em>this coordinator</em>
 * cannot see a majority and the client should ask another. The other two failures are separated by
 * a different question -- whether the outcome is <em>known</em>: a lost ballot duel never reached
 * Accept, so it reports {@link ClientErrorCode#BALLOT_LOST} (determinate), while a stalled accept
 * may still be completed by a later proposer and reports {@link ClientErrorCode#UNAVAILABLE}
 * (indeterminate).
 * <p>
 * All four operations are driven <b>together</b> rather than one per test method, so their waits
 * overlap instead of adding up.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("ClientHandler -- unanswered peers are NO_QUORUM_AT_COORDINATOR on every operation")
class ClientHandlerErrorCodeTest {

    private static final ClientId CLIENT = ClientId.of("c");
    private static final ByteBuffer KEY = ByteBuffer.wrap(new byte[] {1, 2, 3});
    private static final ByteBuffer VALUE = ByteBuffer.wrap(new byte[] {4});
    // A round that never collects a quorum of promises is not retried -- retrying re-asks the same
    // silent peers -- so it gives up after a single 5s round timeout. Allow a generous margin.
    private static final Duration AWAIT = Duration.ofSeconds(20);

    /** The four client operations, each identified by the response it must produce. */
    private enum Op { GET, PUT, CAS, DELETE }

    private EventLoop loop;
    private ClientHandler handler;

    @BeforeEach
    void setUp() {
        final NodeId self = NodeId.of("1");
        final List<NodeId> allNodes = List.of(self, NodeId.of("2"), NodeId.of("3"));

        loop = new EventLoop("no-quorum-test");
        loop.start();

        // Peers 2 and 3 are never registered, so no prepare/accept is ever answered and the
        // round can only end in the timeout.
        final InProcessPeerTransport transport =
                new InProcessPeerTransport(self, allNodes.size(), loop, InMemoryMembers.ofNodes(allNodes));
        final LocalStore store = new LocalStore(new InMemoryWal());
        final Proposer proposer = new Proposer(self, transport, new Acceptor(self, store), loop, store,
                new CorrelationIdGenerator(self));

        handler = new ClientHandler(self, proposer, store, loop, new ClientAuthorizer());
    }

    @AfterEach
    void tearDown() {
        if (loop != null) {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(5));
        }
    }

    @Test
    void everyOperationReportsNoQuorumAtCoordinator() throws Exception {
        final Map<Op, AtomicReference<ClientMessage>> replies = new EnumMap<>(Op.class);
        for (final Op op : Op.values()) {
            replies.put(op, new AtomicReference<>());
        }
        final CountDownLatch answered = new CountDownLatch(Op.values().length);

        loop.execute(() -> {
            handler.handleGet(CLIENT, new ClientMessage.ClientGetReq("c", 1L, KEY),
                    sink(replies.get(Op.GET), answered));
            handler.handlePut(CLIENT, new ClientMessage.ClientPutReq("c", 2L, KEY, VALUE),
                    sink(replies.get(Op.PUT), answered));
            handler.handleCas(CLIENT, new ClientMessage.ClientCasReq("c", 3L, KEY, Ballot.ZERO, VALUE),
                    sink(replies.get(Op.CAS), answered));
            handler.handleDelete(CLIENT, new ClientMessage.ClientDeleteReq("c", 4L, KEY),
                    sink(replies.get(Op.DELETE), answered));
        });

        assertTrue(answered.await(AWAIT.toMillis(), TimeUnit.MILLISECONDS),
                "Not every operation answered within " + AWAIT);

        final ClientMessage.ClientGetResp get = require(replies, Op.GET, ClientMessage.ClientGetResp.class);
        assertFalse(get.ok());
        assertEquals(ClientErrorCode.NO_QUORUM_AT_COORDINATOR, get.errorCode());

        final ClientMessage.ClientPutResp put = require(replies, Op.PUT, ClientMessage.ClientPutResp.class);
        assertFalse(put.ok());
        assertEquals(ClientErrorCode.NO_QUORUM_AT_COORDINATOR, put.errorCode());

        final ClientMessage.ClientCasResp cas = require(replies, Op.CAS, ClientMessage.ClientCasResp.class);
        assertFalse(cas.ok());
        assertFalse(cas.swapped());
        assertEquals(ClientErrorCode.NO_QUORUM_AT_COORDINATOR, cas.errorCode());

        final ClientMessage.ClientDeleteResp delete =
                require(replies, Op.DELETE, ClientMessage.ClientDeleteResp.class);
        assertFalse(delete.ok());
        assertEquals(ClientErrorCode.NO_QUORUM_AT_COORDINATOR, delete.errorCode());
    }

    private static ResponseSink sink(final AtomicReference<ClientMessage> slot,
                                     final CountDownLatch answered) {
        return message -> {
            slot.set(message);
            answered.countDown();
        };
    }

    private static <T extends ClientMessage> T require(
            final Map<Op, AtomicReference<ClientMessage>> replies,
            final Op op,
            final Class<T> type) {
        final ClientMessage message = replies.get(op).get();
        assertNotNull(message, op + " produced no reply");
        return type.cast(message);
    }
}
