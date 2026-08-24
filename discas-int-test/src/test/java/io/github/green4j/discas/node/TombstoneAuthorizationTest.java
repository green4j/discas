/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ResponseSink;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.acl.ClientAcl;
import io.github.green4j.discas.node.acl.ClientAuthorizer;
import io.github.green4j.discas.node.acl.ClientOp;
import io.github.green4j.discas.node.acl.InMemoryClientAcl;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CAS whose desired value is {@code null} commits a tombstone: it is a delete, whatever the wire
 * type says. So it needs the {@code DELETE} grant, not only {@code CAS} -- otherwise a grant
 * written to let a client update keys but never remove them would not hold, and the ACL would be
 * enforcing a distinction the code does not make.
 * <p>
 * Both CAS spellings are covered, because both accept a null desired.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("ClientHandler -- a null-desired CAS is a delete and needs the DELETE grant")
class TombstoneAuthorizationTest {

    private static final ClientId UPDATER = ClientId.of("updater");  // CAS, no DELETE
    private static final ClientId REMOVER = ClientId.of("remover");  // CAS and DELETE
    private static final String PREFIX = "app/";
    private static final ByteBuffer KEY =
            ByteBuffer.wrap("app/counter".getBytes(StandardCharsets.UTF_8));
    private static final ByteBuffer VALUE = ByteBuffer.wrap(new byte[] {7});

    /**
     * The permitted requests still have to finish a round against peers that were never
     * registered, which ends in the coordinator's own quorum check or its round timeout. Generous,
     * because what is asserted about them is only that they got past the ACL.
     */
    private static final Duration AWAIT = Duration.ofSeconds(20);

    private EventLoop loop;
    private ClientHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        final NodeId self = NodeId.of("1");
        final List<NodeId> allNodes = List.of(self, NodeId.of("2"), NodeId.of("3"));

        loop = new EventLoop("tombstone-authz-test");
        loop.start();

        final InProcessPeerTransport transport = new InProcessPeerTransport(
                self, allNodes.size(), loop, InMemoryMembers.ofNodes(allNodes));
        final LocalStore store = new LocalStore(new InMemoryWal());
        final Proposer proposer = new Proposer(self, transport, new Acceptor(self, store), loop, store,
                new CorrelationIdGenerator(self));

        final ClientAcl acl = InMemoryClientAcl.builder()
                .grant(UPDATER, PREFIX, ClientOp.GET, ClientOp.PUT, ClientOp.CAS)
                .grant(REMOVER, PREFIX, ClientOp.GET, ClientOp.PUT, ClientOp.CAS, ClientOp.DELETE)
                .build();
        final ClientAuthorizer authorizer = new ClientAuthorizer();
        authorizer.bind(acl, loop);
        // bind() marshals the replayed snapshot onto the loop; wait for a follow-up task so the
        // snapshot is applied (FIFO) before anything is dispatched.
        final CountDownLatch applied = new CountDownLatch(1);
        loop.execute(applied::countDown);
        assertTrue(applied.await(5, TimeUnit.SECONDS), "The ACL snapshot was never applied");

        handler = new ClientHandler(self, proposer, store, loop, authorizer);
    }

    @AfterEach
    void tearDown() {
        if (loop != null) {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("CAS without DELETE may swap a value but may not tombstone one")
    void casGrantAloneCannotTombstone() throws Exception {
        final AtomicReference<ClientMessage> tombstone = new AtomicReference<>();
        final AtomicReference<ClientMessage> swap = new AtomicReference<>();
        final CountDownLatch answered = new CountDownLatch(2);

        loop.execute(() -> {
            handler.handleCas(UPDATER,
                    new ClientMessage.ClientCasReq("updater", 1L, KEY, Ballot.ZERO, null),
                    sink(tombstone, answered));
            // Control: the same client, the same key, a non-null desired -- must not be refused,
            // or the assertion above would pass for the wrong reason.
            handler.handleCas(UPDATER,
                    new ClientMessage.ClientCasReq("updater", 2L, KEY, Ballot.ZERO, VALUE),
                    sink(swap, answered));
        });

        assertTrue(answered.await(AWAIT.toMillis(), TimeUnit.MILLISECONDS),
                "Not every request answered within " + AWAIT);

        assertEquals(ClientErrorCode.ACCESS_DENIED,
                require(tombstone, ClientMessage.ClientCasResp.class).errorCode(),
                "cas(key, version, null) -- the client's delete(key, Version) -- needs DELETE");
        assertNotEquals(ClientErrorCode.ACCESS_DENIED,
                require(swap, ClientMessage.ClientCasResp.class).errorCode(),
                "A value-writing CAS is still permitted by the CAS grant alone");
    }

    @Test
    @DisplayName("CAS together with DELETE may tombstone")
    void casPlusDeleteMayTombstone() throws Exception {
        final AtomicReference<ClientMessage> tombstone = new AtomicReference<>();
        final CountDownLatch answered = new CountDownLatch(1);

        loop.execute(() -> handler.handleCas(REMOVER,
                new ClientMessage.ClientCasReq("remover", 1L, KEY, Ballot.ZERO, null),
                sink(tombstone, answered)));

        assertTrue(answered.await(AWAIT.toMillis(), TimeUnit.MILLISECONDS),
                "The request was not answered within " + AWAIT);

        assertNotEquals(ClientErrorCode.ACCESS_DENIED,
                require(tombstone, ClientMessage.ClientCasResp.class).errorCode());
    }

    private static ResponseSink sink(final AtomicReference<ClientMessage> slot,
                                     final CountDownLatch answered) {
        return message -> {
            slot.set(message);
            answered.countDown();
        };
    }

    private static <T extends ClientMessage> T require(final AtomicReference<ClientMessage> slot,
                                                       final Class<T> type) {
        final ClientMessage message = slot.get();
        assertNotNull(message, "no reply");
        return type.cast(message);
    }
}
