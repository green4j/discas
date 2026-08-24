/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientIngress;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node that has not finished recovery must <em>answer</em> client requests with a retryable
 * failure, not swallow them.
 * <p>
 * Silence costs the client a full per-attempt timeout against this node, and on a cluster restart
 * -- every node recovering at once -- that is the entire retry budget spent learning nothing.
 * <p>
 * {@code ready} only becomes true when recovery completes, so these tests start the event loop
 * <em>without</em> calling {@link DisCasNode#start()}: the loop drains the ingress while the node
 * is still not ready, which is exactly the window under test.
 */
@DisplayName("DisCasNode -- requests arriving before recovery completes")
class NodeNotReadyTest {

    private static final ClientId CLIENT = ClientId.of("c1");
    private static final NodeId N1 = NodeId.of("1");

    private EventLoop loop;
    private DisCasNode node;
    private InProcessPeerTransport transport;
    private ClientIngress ingress;
    private BlockingQueue<ClientMessage> replies;
    private AtomicInteger refusals;

    @BeforeEach
    void setUp() {
        loop = new EventLoop("not-ready-node");
        transport = new InProcessPeerTransport(
                N1, 1, loop, InMemoryMembers.ofNodes(List.of(N1)));
        refusals = new AtomicInteger();
        node = new DisCasNode(new NodeConfig(N1, ClusterId.of("not-ready-cluster"), 1),
                new InMemoryWal(), loop, transport,
                new NodeObserver() {
                    @Override
                    public void requestBeforeReady(final boolean peerMessage) {
                        if (!peerMessage) {
                            refusals.incrementAndGet();
                        }
                    }
                });
        replies = new ArrayBlockingQueue<>(8);
        node.registerClientMessages(registrar -> ingress = registrar);
        // Loop up, node never started: recovery has not run, so ready == false.
        loop.start();
    }

    @AfterEach
    void tearDown() {
        loop.shutdown();
        loop.awaitTermination(Duration.ofSeconds(2));
    }

    private ClientMessage send(final ClientMessage request) throws Exception {
        ingress.accept(CLIENT, request, replies::add);
        final ClientMessage reply = replies.poll(5, TimeUnit.SECONDS);
        assertNotNull(reply, "A not-ready node must answer, not stay silent");
        return reply;
    }

    @Test
    @DisplayName("GET before ready is refused as NOT_READY, not dropped")
    void getIsRefused() throws Exception {
        final ClientMessage.ClientGetResp reply = (ClientMessage.ClientGetResp)
                send(new ClientMessage.ClientGetReq(CLIENT.value(), 1L, TestBytes.utf8("k")));

        assertFalse(reply.ok());
        assertEquals(ClientErrorCode.NOT_READY, reply.errorCode());
        assertEquals(1, refusals.get());
    }

    @Test
    @DisplayName("PUT, CAS and DELETE before ready are all refused as NOT_READY")
    void writesAreRefused() throws Exception {
        final ClientMessage.ClientPutResp put = (ClientMessage.ClientPutResp)
                send(new ClientMessage.ClientPutReq(CLIENT.value(), 1L, TestBytes.utf8("k"), TestBytes.utf8("v")));
        assertFalse(put.ok());
        assertEquals(ClientErrorCode.NOT_READY, put.errorCode());

        final ClientMessage.ClientCasResp cas = (ClientMessage.ClientCasResp)
                send(new ClientMessage.ClientCasReq(CLIENT.value(), 2L, TestBytes.utf8("k"), Ballot.ZERO,
                        TestBytes.utf8("v")));
        assertFalse(cas.ok());
        assertFalse(cas.swapped());
        assertEquals(ClientErrorCode.NOT_READY, cas.errorCode());

        final ClientMessage.ClientDeleteResp del = (ClientMessage.ClientDeleteResp)
                send(new ClientMessage.ClientDeleteReq(CLIENT.value(), 3L, TestBytes.utf8("k")));
        assertFalse(del.ok());
        assertEquals(ClientErrorCode.NOT_READY, del.errorCode());
    }

    @Test
    @DisplayName("SCAN before ready stays silent -- an empty page would look like an empty store")
    void scanStaysSilent() throws Exception {
        ingress.accept(CLIENT, new ClientMessage.ClientScanReq(
                CLIENT.value(), 1L, ByteBuffers.EMPTY, null, 10), replies::add);

        // ClientScanResp has no error field, so any reply is an *empty page* -- indistinguishable
        // from "this node holds no keys" and enough to corrupt the client's quorum merge. Being a
        // non-responder is the correct answer here.
        assertNull(replies.poll(500, TimeUnit.MILLISECONDS),
                "A not-ready node must not contribute an empty page to a scan quorum");
        assertTrue(refusals.get() >= 1, "The refusal is still reported to the observer");
    }
}
