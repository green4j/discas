/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("InProcessClientTransport -- co-located fast path")
class InProcessClientTransportTest {

    private static final NodeId NODE_ID = NodeId.of("42");
    private static final ClientId CLIENT_ID = ClientId.of("test-client");
    // Single fake node registered by this fixture, so N == 1.
    private static final int CLUSTER_SIZE = 1;

    private EventLoop nodeLoop;
    private EventLoop clientLoop;

    @AfterEach
    void tearDown() {
        InProcessClientRegistry.unregister(NODE_ID);
        if (nodeLoop != null) {
            nodeLoop.shutdown();
            nodeLoop.awaitTermination(Duration.ofSeconds(2));
        }
        if (clientLoop != null && clientLoop != nodeLoop) {
            clientLoop.shutdown();
            clientLoop.awaitTermination(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("Distinct loops -- send is enqueued on the node loop")
    void distinctLoopsEnqueuesOnNodeLoop() throws Exception {
        nodeLoop = new EventLoop("test-node-loop");
        clientLoop = new EventLoop("test-client-loop");
        nodeLoop.start();
        clientLoop.start();

        final AtomicReference<Thread> ingressThread = new AtomicReference<>();
        final CountDownLatch delivered = new CountDownLatch(1);
        InProcessClientRegistry.register(NODE_ID, nodeLoop, (clientId, message, replySink) -> {
            ingressThread.set(Thread.currentThread());
            delivered.countDown();
        }, CLUSTER_SIZE);

        final InProcessClientTransport transport = new InProcessClientTransport(clientLoop, List.of(NODE_ID),
                CLIENT_ID);

        final CountDownLatch sent = new CountDownLatch(1);
        clientLoop.execute(() -> {
            transport.send(NODE_ID, sampleGet(1));
            sent.countDown();
        });

        assertTrue(sent.await(2, TimeUnit.SECONDS));
        assertTrue(delivered.await(2, TimeUnit.SECONDS));
        assertNotNull(ingressThread.get());
        assertFalse(ingressThread.get().getName().equals("test-client-loop"),
                "Ingress must run on the node loop, not the client loop");
        assertTrue(ingressThread.get().getName().equals("test-node-loop"),
                "Ingress must run on the node loop");
    }

    @Test
    @DisplayName("Shared loop + in-loop caller -- request delivered inline")
    void sharedLoopInLoopCallerDispatchesInline() throws Exception {
        nodeLoop = new EventLoop("shared-loop");
        clientLoop = nodeLoop;
        nodeLoop.start();

        final AtomicBoolean invokedInline = new AtomicBoolean();
        InProcessClientRegistry.register(NODE_ID, nodeLoop, (clientId, message, replySink) -> {
            // Called synchronously on the sending stack when fast path fires
            invokedInline.set(true);
        }, CLUSTER_SIZE);

        final InProcessClientTransport transport = new InProcessClientTransport(clientLoop, List.of(NODE_ID),
                CLIENT_ID);

        // Inline delivery is measured by reading invokedInline before send() returns: had the
        // transport enqueued instead, the flag would still be false on this stack.
        final AtomicBoolean inlineOnSendingStack = new AtomicBoolean();
        final CountDownLatch done = new CountDownLatch(1);
        nodeLoop.execute(() -> {
            transport.send(NODE_ID, sampleGet(2));
            inlineOnSendingStack.set(invokedInline.get());
            done.countDown();
        });

        assertTrue(done.await(2, TimeUnit.SECONDS));
        assertTrue(inlineOnSendingStack.get(),
                "Fast path must invoke ingress synchronously, not defer it to a later loop iteration");
    }

    @Test
    @DisplayName("Shared loop + off-loop caller -- send is enqueued")
    void sharedLoopOffLoopCallerEnqueues() throws Exception {
        nodeLoop = new EventLoop("shared-loop-off");
        clientLoop = nodeLoop;
        nodeLoop.start();

        final AtomicReference<Thread> ingressThread = new AtomicReference<>();
        final CountDownLatch delivered = new CountDownLatch(1);
        InProcessClientRegistry.register(NODE_ID, nodeLoop, (clientId, message, replySink) -> {
            ingressThread.set(Thread.currentThread());
            delivered.countDown();
        }, CLUSTER_SIZE);

        final InProcessClientTransport transport = new InProcessClientTransport(clientLoop, List.of(NODE_ID),
                CLIENT_ID);

        // Call send() from the test thread -- not the loop thread -- so the fast path
        // gate (clientLoop.inLoop()) is false and the request must be enqueued
        transport.send(NODE_ID, sampleGet(3));

        assertTrue(delivered.await(2, TimeUnit.SECONDS));
        assertEquals("shared-loop-off", ingressThread.get().getName(),
                "Ingress must run on the loop thread even when called from a foreign thread");
    }

    @Test
    @DisplayName("Reply sink is delivered through clientLoop.execute even when co-located")
    void replyIsDeferredEvenWhenColocated() throws Exception {
        nodeLoop = new EventLoop("shared-loop-reply");
        clientLoop = nodeLoop;
        nodeLoop.start();

        final AtomicBoolean synchronousReplyRan = new AtomicBoolean();
        final CountDownLatch responseSeen = new CountDownLatch(1);

        InProcessClientRegistry.register(NODE_ID, nodeLoop, (clientId, message, replySink) -> {
            // Fire the reply synchronously -- mirrors ClientHandler.handleScan happy path
            replySink.send(new ClientMessage.ClientGetResp(
                    NODE_ID.value(),
                    ((ClientMessage.ClientGetReq) message).correlationId(),
                    true,
                    null,
                    null,
                    ClientErrorCode.NONE));
        }, CLUSTER_SIZE);

        final InProcessClientTransport transport = new InProcessClientTransport(clientLoop, List.of(NODE_ID),
                CLIENT_ID);
        transport.register(response -> {
            responseSeen.countDown();
            synchronousReplyRan.set(true);
        });

        final AtomicBoolean sawResponseInSendFrame = new AtomicBoolean();
        nodeLoop.execute(() -> {
            transport.send(NODE_ID, sampleGet(4));
            // If reply were inlined, the responseHandler would already have fired
            sawResponseInSendFrame.set(synchronousReplyRan.get());
        });

        assertTrue(responseSeen.await(2, TimeUnit.SECONDS));
        assertFalse(sawResponseInSendFrame.get(),
                "Reply must not fire on the sending stack even when co-located");
    }

    private static ClientMessage sampleGet(final long correlationId) {
        return new ClientMessage.ClientGetReq("1", correlationId, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    }
}
