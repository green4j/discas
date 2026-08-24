/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A coordinator whose connection dies mid-request must not cost a per-attempt timeout.
 * <p>
 * The transport knows the moment a socket drops, and without this the caller would find out only
 * when its own timer expired -- five seconds, on the default per-attempt budget, of waiting for an
 * answer that provably will never come. So every assertion here is about the request finishing far
 * inside that budget rather than merely finishing.
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES)
@DisplayName("DisCasClient -- a dropped connection fails its in-flight requests at once")
class ConnectionLostFailoverTest {

    private static final ClientId CLIENT = ClientId.of("c1");
    /** Long enough that waiting it out would be unmistakable in the elapsed time. */
    private static final Duration PER_ATTEMPT = Duration.ofSeconds(5);

    private DisCasClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    private static List<NodeId> peers(final int m) {
        final List<NodeId> out = new ArrayList<>(m);
        for (int i = 1; i <= m; i++) {
            out.add(NodeId.of(Integer.toString(i)));
        }
        return Collections.unmodifiableList(out);
    }

    /** Lets a test sever a connection the way a dying coordinator would. */
    private static final class DroppableTransport implements ClientTransport {
        private final List<NodeId> addressed = Collections.synchronizedList(new ArrayList<>());
        private final List<NodeId> peerList;
        private final BiFunction<NodeId, ClientMessage, ClientMessage> responder;
        private Consumer<ClientMessage> handler;
        private Consumer<NodeId> connectionLost;

        DroppableTransport(final List<NodeId> peerList,
                           final BiFunction<NodeId, ClientMessage, ClientMessage> responder) {
            this.peerList = peerList;
            this.responder = responder;
        }

        void drop(final NodeId peer) {
            connectionLost.accept(peer);
        }

        @Override
        public void send(final NodeId target, final ClientMessage message) {
            addressed.add(target);
            final ClientMessage reply = responder.apply(target, message);
            if (reply != null && handler != null) {
                handler.accept(reply);
            }
        }

        @Override
        public void register(final Consumer<ClientMessage> h) {
            this.handler = h;
        }

        @Override
        public void registerConnectionLost(final Consumer<NodeId> h) {
            this.connectionLost = h;
        }

        @Override
        public List<NodeId> peers() {
            return peerList;
        }

        @Override
        public int clusterSize() {
            return peerList.size();
        }
    }

    private DisCasClient newClient(final ClientTransport transport) {
        client = new DisCasClient(CLIENT, transport, new EventLoop("conn-lost"), true,
                ClientObserver.NONE,
                DisCasClientConfig.builder()
                        .perAttemptTimeout(PER_ATTEMPT)
                        .requestDeadline(Duration.ofSeconds(30))
                        // Small enough that the failover itself is never delayed by it.
                        .peerRetryMinBackoff(Duration.ofMillis(5))
                        .peerRetryMaxBackoff(Duration.ofMillis(20))
                        .build());
        return client;
    }

    @Test
    @DisplayName("An in-flight request moves to another coordinator without awaiting the timeout")
    void inFlightRequestFailsOverImmediately() throws Exception {
        // The first coordinator addressed swallows the request; every other one serves it.
        final AtomicReference<NodeId> swallowed = new AtomicReference<>();
        final DroppableTransport transport = new DroppableTransport(peers(3), (target, message) -> {
            if (swallowed.compareAndSet(null, target)) {
                return null; // accepted, and silent -- exactly what a coordinator about to die does
            }
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    true, null, ClientErrorCode.NONE);
        });
        final DisCasClient c = newClient(transport);

        final CompletableFuture<Version> put = c.put(TestBytes.utf8("k"), TestBytes.utf8("v"));
        // Let the first attempt reach the transport before severing it.
        waitUntil(() -> swallowed.get() != null);

        final long startedAt = System.nanoTime();
        transport.drop(swallowed.get());
        put.get(10, TimeUnit.SECONDS);
        final Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertTrue(elapsed.compareTo(Duration.ofSeconds(1)) < 0,
                "The drop must drive the failover, not the " + PER_ATTEMPT.toSeconds()
                        + "s per-attempt timer; took " + elapsed.toMillis() + "ms");
        assertEquals(2, transport.addressed.size(),
                "Expected exactly one failover, saw " + transport.addressed);
    }

    @Test
    @DisplayName("A drop with nothing in flight is remembered, not ignored")
    void idleDropStillPenalisesThePeer() throws Exception {
        // Nothing is riding on the connection, so there is nothing to re-dispatch -- but the peer
        // is still down, and the next request must not pay a connect to discover that.
        final DroppableTransport transport = new DroppableTransport(peers(3), (target, message) -> {
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    true, null, ClientErrorCode.NONE);
        });
        final DisCasClient c = newClient(transport);

        // Learn which coordinator this key routes to, then sever it while idle.
        c.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(10, TimeUnit.SECONDS);
        final NodeId routed = transport.addressed.get(0);
        transport.drop(routed);

        c.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(10, TimeUnit.SECONDS);

        assertEquals(2, transport.addressed.size(), "Expected one send per put");
        assertTrue(!routed.equals(transport.addressed.get(1)),
                "The second put must skip the peer whose connection was lost, but went to "
                        + transport.addressed.get(1));
    }

    private static void waitUntil(final BooleanSupplier condition)
            throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("Condition not met within 5s");
            }
            Thread.sleep(5L);
        }
    }
}
