/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A coordinator that takes the request and then says nothing -- the case with no error code to
 * read, and so the case where only the shape of the request can decide what happens next.
 * <p>
 * This is the harder half of the rule {@code worthAnotherCoordinator} states for answered
 * failures. An explicit refusal names the phase it failed in, which is what lets even an unfenced
 * write move on a node-local {@code NOT_READY}. Silence names nothing: the coordinator may be
 * driving a round to completion right now. So a version-fenced write moves on -- its duplicate
 * carries an overtaken ballot and provably cannot apply -- a read moves on because a duplicate
 * read changes nothing, and an unfenced write stops, because a duplicate of one that lands after
 * an intervening writer reverts them.
 * <p>
 * The per-attempt timeout is kept short so the walk is driven by it and not by the test's patience.
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES)
@DisplayName("DisCasClient -- a coordinator that accepts the request and stays silent")
class SilentCoordinatorTest {

    private static final ClientId CLIENT = ClientId.of("c1");
    private static final List<NodeId> PEERS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));
    private static final Duration PER_ATTEMPT = Duration.ofMillis(200);

    private DisCasClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    /** Records every peer addressed and answers only when {@code responder} chooses to. */
    private static final class ScriptedTransport implements ClientTransport {
        private final List<NodeId> addressed = Collections.synchronizedList(new ArrayList<>());
        private final BiFunction<NodeId, ClientMessage, ClientMessage> responder;
        private Consumer<ClientMessage> handler;

        ScriptedTransport(final BiFunction<NodeId, ClientMessage, ClientMessage> responder) {
            this.responder = responder;
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
        public List<NodeId> peers() {
            return PEERS;
        }

        @Override
        public int clusterSize() {
            return PEERS.size();
        }
    }

    private DisCasClient newClient(final ClientTransport transport) {
        client = new DisCasClient(CLIENT, transport, new EventLoop("silent-coordinator"), true,
                ClientObserver.NONE,
                DisCasClientConfig.builder()
                        .perAttemptTimeout(PER_ATTEMPT)
                        .requestDeadline(Duration.ofSeconds(5))
                        .peerRetryMinBackoff(Duration.ofMillis(1))
                        .peerRetryMaxBackoff(Duration.ofMillis(5))
                        .build());
        return client;
    }

    @Test
    @DisplayName("A fenced write walks past the silent coordinator and lands on the next")
    void fencedWriteWalksPastSilence() throws Exception {
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            if (sends.getAndIncrement() == 0) {
                return null; // took it, said nothing
            }
            final ClientMessage.ClientCasReq cas = (ClientMessage.ClientCasReq) message;
            return new ClientMessage.ClientCasResp(target.value(), cas.correlationId(),
                    true, true, null, new Ballot(5L, NodeId.of("2")), null, ClientErrorCode.NONE);
        });
        final DisCasClient c = newClient(transport);

        final CasResult result = c.cas(TestBytes.utf8("k"),
                new Version(new Ballot(4L, NodeId.of("1"))), TestBytes.utf8("v"))
                .get(10, TimeUnit.SECONDS);

        assertTrue(result.swapped());
        assertEquals(2, transport.addressed.size(),
                "The fenced write must be re-sent exactly once, saw " + transport.addressed);
    }

    @Test
    @DisplayName("A read walks past it too -- a duplicate read changes nothing")
    void readWalksPastSilence() throws Exception {
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            if (sends.getAndIncrement() == 0) {
                return null;
            }
            final ClientMessage.ClientGetReq get = (ClientMessage.ClientGetReq) message;
            return new ClientMessage.ClientGetResp(target.value(), get.correlationId(),
                    true, ByteBuffer.wrap("v".getBytes()), null, ClientErrorCode.NONE);
        });
        final DisCasClient c = newClient(transport);

        c.get(TestBytes.utf8("k")).get(10, TimeUnit.SECONDS);

        assertEquals(2, transport.addressed.size(),
                "The read must be re-sent exactly once, saw " + transport.addressed);
    }

    @Test
    @DisplayName("An unfenced write stops at the silent coordinator with an unknown outcome")
    void unfencedWriteStopsAtSilence() {
        // Every peer would answer if asked -- the point is that only one is ever asked.
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            if (sends.getAndIncrement() == 0) {
                return null;
            }
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    true, null, ClientErrorCode.NONE);
        });
        final DisCasClient c = newClient(transport);

        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> c.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(10, TimeUnit.SECONDS));

        assertEquals(RequestFailedException.Cause.INDETERMINATE,
                assertInstanceOf(RequestFailedException.class, thrown.getCause()).cause());
        assertEquals(1, transport.addressed.size(),
                "The unfenced write must not be re-sent, saw " + transport.addressed);
    }

    @Test
    @DisplayName("An unfenced write that never reached the wire is safe to move, and does")
    void unfencedWriteThatNeverLeftIsStillMoved() throws Exception {
        // The distinction the rule turns on: a coordinator that was never handed the request
        // cannot be driving a round for it, so there is no duplicate to fear.
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            if (sends.getAndIncrement() == 0) {
                throw new IllegalStateException("Connection refused");
            }
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    true, null, ClientErrorCode.NONE);
        });
        final DisCasClient c = newClient(transport);

        c.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(10, TimeUnit.SECONDS);

        assertEquals(2, transport.addressed.size(),
                "A send that threw must be retried elsewhere, saw " + transport.addressed);
    }
}
