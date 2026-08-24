/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a watch does when the cluster stops being able to answer its polls.
 * <p>
 * A watch is a blocking query: the caller asks to be told within {@code maxWait} and the polling
 * underneath is an implementation detail. A partition makes each poll fail -- a linearizable read
 * is a Paxos round, and without a quorum there is no round -- so the question is whether a watch
 * with most of its budget left rides that out or gives up.
 * <p>
 * A scripted transport is used rather than a real partition because the interesting variable is
 * how many polls fail before one succeeds, and with a real cluster that is a timing accident.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Watch -- polls that fail while the cluster has no quorum")
class WatchUnderPartitionTest {

    private static final ClientId CLIENT = ClientId.of("c1");
    private static final List<NodeId> PEERS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));

    private DisCasClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * Answers the first {@code unavailablePolls} reads with {@code UNAVAILABLE} -- what a
     * coordinator returns when it cannot assemble a quorum -- and every later one with a committed
     * value, as though the partition had healed.
     */
    private static final class PartitionedTransport implements ClientTransport {
        private final int failingPolls;
        private final ClientErrorCode code;
        final AtomicInteger polls = new AtomicInteger();
        private Consumer<ClientMessage> handler;

        PartitionedTransport(final int failingPolls) {
            this(failingPolls, ClientErrorCode.UNAVAILABLE);
        }

        PartitionedTransport(final int failingPolls, final ClientErrorCode code) {
            this.failingPolls = failingPolls;
            this.code = code;
        }

        @Override
        public void send(final NodeId target, final ClientMessage message) {
            if (!(message instanceof ClientMessage.ClientGetReq) || handler == null) {
                return;
            }
            final ClientMessage.ClientGetReq req = (ClientMessage.ClientGetReq) message;
            if (polls.incrementAndGet() <= failingPolls) {
                handler.accept(new ClientMessage.ClientGetResp(
                        target.value(), req.correlationId(), false, null,
                        "refused", code));
                return;
            }
            handler.accept(new ClientMessage.ClientGetResp(
                    target.value(), req.correlationId(), true,
                    ByteBuffer.wrap("healed".getBytes(StandardCharsets.UTF_8)), null,
                    ClientErrorCode.NONE, new Ballot(7L, target)));
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

    private DisCasClient clientOver(final ClientTransport transport) {
        client = new DisCasClient(CLIENT, transport, new EventLoop("cas-client-watch-test"),
                true, ClientObserver.NONE, DisCasClientConfig.builder()
                        // Poll briskly so a handful of attempts fit in the test's budget.
                        .watchMinBackoff(Duration.ofMillis(20))
                        .watchMaxBackoff(Duration.ofMillis(40))
                        .build());
        return client;
    }

    private static ByteBuffer key() {
        return ByteBuffer.wrap("k".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("A watch rides out a quorum outage and fires once the cluster recovers")
    void watchSurvivesTransientQuorumLoss() throws Exception {
        final PartitionedTransport transport = new PartitionedTransport(3);
        final DisCasClient c = clientOver(transport);

        // The caller asked to be told within 10s. Three failed polls at ~30ms apart use a tiny
        // fraction of that budget, so a watch that gives up has discarded almost all of it.
        final WatchResult result = c.watch(key(), Version.INITIAL, Duration.ofSeconds(10), ReadConsistency.LINEARIZABLE)
                .get(20, TimeUnit.SECONDS);

        assertTrue(result.changed(), "The watch must report the value that appeared after recovery");
        assertTrue(transport.polls.get() > 3,
                "The watch must have polled past the outage, saw " + transport.polls.get());
    }

    @Test
    @DisplayName("A watch whose whole budget is spent without quorum still reports the outage")
    void watchThatNeverRecoversReportsTheOutage() {
        final PartitionedTransport transport = new PartitionedTransport(Integer.MAX_VALUE);
        final DisCasClient c = clientOver(transport);

        // Nothing to report and no way to find out: this must resolve at the deadline rather than
        // hang, and the caller has to be able to tell it apart from "unchanged".
        final long startNanos = System.nanoTime();
        assertThrows(ExecutionException.class,
                () -> c.watch(key(), Version.INITIAL, Duration.ofMillis(500), ReadConsistency.LINEARIZABLE)
                        .get(20, TimeUnit.SECONDS));
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertTrue(elapsedMs < 10_000, "Must resolve promptly, took " + elapsedMs + "ms");
        assertTrue(transport.polls.get() >= 1);
    }

    @Test
    @DisplayName("A caller error ends the watch at once rather than stalling for the whole budget")
    void callerErrorIsNotRetried() {
        // The other half of the retry rule. ACCESS_DENIED fails identically however many times it
        // is retried, so riding it out would turn an immediate answer into a ten-second wait for
        // the same one -- and, over the agent, a prompt 403 into a gateway timeout.
        final PartitionedTransport transport =
                new PartitionedTransport(Integer.MAX_VALUE, ClientErrorCode.ACCESS_DENIED);
        final DisCasClient c = clientOver(transport);

        final long startNanos = System.nanoTime();
        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> c.watch(key(), Version.INITIAL, Duration.ofSeconds(10),
                        ReadConsistency.LINEARIZABLE).get(20, TimeUnit.SECONDS));
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertEquals(ClientErrorCode.ACCESS_DENIED,
                assertInstanceOf(DisCasOperationException.class, thrown.getCause()).code());
        assertEquals(1, transport.polls.get(), "A caller error must not be polled again");
        assertTrue(elapsedMs < 2_000,
                "Must fail immediately, not after the 10s budget; took " + elapsedMs + "ms");
    }
}
