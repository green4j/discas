/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.TransportUnavailableException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
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
 * Failover discipline: short when there is an alternative, incrementing when there is not.
 * <p>
 * The size of the coordinator list is the axis that matters, which is why the retry cases run for
 * M = 1, 2, 3 and 5. A bound of {@code attempt < peers.size()} would kill a request on its first
 * failure with a single configured coordinator, no retry and no reconnect at all -- and the agent's
 * nodes-file bootstrap runs on exactly that shape until the full membership loads. Everything here
 * is driven by a fake transport, so no real timer has to elapse beyond the tiny backoffs below.
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES)
@DisplayName("DisCasClient -- per-peer backoff and failover discipline")
class PeerBackoffTest {

    private static final ClientId CLIENT = ClientId.of("c1");
    // A wide min-to-max ratio, so an escalated backoff is plainly distinguishable from a reset
    // one by how many attempts fit in the deadline: ~7 when escalation restarts from the base,
    // ~2 when the peer is still pinned at the ceiling.
    private static final Duration MIN_BACKOFF = Duration.ofMillis(5);
    private static final Duration MAX_BACKOFF = Duration.ofMillis(400);
    private static final Duration DEADLINE = Duration.ofMillis(600);

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
        return out;
    }

    private static DisCasClientConfig config(final Duration min, final Duration max,
                                             final Duration deadline, final Duration perAttempt) {
        return DisCasClientConfig.builder()
                .perAttemptTimeout(perAttempt)
                .requestDeadline(deadline)
                .peerRetryMinBackoff(min)
                .peerRetryMaxBackoff(max)
                .build();
    }

    /** The retry-shaped config: tiny base, wide ceiling, short deadline. */
    private static DisCasClientConfig retryConfig() {
        return config(MIN_BACKOFF, MAX_BACKOFF, DEADLINE, Duration.ofMillis(100));
    }

    /** Owns its loop, so {@code close()} in teardown shuts everything down. */
    private static DisCasClient newClient(final ClientTransport transport) {
        return newClient(transport, retryConfig());
    }

    private static DisCasClient newClient(final ClientTransport transport,
                                          final DisCasClientConfig cfg) {
        return new DisCasClient(CLIENT, transport, new EventLoop("peer-backoff"), true,
                ClientObserver.NONE, cfg);
    }

    /** Answers according to {@code responder}; a null reply means silence. */
    private static final class ScriptedTransport implements ClientTransport {
        private final List<NodeId> addressed = new ArrayList<>();
        private final List<NodeId> peerList;
        private final BiFunction<NodeId, ClientMessage, ClientMessage> responder;
        private Consumer<ClientMessage> handler;

        ScriptedTransport(final List<NodeId> peerList,
                          final BiFunction<NodeId, ClientMessage, ClientMessage> responder) {
            this.peerList = peerList;
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
            return peerList;
        }

        @Override
        public int clusterSize() {
            return peerList.size();
        }
    }

    /** A transport that cannot reach anyone -- every send throws, as a failed connect does. */
    private static ScriptedTransport unreachable(final List<NodeId> peerList) {
        return new ScriptedTransport(peerList, (target, message) -> {
            throw new TransportUnavailableException(
                    TransportUnavailableException.Reason.CONNECT_FAILED, "connect failed " + target);
        });
    }

    @ParameterizedTest(name = "M={0}")
    @ValueSource(ints = {1, 2, 3, 5})
    @DisplayName("An unreachable cluster is retried with backoff until the deadline, not abandoned")
    void unreachableClusterRetriesUntilDeadline(final int m) {
        final ScriptedTransport transport = unreachable(peers(m));
        client = newClient(transport);

        final long startedAt = System.nanoTime();
        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(30, TimeUnit.SECONDS));
        final Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        final RequestFailedException cause =
                assertInstanceOf(RequestFailedException.class, thrown.getCause());
        // Never reached the wire, so the cause must say so rather than blaming the clock.
        assertEquals(RequestFailedException.Cause.ALL_PEERS_EXHAUSTED, cause.cause());

        // It keeps trying rather than giving up after one pass over the peer list.
        assertTrue(transport.addressed.size() > m,
                "Expected retries beyond one pass over " + m + " peer(s), saw "
                        + transport.addressed.size());
        // And it backed off rather than spinning: ensureConnected opens a fresh socket per send
        // and rate-limits nothing itself, so an unthrottled loop would be a connect storm.
        final long maxPlausible = 4 * (DEADLINE.toMillis() / MIN_BACKOFF.toMillis() + m);
        assertTrue(transport.addressed.size() < maxPlausible,
                "Expected backoff to bound the attempts, saw " + transport.addressed.size());
        // The deadline is what ends it, so it must not end appreciably early.
        assertTrue(elapsed.toMillis() >= DEADLINE.toMillis() / 2,
                "Gave up after only " + elapsed.toMillis() + "ms");
    }

    @ParameterizedTest(name = "M={0}")
    @ValueSource(ints = {2, 3, 5})
    @DisplayName("A healthy alternative is used at once -- no backoff delay is inserted")
    void healthyAlternativeIsImmediate(final int m) throws Exception {
        // The first coordinator addressed refuses; any other serves. With an alternative available
        // the switch must cost a message, not a backoff.
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport(peers(m), (target, message) -> {
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            if (sends.getAndIncrement() == 0) {
                return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                        false, "no majority", ClientErrorCode.NO_QUORUM_AT_COORDINATOR);
            }
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    true, null, ClientErrorCode.NONE);
        });
        // A deliberately huge backoff: if the switch consulted it at all, this test would take
        // 2s. Anything under a fraction of that proves the alternative was taken immediately.
        final Duration hugeBackoff = Duration.ofSeconds(2);
        client = newClient(transport,
                config(hugeBackoff, hugeBackoff, Duration.ofSeconds(30), Duration.ofSeconds(5)));

        final long startedAt = System.nanoTime();
        client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(10, TimeUnit.SECONDS);
        final Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertEquals(2, transport.addressed.size(), "Expected exactly one failover");
        assertTrue(elapsed.toMillis() < 500,
                "A switch with an alternative available must insert no delay, took "
                        + elapsed.toMillis() + "ms against a " + hugeBackoff.toMillis()
                        + "ms per-peer backoff");
    }

    @Test
    @DisplayName("Backoff is re-settable -- a recovered peer starts from base again")
    void backoffResetsAfterRecovery() throws Exception {
        // Peer state is keyed per peer, so drive a single-coordinator client: fail it, let it
        // recover, then fail it again and check the second outage is not punished harder.
        final AtomicInteger sends = new AtomicInteger();
        final boolean[] healthy = {false};
        final ScriptedTransport transport = new ScriptedTransport(peers(1), (target, message) -> {
            sends.incrementAndGet();
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            if (!healthy[0]) {
                throw new TransportUnavailableException(
                        TransportUnavailableException.Reason.CONNECT_FAILED, "down");
            }
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    true, null, ClientErrorCode.NONE);
        });
        client = newClient(transport);

        // Outage one: escalates the backoff towards the maximum.
        assertThrows(ExecutionException.class,
                () -> client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(30, TimeUnit.SECONDS));

        // Recovery. The success must clear the escalation, not decay it.
        healthy[0] = true;
        client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(10, TimeUnit.SECONDS);

        // Outage two: if the counter had survived, the peer would sit at MAX_BACKOFF from the
        // first attempt and manage far fewer tries within the same deadline.
        healthy[0] = false;
        sends.set(0);
        assertThrows(ExecutionException.class,
                () -> client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(30, TimeUnit.SECONDS));
        final int afterReset = sends.get();

        // A peer still pinned at the 400ms ceiling manages one or two tries in the 600ms budget;
        // one that restarted from the 5ms base manages several. The gap is the assertion.
        final long atMaxBackoff = DEADLINE.toMillis() / MAX_BACKOFF.toMillis() + 1;
        assertTrue(afterReset > atMaxBackoff,
                "Backoff did not reset: " + afterReset + " attempts is around what a peer pinned "
                        + "at MAX_BACKOFF would manage, expected clearly more");
    }

    @Test
    @DisplayName("A caller error leaves the coordinator's backoff untouched")
    void callerErrorDoesNotPenaliseThePeer() {
        // ACCESS_DENIED is a property of the request, not of the node. Penalising the peer would
        // route later traffic away from a coordinator that is working perfectly.
        final ScriptedTransport transport = new ScriptedTransport(peers(3), (target, message) -> {
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    false, ClientMessage.ERR_ACCESS_DENIED, ClientErrorCode.ACCESS_DENIED);
        });
        client = newClient(transport);

        for (int i = 0; i < 3; i++) {
            assertThrows(ExecutionException.class,
                    () -> client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(10, TimeUnit.SECONDS));
        }

        // One attempt each: a refusal that repeats everywhere is reported at once, and no attempt
        // was ever delayed by a backoff the peer should not have accrued.
        assertEquals(3, transport.addressed.size(),
                "A caller error must cost exactly one attempt per call, saw "
                        + transport.addressed);
    }
}
