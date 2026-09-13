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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How often a watch goes back to the cluster, and what the caller can say about it.
 * <p>
 * A watch is a standing query answered by polling, so its period is load on every node for as long
 * as the watch stands. The pacing is what this file pins: a floor the caller cannot go under, a gap
 * counted from the answer rather than from the request, and a budget the pacing may not overrun.
 * <p>
 * A scripted transport rather than a cluster, because the variable of interest is when each poll
 * was sent, and against a real cluster that is a timing accident.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("Watch -- how often it polls")
class WatchPollPacingTest {

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

    @Test
    @DisplayName("Refuses a period under the floor rather than quietly raising it")
    void refusesAPeriodUnderTheFloor() {
        final DisCasClient c = clientOver(new SteadyTransport(Duration.ZERO));

        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> c.watch(key(), Version.INITIAL, Duration.ofSeconds(1), ReadConsistency.LINEARIZABLE,
                        DisCasClient.MIN_WATCH_POLL_PERIOD.minusMillis(1)).get(20, TimeUnit.SECONDS));

        assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
    }

    @Test
    @DisplayName("The period is a floor, counted from the answer and not from the request")
    void spacesPollsByThePeriodAfterEachAnswer() throws Exception {
        // A slow round is the case the wording is about: a gap counted from the request would let
        // the next poll follow this one almost at once, and a watch on a struggling cluster would
        // poll it hardest exactly when it is least able to answer.
        final Duration roundTrip = Duration.ofMillis(300);
        final Duration period = DisCasClient.MIN_WATCH_POLL_PERIOD;
        final SteadyTransport transport = new SteadyTransport(roundTrip);
        final DisCasClient c = clientOver(transport);

        c.watch(key(), SEEN, Duration.ofSeconds(5), ReadConsistency.LINEARIZABLE, period)
                .get(30, TimeUnit.SECONDS);

        // Every gap but the last, which the budget shortens on purpose -- see the test below. What
        // is checked here is that the rest are the round trip plus the period and not the period
        // alone, which is the difference between counting from the answer and from the request.
        final List<Long> sentAtMs = transport.sentAtMs();
        assertTrue(sentAtMs.size() > 2, "too few polls to pace anything, saw " + sentAtMs.size());
        final long floorMs = roundTrip.plus(period).toMillis() - SCHEDULER_SLACK_MS;
        for (int poll = 1; poll < sentAtMs.size() - 1; poll++) {
            final long gapMs = sentAtMs.get(poll) - sentAtMs.get(poll - 1);
            assertTrue(gapMs >= floorMs,
                    "poll " + poll + " came " + gapMs + "ms after the one before, under " + floorMs);
        }
    }

    @Test
    @DisplayName("Pacing never outstays the budget the caller was promised")
    void answersWithinTheBudget() throws Exception {
        // The gap is drawn up to five times the period, which is longer than this whole budget. It
        // is the budget that wins: a caller told 1s must not wait 2.5s because a poll gap was
        // rolled after the last poll that could still have fitted.
        final SteadyTransport transport = new SteadyTransport(Duration.ZERO);
        final DisCasClient c = clientOver(transport);

        final long startNanos = System.nanoTime();
        final WatchResult result = c
                .watch(key(), SEEN, Duration.ofSeconds(1), ReadConsistency.LINEARIZABLE,
                        DisCasClient.MIN_WATCH_POLL_PERIOD)
                .get(30, TimeUnit.SECONDS);
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertFalse(result.changed(), "the key never moved, so the watch reports it unchanged");
        assertTrue(elapsedMs < 1_500,
                "a 1s budget must not become a poll gap's worth longer; took " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("A longer period is fewer polls over the same budget")
    void aLongerPeriodPollsLess() throws Exception {
        final SteadyTransport brisk = new SteadyTransport(Duration.ZERO);
        clientOver(brisk).watch(key(), SEEN, Duration.ofSeconds(3), ReadConsistency.LINEARIZABLE,
                DisCasClient.MIN_WATCH_POLL_PERIOD).get(30, TimeUnit.SECONDS);
        client.close();

        final SteadyTransport sedate = new SteadyTransport(Duration.ZERO);
        clientOver(sedate).watch(key(), SEEN, Duration.ofSeconds(3), ReadConsistency.LINEARIZABLE,
                Duration.ofSeconds(3)).get(30, TimeUnit.SECONDS);

        assertTrue(brisk.sentAtMs().size() > sedate.sentAtMs().size(),
                "500ms paced " + brisk.sentAtMs().size() + " polls and 3s paced "
                        + sedate.sentAtMs().size());
    }

    /** The version the watch is told it has already seen, so every answer below is "unchanged". */
    private static final Version SEEN = Version.parse("7:1");

    /** What a scheduled wake-up may run late by on a loaded machine before it means anything. */
    private static final long SCHEDULER_SLACK_MS = 60L;

    private DisCasClient clientOver(final ClientTransport transport) {
        client = new DisCasClient(CLIENT, transport, new EventLoop("cas-client-pacing-test"),
                true, ClientObserver.NONE, DisCasClientConfig.defaults());
        return client;
    }

    private static ByteBuffer key() {
        return ByteBuffer.wrap("k".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Answers every read with the same committed value at the version the watch already has, so
     * nothing ever changes and the watch polls until its budget runs out. Records when each poll
     * was sent, and optionally takes {@code roundTrip} to answer it.
     */
    private static final class SteadyTransport implements ClientTransport {
        private final Duration roundTrip;
        private final List<Long> sentAtMs = new ArrayList<>();
        private Consumer<ClientMessage> handler;

        SteadyTransport(final Duration roundTrip) {
            this.roundTrip = roundTrip;
        }

        synchronized List<Long> sentAtMs() {
            return new ArrayList<>(sentAtMs);
        }

        @Override
        public void send(final NodeId target, final ClientMessage message) {
            if (!(message instanceof ClientMessage.ClientGetReq) || handler == null) {
                return;
            }
            final ClientMessage.ClientGetReq req = (ClientMessage.ClientGetReq) message;
            synchronized (this) {
                sentAtMs.add(System.nanoTime() / 1_000_000L);
            }
            if (!roundTrip.isZero()) {
                try {
                    Thread.sleep(roundTrip.toMillis());
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            handler.accept(new ClientMessage.ClientGetResp(
                    target.value(), req.correlationId(), true,
                    ByteBuffer.wrap("steady".getBytes(StandardCharsets.UTF_8)), null,
                    ClientErrorCode.NONE, new Ballot(7L, NodeId.of("1"))));
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
}
