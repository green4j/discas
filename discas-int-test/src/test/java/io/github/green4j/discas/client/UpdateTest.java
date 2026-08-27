/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestCluster;
import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static io.github.green4j.discas.TestBytes.string;
import static io.github.green4j.discas.TestBytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code update} is read -> transform -> version-fenced CAS, looping while the compare is lost.
 * <p>
 * The invariant the whole operation rests on is that <b>only a lost compare</b> is retried. A
 * lost compare is a fact the cluster stated; a timeout is the absence of a fact, and re-running
 * {@code v -> v + 1} against it would increment twice. The scripted-transport cases below pin
 * that boundary exactly -- they need the cluster to say precise things at precise moments, which
 * a real one cannot be made to do -- and the cluster cases pin that the loop actually converges.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("update -- read, transform, write, retry only on a lost compare")
class UpdateTest {

    private static final long TIMEOUT_MS = 10_000L;
    private static final ClientId SCRIPTED_CLIENT = ClientId.of("update-scripted");
    private static final List<NodeId> SCRIPTED_PEERS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));

    private TestCluster cluster;

    @BeforeAll
    void setUp() throws Exception {
        cluster = new TestCluster(3, 3);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterAll
    void tearDown() {
        cluster.close();
    }

    @Test
    @DisplayName("Applies the transform to the current value and returns what it committed")
    void appliesTransformToCurrentValue() throws Exception {
        final DisCasClient client = cluster.client(0);
        final String key = "update/plain";
        client.put(key, utf8("1")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        final GetResult result = client
                .update(key, current -> Integer.toString(Integer.parseInt(current) + 1))
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertEquals("2", string(result.value()));
        assertEquals("2", string(client.get(key).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value()));
        assertEquals(result.version(),
                client.get(key).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).version(),
                "the returned version must be the one the write committed at");
    }

    @Test
    @DisplayName("A key that was never written arrives at the transform as null")
    void absentKeyIsSeenAsNull() throws Exception {
        final DisCasClient client = cluster.client(0);
        final String key = "update/absent";

        final GetResult created = client
                .update(key, current -> current == null ? "seeded" : "wrong")
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertEquals("seeded", string(created.value()));
        assertNotEquals(Version.INITIAL, created.version());
    }

    @Test
    @DisplayName("Returning null tombstones the key, and the version still advances")
    void returningNullTombstones() throws Exception {
        final DisCasClient client = cluster.client(0);
        final ByteBuffer key = utf8("update/tombstone");
        final Version atValue = client.put(key.duplicate(), utf8("doomed"))
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        final GetResult deleted = client.update(key.duplicate(), current -> null)
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertNull(deleted.value(), "a tombstone has no value");
        assertNotEquals(atValue, deleted.version(), "a delete is a commit like any other");
        assertNull(client.get(key.duplicate()).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value());
    }

    @Test
    @DisplayName("Throwing from the transform abandons the update and writes nothing")
    void throwingFromTransformWritesNothing() throws Exception {
        final DisCasClient client = cluster.client(0);
        final String key = "update/abandoned";
        client.put(key, utf8("untouched")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> client.update(key, current -> {
                    throw new IllegalStateException("Not today");
                }).get(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        assertInstanceOf(IllegalStateException.class, thrown.getCause());
        assertEquals("untouched",
                string(client.get(key).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value()));
    }

    @Test
    @DisplayName("Concurrent writers on one key lose no increment")
    void concurrentWritersLoseNoIncrement() throws Exception {
        final String key = "update/counter";
        final int writers = 3;
        final int perWriter = 10;
        cluster.client(0).put(key, utf8("0")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        final ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> running = new ArrayList<>(writers);
            for (int w = 0; w < writers; w++) {
                final DisCasClient client = cluster.client(w);
                running.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        client.update(key,
                                        current -> Integer.toString(Integer.parseInt(current) + 1),
                                        Duration.ofSeconds(30))
                                .get(TIMEOUT_MS * 3, TimeUnit.MILLISECONDS);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (final Future<?> f : running) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(Integer.toString(writers * perWriter),
                string(cluster.client(0).get(key).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value()),
                "every increment must be in the register -- that is what the retry loop is for");
    }

    @Test
    @DisplayName("putIfAbsent creates once; the loser is told who won without a second read")
    void putIfAbsentCreatesOnce() throws Exception {
        final DisCasClient client = cluster.client(0);
        final String key = "update/put-if-absent";

        final CasResult created = client.putIfAbsent(key, "first")
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertTrue(created.swapped());

        final CasResult refused = client.putIfAbsent(key, "second")
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals(false, refused.swapped());
        assertEquals("first", string(refused.value()));
        assertEquals(created.version(), refused.version());
    }

    /**
     * The cases that need the cluster to answer in a scripted way: a lost compare that never stops
     * being lost, and a CAS that is never answered at all. Neither can be provoked reliably on a
     * real cluster, and both are the point of the operation.
     */
    @Nested
    @DisplayName("Only a lost compare is retried")
    class RetryBoundary {

        @Test
        @DisplayName("A compare that keeps losing ends on the retry budget, not on a timeout")
        void budgetExhaustedOnPerpetualLoss() throws Exception {
            final AtomicInteger casAttempts = new AtomicInteger();
            final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
                if (message instanceof ClientMessage.ClientGetReq) {
                    final ClientMessage.ClientGetReq get = (ClientMessage.ClientGetReq) message;
                    return new ClientMessage.ClientGetResp(target.value(), get.correlationId(),
                            true, utf8("1"), null, ClientErrorCode.NONE, new Ballot(7, target));
                }
                final ClientMessage.ClientCasReq cas = (ClientMessage.ClientCasReq) message;
                casAttempts.incrementAndGet();
                // Somebody else always got there first, and says so.
                return new ClientMessage.ClientCasResp(target.value(), cas.correlationId(),
                        true, false, utf8("winner"), new Ballot(9, target), null,
                        ClientErrorCode.NONE);
            });
            try (DisCasClient client = new DisCasClient(SCRIPTED_CLIENT, transport)) {
                final ExecutionException thrown = assertThrows(ExecutionException.class,
                        () -> client.update(utf8("hot"), current -> utf8("mine"),
                                Duration.ofMillis(200)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS));

                final UpdateContendedException contended = assertInstanceOf(
                        UpdateContendedException.class, thrown.getCause());
                assertTrue(contended.attempts() > 1,
                        "the loop must have retried, saw " + contended.attempts());
                assertEquals(contended.attempts(), casAttempts.get());
                assertTrue(contended.isTransient());
            }
        }

        @Test
        @DisplayName("An unanswered CAS fails the update -- it is never re-sent")
        void unansweredCasIsNotRetried() throws Exception {
            // The read is answered so the transform runs; the write is swallowed, so the outcome
            // is unknown. Re-running the transform here is exactly the double-apply this operation
            // refuses to risk: only the caller can know whether that is safe.
            final AtomicInteger transformCalls = new AtomicInteger();
            final AtomicInteger casSends = new AtomicInteger();
            final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
                if (message instanceof ClientMessage.ClientGetReq) {
                    final ClientMessage.ClientGetReq get = (ClientMessage.ClientGetReq) message;
                    return new ClientMessage.ClientGetResp(target.value(), get.correlationId(),
                            true, utf8("1"), null, ClientErrorCode.NONE, new Ballot(7, target));
                }
                casSends.incrementAndGet();
                return null;
            });
            final DisCasClientConfig config = DisCasClientConfig.builder()
                    .perAttemptTimeout(Duration.ofMillis(150))
                    .requestDeadline(Duration.ofMillis(400))
                    .build();
            try (DisCasClient client = new DisCasClient(SCRIPTED_CLIENT, transport,
                    new EventLoop("update-test"), true,
                    ClientObserver.NONE, config)) {
                final ExecutionException thrown = assertThrows(ExecutionException.class,
                        () -> client.update(utf8("silent"), current -> {
                            transformCalls.incrementAndGet();
                            return utf8("mine");
                        }, Duration.ofSeconds(30)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS));

                assertEquals(1, transformCalls.get(),
                        "an unknown outcome must not run the transform a second time");
                assertEquals(false, thrown.getCause() instanceof UpdateContendedException,
                        "a silent cluster is not a contended key");
            }
        }
    }

    /** Answers each request according to {@code responder}; a {@code null} reply is silence. */
    private static final class ScriptedTransport implements ClientTransport {
        private final BiFunction<NodeId, ClientMessage, ClientMessage> responder;
        private Consumer<ClientMessage> handler;

        ScriptedTransport(final BiFunction<NodeId, ClientMessage, ClientMessage> responder) {
            this.responder = responder;
        }

        @Override
        public void send(final NodeId target, final ClientMessage message) {
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
            return SCRIPTED_PEERS;
        }

        @Override
        public int clusterSize() {
            return SCRIPTED_PEERS.size();
        }
    }
}
