/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
 * {@link DisCasClientConfig} -- the client's deadlines, and that setting them changes behaviour.
 * <p>
 * Both timing tests use peers that are <em>reachable but silent</em>, which is the only case that
 * actually reaches a timeout: everything else settles early (a majority answers, every remaining
 * peer proves unreachable, or a majority becomes impossible). Each asserts an upper bound derived
 * from the configured value and comfortably below the default, so a client that ignored the config
 * would blow the bound rather than merely be slower.
 */
@DisplayName("DisCasClientConfig -- injectable deadlines")
class DisCasClientConfigTest {

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

    /** Accepts every send and never answers: up, reachable, and silent. */
    private static final class SilentTransport implements ClientTransport {
        final AtomicInteger sends = new AtomicInteger();

        @Override
        public void send(final NodeId target, final ClientMessage message) {
            sends.incrementAndGet();
        }

        @Override
        public void register(final Consumer<ClientMessage> handler) {
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

    private DisCasClient clientWith(final ClientTransport transport, final DisCasClientConfig cfg) {
        client = new DisCasClient(CLIENT, transport, new EventLoop("cas-client-cfg-test"),
                true, ClientObserver.NONE, cfg);
        return client;
    }

    @Test
    @DisplayName("A short scanTimeout settles the scan at that deadline, not the 10s default")
    void scanTimeoutIsHonoured() {
        final DisCasClient c = clientWith(new SilentTransport(), DisCasClientConfig.builder()
                .scanTimeout(Duration.ofMillis(600)).build());

        final long startNanos = System.nanoTime();
        assertThrows(ExecutionException.class,
                () -> c.scan(ByteBuffer.allocate(0), null, 100).get(20, TimeUnit.SECONDS));
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertTrue(elapsedMs < 5_000,
                "The scan must settle at the configured 600ms, not the 10s default; took "
                        + elapsedMs + "ms");
    }

    @Test
    @DisplayName("A short perAttemptTimeout paces failover; requestDeadline is what ends it")
    void perAttemptTimeoutIsHonoured() {
        final SilentTransport transport = new SilentTransport();
        final DisCasClient c = clientWith(transport, DisCasClientConfig.builder()
                .perAttemptTimeout(Duration.ofMillis(300))
                .requestDeadline(Duration.ofMillis(1_500))
                // Keep the pacing attributable to perAttemptTimeout rather than to backoff.
                .peerRetryMinBackoff(Duration.ofMillis(1))
                .peerRetryMaxBackoff(Duration.ofMillis(5))
                .build());

        final long startNanos = System.nanoTime();
        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> c.get(key()).get(20, TimeUnit.SECONDS));
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertEquals(RequestFailedException.Cause.TIMED_OUT,
                assertInstanceOf(RequestFailedException.class, thrown.getCause()).cause());
        // The walk is not bounded by the peer count: peers keep being retried, with backoff, until
        // the overall deadline. At 300ms an attempt, 1.5s fits several.
        assertTrue(transport.sends.get() > PEERS.size(),
                "Attempts must continue past one pass over the peer list, saw "
                        + transport.sends.get());
        assertTrue(elapsedMs < 6_000,
                "Failover must be paced by the configured 300ms, not the 5s default; took "
                        + elapsedMs + "ms");
    }

    @Test
    @DisplayName("Defaults are the previously hardcoded values, and an override is isolated")
    void defaultsAreUnchanged() {
        final DisCasClientConfig cfg = DisCasClientConfig.defaults();
        // Pinned because they were constants until now: a silent change to any of them changes the
        // behaviour of every caller that never touches the config.
        assertEquals(Duration.ofSeconds(5), cfg.perAttemptTimeout());
        assertEquals(Duration.ofSeconds(30), cfg.requestDeadline());
        assertEquals(Duration.ofMillis(50), cfg.peerRetryMinBackoff());
        assertEquals(Duration.ofSeconds(2), cfg.peerRetryMaxBackoff());
        assertEquals(Duration.ofSeconds(10), cfg.scanTimeout());
        assertEquals(Duration.ofSeconds(5), cfg.shutdownAwaitTimeout());
        assertEquals(Duration.ofMillis(20), cfg.lockMinBackoff());
        assertEquals(Duration.ofMillis(80), cfg.lockMaxBackoff());
        assertEquals(Duration.ofMillis(200), cfg.watchMinBackoff());
        assertEquals(Duration.ofMillis(1000), cfg.watchMaxBackoff());

        // And overriding one of them leaves the rest where they were.
        final DisCasClientConfig overridden = DisCasClientConfig.builder()
                .scanTimeout(Duration.ofSeconds(2)).build();
        assertEquals(Duration.ofSeconds(2), overridden.scanTimeout());
        assertEquals(Duration.ofSeconds(5), overridden.perAttemptTimeout());
        assertEquals(Duration.ofMillis(200), overridden.watchMinBackoff());
    }

    @Test
    @DisplayName("The built config is detached from the builder that made it")
    void buildDetachesFromBuilder() {
        final DisCasClientConfig.Builder builder =
                DisCasClientConfig.builder().scanTimeout(Duration.ofSeconds(2));
        final DisCasClientConfig built = builder.build();

        builder.scanTimeout(Duration.ofSeconds(7));

        assertEquals(Duration.ofSeconds(2), built.scanTimeout(),
                "A config handed to a client must not change under it when the builder is reused");
        assertEquals(Duration.ofSeconds(7), builder.build().scanTimeout());
    }

    @Test
    @DisplayName("Non-positive and null deadlines are rejected at the setter")
    void nonPositiveIsRejected() {
        final DisCasClientConfig.Builder b = DisCasClientConfig.builder();
        assertThrows(IllegalArgumentException.class, () -> b.scanTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> b.perAttemptTimeout(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> b.shutdownAwaitTimeout(null));
    }

    @Test
    @DisplayName("An inverted backoff window is rejected at build, not at the setter")
    void invertedBackoffWindowIsRejected() {
        // Deferred to build() on purpose: either half can legitimately be set first, so the setter
        // cannot tell an inverted pair from a half-applied one.
        final DisCasClientConfig.Builder lock = DisCasClientConfig.builder()
                .lockMinBackoff(Duration.ofMillis(500));
        assertThrows(IllegalArgumentException.class, lock::build);

        final DisCasClientConfig.Builder watch = DisCasClientConfig.builder()
                .watchMaxBackoff(Duration.ofMillis(50));
        assertThrows(IllegalArgumentException.class, watch::build);

        // ...and setting both halves consistently is fine, in either order.
        DisCasClientConfig.builder()
                .lockMaxBackoff(Duration.ofSeconds(2))
                .lockMinBackoff(Duration.ofMillis(500))
                .build();
    }

    private static ByteBuffer key() {
        return ByteBuffer.wrap("k".getBytes(StandardCharsets.UTF_8));
    }
}
