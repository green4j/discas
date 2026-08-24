/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ScanCoverage} -- returning a listing from fewer than a majority, and saying so.
 * <p>
 * Quorum is what makes a scan's completeness guarantee true, so it stays the default. But
 * refusing outright is too blunt for a read-only listing: an operator inspecting a cluster that
 * has lost quorum, or the agent's {@code --nodes-file} bootstrap (which starts on a subset by
 * design), can both use an incomplete answer. The rule is that partial results are fine as long
 * as they are <em>labelled</em> -- the original defect was a scan that returned partial data while
 * looking complete.
 * <p>
 * A scripted transport is used so coverage is exact: with a real cluster, which nodes answer is a
 * timing question, and these assertions are about precise counts.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Scan coverage -- quorum by default, labelled partial on request")
class ScanCoverageTest {

    private static final ClientId CLIENT = ClientId.of("c1");
    private static final List<NodeId> PEERS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"), NodeId.of("4"), NodeId.of("5"));

    private DisCasClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * Answers a scan from exactly the named nodes. The rest are either <em>unreachable</em> (the
     * send throws, as for a node that is down) or <em>silent</em> (the send succeeds but no answer
     * comes, as for a node that is up but partitioned). The distinction matters: unreachable peers
     * let the scan settle at once, silent ones can only be resolved by the timeout.
     */
    private static final class PartialTransport implements ClientTransport {
        private final Set<String> answering;
        private final int clusterSize;
        private final boolean othersUnreachable;
        private Consumer<ClientMessage> handler;

        PartialTransport(final int clusterSize, final String... answering) {
            this(clusterSize, true, answering);
        }

        PartialTransport(final int clusterSize, final boolean othersUnreachable,
                         final String... answering) {
            this.clusterSize = clusterSize;
            this.othersUnreachable = othersUnreachable;
            this.answering = Set.of(answering);
        }

        @Override
        public void send(final NodeId target, final ClientMessage message) {
            if (!answering.contains(target.value()) || handler == null) {
                if (othersUnreachable) {
                    throw new IllegalStateException("Node " + target + " is down");
                }
                return; // reachable but silent
            }
            final ClientMessage.ClientScanReq req = (ClientMessage.ClientScanReq) message;
            final List<ClientMessage.ScanEntry> entries = new ArrayList<>();
            entries.add(new ClientMessage.ScanEntry(
                    ByteBuffer.wrap(("key-" + target.value()).getBytes(StandardCharsets.UTF_8)),
                    new Ballot(1, target), false));
            handler.accept(new ClientMessage.ClientScanResp(
                    target.value(), req.correlationId(), entries, false));
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
            return clusterSize;
        }
    }

    private DisCasClient clientOver(final ClientTransport transport) {
        client = new DisCasClient(CLIENT, transport);
        return client;
    }

    private static ByteBuffer all() {
        return HashedBytes.EMPTY.toBuffer();
    }

    @Test
    @DisplayName("QUORUM fails below a majority rather than returning a short list")
    void quorumFailsBelowMajority() {
        // 2 of 5: a key committed on the other three can be missing from both answers.
        final DisCasClient c = clientOver(new PartialTransport(5, "1", "2"));

        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> c.scan(all(), null, 100).get(20, TimeUnit.SECONDS));

        final RequestFailedException cause =
                assertInstanceOf(RequestFailedException.class, thrown.getCause());
        assertEquals(RequestFailedException.Cause.SCAN_NO_QUORUM, cause.cause());
    }

    @Test
    @DisplayName("ANY_AVAILABLE returns the partial listing and reports how partial")
    void anyAvailableReturnsLabelledPartial() throws Exception {
        final DisCasClient c = clientOver(new PartialTransport(5, "1", "2"));

        final ScanPage page =
                c.scan(all(), null, 100, ScanCoverage.ANY_AVAILABLE).get(20, TimeUnit.SECONDS);

        assertEquals(2, page.results().size(), "Both answering nodes contributed");
        assertEquals(2, page.respondedNodes());
        assertEquals(5, page.clusterSize());
        // The label is the whole point: without it this is indistinguishable from a complete scan.
        assertFalse(page.quorumReached(),
                "2 of 5 is not a majority, and the page must say so");
    }

    @Test
    @DisplayName("ANY_AVAILABLE still reports quorumReached when a majority did answer")
    void anyAvailableReportsQuorumWhenReached() throws Exception {
        final DisCasClient c = clientOver(new PartialTransport(5, "1", "2", "3"));

        final ScanPage page =
                c.scan(all(), null, 100, ScanCoverage.ANY_AVAILABLE).get(20, TimeUnit.SECONDS);

        assertEquals(3, page.respondedNodes());
        assertTrue(page.quorumReached(),
                "Opting into partial results must not understate coverage that was achieved");
    }

    @Test
    @DisplayName("QUORUM completes at the majority without waiting for stragglers")
    void quorumCompletesEarly() throws Exception {
        final DisCasClient c = clientOver(new PartialTransport(5, "1", "2", "3", "4", "5"));

        final ScanPage page = c.scan(all(), null, 100).get(20, TimeUnit.SECONDS);

        // Extra answers past the majority cannot change the guarantee, only the latency, so the
        // scan settles at 3 of 5 rather than collecting all five.
        assertEquals(3, page.respondedNodes());
        assertTrue(page.quorumReached());
    }

    @Test
    @DisplayName("A partial scan settles at once when the rest are unreachable")
    void partialSettlesWithoutWaitingOutTheTimeout() throws Exception {
        final DisCasClient c = clientOver(new PartialTransport(5, "1", "2"));

        final long startNanos = System.nanoTime();
        final ScanPage page =
                c.scan(all(), null, 100, ScanCoverage.ANY_AVAILABLE).get(20, TimeUnit.SECONDS);
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertEquals(2, page.respondedNodes());
        // Waiting out the scan timeout here would exceed the agent's own request budget, so a
        // best-effort listing would 504 before it could return anything.
        assertTrue(elapsedMs < 2_000,
                "A scan whose remaining peers are unreachable must not wait out the timeout,"
                        + " took " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("QUORUM fails fast once a majority has become impossible")
    void quorumFailsFastWhenMajorityImpossible() {
        final DisCasClient c = clientOver(new PartialTransport(5, "1", "2"));

        final long startNanos = System.nanoTime();
        assertThrows(ExecutionException.class,
                () -> c.scan(all(), null, 100).get(20, TimeUnit.SECONDS));
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertTrue(elapsedMs < 2_000,
                "Holding the caller for the full timeout cannot improve an already-decided"
                        + " failure, took " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("A silent (reachable but unanswering) peer is resolved by the timeout")
    void silentPeersFallBackToTheTimeout() throws Exception {
        // Reachable-but-silent is genuinely undecidable early: another answer really might arrive.
        final DisCasClient c = clientOver(new PartialTransport(5, false, "1", "2"));

        final ScanPage page =
                c.scan(all(), null, 100, ScanCoverage.ANY_AVAILABLE).get(30, TimeUnit.SECONDS);

        assertEquals(2, page.respondedNodes());
        assertFalse(page.quorumReached());
    }

    @Test
    @DisplayName("ANY_AVAILABLE still fails when no node answers at all")
    void anyAvailableFailsWhenNobodyAnswers() {
        // An empty page from nobody is not a partial result -- it is indistinguishable from an
        // empty cluster, which is exactly the silent-wrong-answer this whole design avoids.
        final DisCasClient c = clientOver(new PartialTransport(5));

        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> c.scan(all(), null, 100, ScanCoverage.ANY_AVAILABLE).get(20, TimeUnit.SECONDS));

        assertEquals(RequestFailedException.Cause.SCAN_NO_QUORUM,
                assertInstanceOf(RequestFailedException.class, thrown.getCause()).cause());
    }

    @Test
    @DisplayName("The auto-paging overload carries the coverage through")
    void autoPagingHonoursCoverage() throws Exception {
        final DisCasClient c = clientOver(new PartialTransport(5, "1", "2"));

        // Default still fails...
        assertThrows(ExecutionException.class,
                () -> c.scan("").get(20, TimeUnit.SECONDS));
        // ...and the coverage must not be dropped on the way through scanFrom().
        final List<ScanResult> results =
                c.scan("", ScanCoverage.ANY_AVAILABLE).get(20, TimeUnit.SECONDS).results();
        assertEquals(2, results.size());
    }
}
