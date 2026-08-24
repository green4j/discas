/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code close()} and {@code start()} are idempotent across the lifecycle-bearing types.
 * <p>
 * {@link DisCasNode#close()} in particular had no guard: a second call re-ran the whole shutdown
 * sequence and then blocked for the full pre-shutdown timeout on a latch the dead loop could
 * never count down, before throwing. Anything holding a node in a try-with-resources <em>and</em>
 * closing it explicitly hit that.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("Lifecycle -- close() and start() are idempotent")
class LifecycleIdempotencyTest {

    private static final NodeId N1 = NodeId.of("1");
    private static final ClusterId CLUSTER = ClusterId.of("lifecycle-cluster");

    @Test
    @DisplayName("DisCasNode.close() twice returns promptly and does not re-run shutdown")
    void nodeCloseIsIdempotent() {
        final EventLoop loop = new EventLoop("idempotent-close-node");
        final AtomicInteger closings = new AtomicInteger();
        final DisCasNode node = new DisCasNode(new NodeConfig(N1, CLUSTER, 1), new InMemoryWal(), loop,
                new InProcessPeerTransport(N1, 1, loop, InMemoryMembers.ofNodes(List.of(N1))),
                new NodeObserver() {
                    @Override
                    public void nodeState(final NodeState from, final NodeState to,
                                          final String detail, final Throwable cause) {
                        if (to == NodeState.CLOSING) {
                            closings.incrementAndGet();
                        }
                    }
                });
        node.start();

        node.close();
        final long startedAt = System.nanoTime();
        assertDoesNotThrow(node::close);
        final Duration secondClose = Duration.ofNanos(System.nanoTime() - startedAt);

        assertEquals(1, closings.get(), "Shutdown must run exactly once");
        assertTrue(secondClose.toSeconds() < 2,
                "The second close() must return promptly, took " + secondClose);
    }

    @Test
    @DisplayName("DisCasNode.start() twice does not start the loop thread twice")
    void nodeStartIsIdempotent() {
        final EventLoop loop = new EventLoop("idempotent-start-node");
        final DisCasNode node = new DisCasNode(new NodeConfig(N1, CLUSTER, 1), new InMemoryWal(), loop,
                new InProcessPeerTransport(N1, 1, loop, InMemoryMembers.ofNodes(List.of(N1))),
                NodeObserver.NONE);
        try {
            node.start();
            // Without the guard this reaches Thread.start() a second time and throws
            // IllegalThreadStateException.
            assertDoesNotThrow(node::start);
        } finally {
            node.close();
        }
    }

    @Test
    @DisplayName("EventLoop.start() twice is a no-op")
    void eventLoopStartIsIdempotent() {
        final EventLoop loop = new EventLoop("idempotent-start-loop");
        try {
            loop.start();
            assertDoesNotThrow(loop::start);
        } finally {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("The in-process transports close idempotently")
    void inProcessTransportsCloseIdempotently() {
        final EventLoop loop = new EventLoop("idempotent-close-transport");
        loop.start();
        try {
            final InProcessPeerTransport peerTransport =
                    new InProcessPeerTransport(N1, 1, loop, InMemoryMembers.ofNodes(List.of(N1)));
            peerTransport.close();
            assertDoesNotThrow(peerTransport::close);

            final InProcessClientTransport clientTransport =
                    new InProcessClientTransport(loop, List.of(N1), ClientId.of("c1"));
            clientTransport.close();
            assertDoesNotThrow(clientTransport::close);
        } finally {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(5));
        }
    }
}
