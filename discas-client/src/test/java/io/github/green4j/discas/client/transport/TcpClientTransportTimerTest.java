/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression guard for the {@code IoDriver.poll(timeoutMs)} contract: when the
 * event loop has a timer due it passes {@code 0}, which must be a <b>non-blocking</b>
 * poll ({@code selectNow}), not {@code select(0)} (which blocks indefinitely in NIO).
 * <p>
 * A {@link TcpClientTransport} registers itself as the loop's IO driver but only opens a
 * {@link java.nio.channels.Selector} -- it connects no channels until a request is sent.
 * With no server and no request, the loop is otherwise idle, so a scheduled timer fires
 * only if {@code poll(0)} returns promptly. Before the fix the loop wedged in
 * {@code select(0)} and the timer never fired (the symptom that hung a contended
 * {@code DisCasClient.lock()} on an idle client loop).
 */
@DisplayName("TcpClientTransport -- scheduled timers fire on an idle loop (poll(0) must be non-blocking)")
class TcpClientTransportTimerTest {

    @Test
    void scheduledTimerFiresOnIdleLoopWithTcpDriver() throws Exception {
        final EventLoop loop = new EventLoop("timer-test");
        final Map<NodeId, InetSocketAddress> addresses =
                Map.of(NodeId.of("1"), new InetSocketAddress("127.0.0.1", 1)); // never connected
        final TcpClientTransport transport =
                new TcpClientTransport(loop, addresses, config(), ClientId.of("timer-client"), null);
        try {
            loop.start();
            final CountDownLatch fired = new CountDownLatch(1);
            loop.schedule(Duration.ofMillis(50), fired::countDown);
            assertTrue(fired.await(3, TimeUnit.SECONDS),
                    "A scheduled timer must fire on an idle loop; poll(0) blocked in select(0) instead");
        } finally {
            transport.close();
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(5));
        }
    }

    private static ClientTransportConfig config() {
        return ClientTransportConfig.defaults();
    }
}
