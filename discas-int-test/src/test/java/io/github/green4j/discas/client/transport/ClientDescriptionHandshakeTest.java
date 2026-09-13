/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientHello;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ClientMessageCodec;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientDescription;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClientIdentity;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.TransportProtocol;
import io.github.green4j.discas.node.audit.AuditRecorder;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The description a client presents at CLIENT_HELLO reaches the node bound to the connection, and
 * reaches the audit with it.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("CLIENT_HELLO -- the description is bound to the connection")
class ClientDescriptionHandshakeTest {

    private static final ClientId WEB = ClientId.of("web-1");
    private static final ClientDescription DESCRIPTION =
            ClientDescription.of("Jenkins euc1-blue, piplex");

    @Test
    void theIngressAndTheAuditSeeTheDescription() throws Exception {
        final EventLoop loop = new EventLoop("description-handshake");
        final ClientTransportConfig config = ClientTransportConfig.builder()
                .maxFrameBytes(8192).maxRxBufferBytes(1 << 16).maxConnections(8).build();
        final TcpClientServerTransport server = new TcpClientServerTransport(
                loop, new InetSocketAddress("127.0.0.1", 0), config, 1);

        final CountDownLatch received = new CountDownLatch(1);
        final List<ClientIdentity> viaIngress = new CopyOnWriteArrayList<>();
        final List<ClientIdentity> viaAudit = new CopyOnWriteArrayList<>();
        server.registerIngress((identity, message, sink) -> {
            viaIngress.add(identity);
            received.countDown();
        });
        server.registerAudit(new AuditRecorder() {
            @Override
            public void sessionOpened(final ClientIdentity identity) {
                viaAudit.add(identity);
            }
        });

        loop.start();
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5000);
            final FrameCodec frameCodec = new FrameCodec(config.maxFrameBytes());
            write(socket, frameCodec.encode(FrameCodec.TYPE_CLIENT_HELLO,
                    ClientHello.encode(TransportProtocol.PROTOCOL_VERSION, WEB, null, DESCRIPTION)));
            write(socket, frameCodec.encode(FrameCodec.TYPE_CLIENT_MESSAGE,
                    ClientMessageCodec.encode(new ClientMessage.ClientGetReq(
                            WEB.value(), 1L, ByteBuffer.wrap(new byte[] {1})))));

            assertTrue(received.await(10, TimeUnit.SECONDS),
                    "no client message reached the ingress; audit saw " + viaAudit);
        } finally {
            server.close();
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(2));
        }

        assertEquals(WEB, viaIngress.get(0).id());
        assertEquals(DESCRIPTION, viaIngress.get(0).description());
        assertEquals("web-1 (Jenkins euc1-blue, piplex)", viaIngress.get(0).label());
        assertEquals(1, viaAudit.size(), "the accepted hello was not recorded");
        assertEquals(DESCRIPTION, viaAudit.get(0).description());
    }

    private static void write(final Socket socket, final ByteBuffer frame) throws Exception {
        final byte[] raw = new byte[frame.remaining()];
        frame.get(raw);
        socket.getOutputStream().write(raw);
        socket.getOutputStream().flush();
    }
}
