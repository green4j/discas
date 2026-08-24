/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;


import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientHello;
import io.github.green4j.discas.common.transport.ClientHelloRespCodec;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.auth.InMemoryClientTokenStore;
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import io.github.green4j.discas.common.transport.TransportProtocol;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Client handshake -- CLIENT_HELLO version and token accept/deny")
class ClientTokenAuthTest {

    private static final ClientId WEB = ClientId.of("web-1");
    private static final long FAR_FUTURE = Long.MAX_VALUE;
    private static final int PROTOCOL_VERSION = TransportProtocol.PROTOCOL_VERSION;

    static Stream<Arguments> handshakes() {
        return Stream.of(
                Arguments.of("the correct token", PROTOCOL_VERSION, "s3cret", ClientHelloRespStatus.OK),
                Arguments.of("a wrong token", PROTOCOL_VERSION, "nope", ClientHelloRespStatus.ACCESS_DENIED),
                // An AllowAll-style hello carries no credential; a server that wants one refuses it.
                Arguments.of("no token at all", PROTOCOL_VERSION, "", ClientHelloRespStatus.ACCESS_DENIED),
                // The version is checked before the credential is even looked at.
                Arguments.of("a newer protocol version", PROTOCOL_VERSION + 1, "s3cret",
                        ClientHelloRespStatus.PROTOCOL_MISMATCH));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("handshakes")
    @DisplayName("CLIENT_HELLO is answered on its version first and its credential second")
    void helloIsAnswered(final String name, final int protocolVersion, final String credential,
                         final ClientHelloRespStatus expectedStatus) throws Exception {
        assertStatus(protocolVersion, credential, expectedStatus);
    }

    private static void assertStatus(final int protocolVersion, final String credential,
                                     final ClientHelloRespStatus expectedStatus) throws Exception {
        final EventLoop loop = new EventLoop("token-auth-server");
        final ClientTransportConfig config =
                ClientTransportConfig.builder()
                .maxFrameBytes(1024).maxRxBufferBytes(1 << 16).maxConnections(64).build();
        final TokenClientAuthenticator authenticator = new TokenClientAuthenticator(
                InMemoryClientTokenStore.builder().add(WEB, "s3cret", FAR_FUTURE).build());
        final TcpClientServerTransport server = new TcpClientServerTransport(
                loop, new InetSocketAddress("127.0.0.1", 0),  // bind-first: no probe-to-bind window
                config, 1, authenticator);
        server.registerIngress((clientId, message, sink) -> { });

        loop.start();
        try {
            final FrameCodec frameCodec = new FrameCodec(config.maxFrameBytes());
            final ByteBuffer hello = ClientHello.encode(protocolVersion, WEB, credential);
            final ByteBuffer wire = frameCodec.encode(FrameCodec.TYPE_CLIENT_HELLO, hello);

            try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
                socket.setSoTimeout(5000);
                final byte[] raw = new byte[wire.remaining()];
                wire.get(raw);
                socket.getOutputStream().write(raw);
                socket.getOutputStream().flush();

                final FrameCodec.Frame resp = readFirstFrame(socket.getInputStream(),
                        config.maxFrameBytes());
                assertEquals(FrameCodec.TYPE_CLIENT_HELLO_RESP, resp.type);
                assertEquals(expectedStatus, ClientHelloRespCodec.decode(resp.payload).status);
            }
        } finally {
            server.close();
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(2));
        }
    }

    private static FrameCodec.Frame readFirstFrame(final InputStream in, final int maxFrameBytes)
            throws Exception {
        final FrameCodec codec = new FrameCodec(maxFrameBytes);
        final ByteBuffer rx = ByteBuffer.allocate(maxFrameBytes
                + FrameCodec.FRAME_LENGTH_PREFIX_BYTES);
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            final int b = in.read();
            if (b < 0) {
                break;
            }
            rx.put((byte) b);
            final List<FrameCodec.Frame> frames = codec.drain(rx);
            if (!frames.isEmpty()) {
                return frames.get(0);
            }
        }
        return fail("Timed out waiting for HELLO_RESP frame");
    }

}
