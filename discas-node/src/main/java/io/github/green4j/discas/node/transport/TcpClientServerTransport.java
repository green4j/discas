/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.HeapBufferPool;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.transport.ConnectionState;
import io.github.green4j.discas.common.transport.TransportOverloadedException;
import io.github.green4j.discas.common.transport.TransportSetupException;
import io.github.green4j.discas.common.transport.TransportProtocol;

import io.github.green4j.discas.common.EventLoop;

import io.github.green4j.discas.common.client.ClientHello;
import io.github.green4j.discas.common.client.ClientIngress;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ClientMessageCodec;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.transport.ClientHelloRespCodec;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.common.client.ResponseSink;
import io.github.green4j.discas.common.client.auth.AllowAllClientAuthenticator;
import io.github.green4j.discas.common.client.auth.ClientAuthenticator;
import io.github.green4j.discas.common.client.auth.ClientCredential;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.transport.security.ClientChannelSecurity;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Non-blocking server that accepts <i>client</i> TCP connections on a
 * dedicated port. Companion to {@link TcpPeerTransport} (peer-to-peer
 * traffic only). Inbound client messages are routed to the {@link ClientIngress}
 * registered through {@link #registerIngress(ClientIngress)}, which receives the
 * connection's trusted {@link ClientId} and a per-connection {@link ResponseSink};
 * the sink serialises responses and enqueues them on that connection's normal tx queue.
 *
 * <p>Handshake: on every accepted connection the (optional) channel-security handshake runs
 * first (mTLS via {@link ClientChannelSecurity}; a no-op for plaintext), then the first
 * application frame must be {@link FrameCodec#TYPE_CLIENT_HELLO}; the server replies with a
 * {@link FrameCodec#TYPE_CLIENT_HELLO_RESP} routed through the (possibly encrypted) tx path.
 * Validation failures map to {@link ClientHelloRespStatus}.
 */
public final class TcpClientServerTransport implements EventLoop.IoDriver, AutoCloseable {
    private final EventLoop loop;
    private final ClientTransportConfig config;
    private final Selector selector;
    private final Set<SelectionKey> selectedKeys;
    private final ServerSocketChannel serverChannel;
    private final FrameCodec frameCodec;
    private final Map<SocketChannel, ConnectionState> connections = new HashMap<>();
    private final EventLoop.TimerHandle evictionTimer;
    private final long slowConsumerTimeoutNanos;
    private final HeapBufferPool rxPool;
    private final ClientAuthenticator authenticator;
    private final ClientSecurityProvider securityProvider;
    /** The node's frozen cluster size {@code N}, reported to clients in CLIENT_HELLO_RESP. */
    private final int clusterSize;
    private final Map<SocketChannel, ClientChannelSecurity> channelSecurity = new HashMap<>();
    private final Map<SocketChannel, ByteBuffer> netRxBuffers = new HashMap<>();
    private ClientIngress ingress;
    private volatile boolean closed = false;
    private long estimatedTransportBytes = 0L;

    public TcpClientServerTransport(
            final EventLoop loop,
            final InetSocketAddress clientBindAddress,
            final ClientTransportConfig config,
            final int clusterSize) {
        this(loop, clientBindAddress, config, clusterSize, AllowAllClientAuthenticator.INSTANCE);
    }

    public TcpClientServerTransport(
            final EventLoop loop,
            final InetSocketAddress clientBindAddress,
            final ClientTransportConfig config,
            final int clusterSize,
            final ClientAuthenticator authenticator) {
        this(loop, clientBindAddress, config, clusterSize, authenticator,
                PlaintextClientSecurity.PROVIDER);
    }

    /**
     * @param clusterSize the node's frozen cluster size {@code N} (the same value the
     *                    {@code Proposer} derives its quorum from). Reported to every client
     *                    in CLIENT_HELLO_RESP so clients can size the scan quorum correctly.
     */
    public TcpClientServerTransport(
            final EventLoop loop,
            final InetSocketAddress clientBindAddress,
            final ClientTransportConfig config,
            final int clusterSize,
            final ClientAuthenticator authenticator,
            final ClientSecurityProvider securityProvider) {
        this(loop, ListenSocket.bind(clientBindAddress), config, clusterSize, authenticator,
                securityProvider);
    }

    /**
     * Takes a socket that is <b>already bound</b>; see the equivalent on {@code TcpPeerTransport}.
     * Ownership transfers: {@link #close()} closes it.
     */
    public TcpClientServerTransport(
            final EventLoop loop,
            final ListenSocket listenSocket,
            final ClientTransportConfig config,
            final int clusterSize,
            final ClientAuthenticator authenticator,
            final ClientSecurityProvider securityProvider) {
        if (clusterSize < 1 || clusterSize > 255) {
            throw new IllegalArgumentException(
                    "clusterSize must be in [1, 255], got " + clusterSize);
        }
        this.loop = loop;
        this.config = config;
        this.clusterSize = clusterSize;
        this.authenticator = authenticator;
        this.securityProvider = securityProvider;
        this.frameCodec = new FrameCodec(config.maxFrameBytes());
        this.slowConsumerTimeoutNanos =
                Duration.ofMillis(TransportProtocol.SLOW_CONSUMER_TIMEOUT_MS).toNanos();
        this.rxPool = new HeapBufferPool(config.initialRxBytes(), config.maxConnections());
        // The socket arrives already bound (see ListenSocket): registering it is all that is
        // left, so there is no window between choosing an address and owning it.
        Selector openedSelector = null;
        try {
            openedSelector = Selector.open();
            listenSocket.channel().register(openedSelector, SelectionKey.OP_ACCEPT);
            this.selector = openedSelector;
            this.selectedKeys = openedSelector.selectedKeys();
            this.serverChannel = listenSocket.channel();
        } catch (final IOException e) {
            listenSocket.close();
            if (openedSelector != null) {
                try {
                    openedSelector.close();
                } catch (final Exception ignored) {
                    // The registration already failed; a failed close adds nothing.
                }
            }
            throw new TransportSetupException(TransportSetupException.Fault.INITIALIZATION_FAILED,
                    "Failed to initialize TcpClientServerTransport", e);
        }

        loop.registerIoDriver(this);
        this.evictionTimer = loop.scheduleRepeat(Duration.ofSeconds(1), this::evictSlowConsumers);
    }

    /**
     * The port this transport actually listens on.
     * <p>
     * Binding happens in the constructor, so this is valid as soon as the object exists. It is
     * what makes {@code port 0} usable: bind an ephemeral port and read back what the OS gave,
     * rather than probing for a free port, closing it, and hoping it is still free at bind time.
     * {@code HttpServer.boundPort()} has always worked this way.
     */
    public int boundPort() {
        try {
            return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
        } catch (final IOException e) {
            throw new TransportSetupException(TransportSetupException.Fault.INITIALIZATION_FAILED,
                    "Cannot read the bound address of TcpClientServerTransport", e);
        }
    }

    /**
     * Wire an ingress that will be invoked on the EventLoop thread for
     * each decoded client message together with a sink bound to the
     * originating connection.
     */
    public void registerIngress(final ClientIngress ingress) {
        this.ingress = ingress;
    }

    @Override
    public int pollNow() throws Exception {
        if (closed || !selector.isOpen()) {
            return 0;
        }
        try {
            return pollSelector();
        } catch (final ClosedSelectorException e) {
            // closed concurrently by close() on another thread -- stop quietly.
            return 0;
        }
    }

    private int pollSelector() throws Exception {
        // Never blocking: this node runs two drivers on one loop, and a blocking wait in either
        // one delays the other's ready data. See EventLoop.IoDriver.
        selector.selectNow();
        int handled = 0;
        if (!selectedKeys.isEmpty()) {
            final Iterator<SelectionKey> iterator = selectedKeys.iterator();
            while (iterator.hasNext()) {
                final SelectionKey key = iterator.next();
                iterator.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    onAccept();
                } else {
                    if (key.isValid() && key.isReadable()) {
                        onRead((SocketChannel) key.channel());
                    }
                    if (key.isValid() && key.isWritable()) {
                        onWrite((SocketChannel) key.channel());
                    }
                }
                handled++;
            }
        }
        return handled;
    }


    @Override
    public void close() {
        if (loop.inLoop()) {
            closeInternal();
        } else if (loop.isRunning()) {
            loop.execute(this::closeInternal);
        }
        if (!closed) {
            closeInternal();
        }
    }

    private void onAccept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            if (channel == null) {
                return;
            }
            // Both ends: Nagle on either side is enough to stall a request/response exchange.
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            if (!canOpenConnection()) {
                channel.close();
                return;
            }
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ);
            final ConnectionState state = new ConnectionState(channel,
                    config.initialRxBytes(), config.maxFrameBytes(),
                    config.chunkPayloadBytes(), config.maxInflightBytes(), true, rxPool);
            connections.put(channel, state);
            estimatedTransportBytes += state.rxBuffer.capacity();
            channelSecurity.put(channel, securityProvider.forInbound());
            netRxBuffers.put(channel, ByteBuffer.allocate(config.initialRxBytes()));
        } catch (final Exception e) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (final Exception ignored) {
                }
            }
        }
    }

    private void onRead(final SocketChannel channel) {
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            closeChannel(channel);
            return;
        }
        final ClientChannelSecurity security = channelSecurity.get(channel);
        final ByteBuffer netRx = netRxBuffers.get(channel);
        try {
            final int read = channel.read(netRx);
            if (read < 0) {
                closeChannel(channel);
                return;
            }
            if (read == 0) {
                return;
            }
            netRx.flip();
            while (netRx.hasRemaining()) {
                ensureRxCapacity(connection);
                final int netBefore = netRx.position();
                // Network -> application bytes (identity for plaintext, decrypt for TLS).
                security.unwrap(netRx, connection.rxBuffer);
                flushSecurityOutbound(channel, security);
                if (!connections.containsKey(channel)) {
                    return;
                }
                if (security.handshakeFinished() && drainAndDispatch(channel, connection)) {
                    return; // channel closed during dispatch
                }
                if (netRx.position() == netBefore) {
                    // No forward progress (app buffer full, or the handshake needs more
                    // network bytes) -- stop to avoid a busy loop; leftover net bytes are
                    // retained by compact() below.
                    break;
                }
            }
            netRx.compact();
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    /**
     * Drain complete frames from the connection's application buffer and dispatch them.
     * Returns {@code true} if the channel was closed while dispatching.
     */
    private boolean drainAndDispatch(final SocketChannel channel, final ConnectionState connection) {
        final List<FrameCodec.Frame> frames = frameCodec.drain(connection.rxBuffer);
        for (int i = 0; i < frames.size(); i++) {
            final FrameCodec.Frame frame = frames.get(i);
            if (frame.type == FrameCodec.TYPE_CLIENT_HELLO) {
                onClientHelloFrame(channel, connection, frame.payload);
                if (!connections.containsKey(channel)) {
                    return true;
                }
                continue;
            }
            final ByteBuffer payload;
            if (frame.type == FrameCodec.TYPE_CLIENT_MESSAGE) {
                payload = frame.payload;
            } else {
                final int inboundBefore = connection.chunkingEngine.inboundBytes();
                try {
                    payload = connection.chunkingEngine.onFrame(frame);
                } finally {
                    final int delta = connection.chunkingEngine.inboundBytes() - inboundBefore;
                    if (delta != 0) {
                        estimatedTransportBytes += delta;
                    }
                }
            }
            if (payload != null) {
                if (!connection.helloReceived) {
                    // Every application frame must come from a client that completed
                    // CLIENT_HELLO.
                    closeChannel(channel);
                    return true;
                }
                final ClientMessage message = ClientMessageCodec.decode(payload);
                final ClientIngress localIngress = ingress;
                if (localIngress != null) {
                    localIngress.accept(connection.authenticatedClientId, message, sinkFor(channel));
                }
            }
        }
        return false;
    }

    /** Enqueue any network bytes the security handshake needs to send. */
    private void flushSecurityOutbound(final SocketChannel channel, final ClientChannelSecurity security) {
        ByteBuffer out = security.pendingOutbound();
        if (out == null) {
            return;
        }
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            return;
        }
        do {
            final int bytes = out.remaining();
            connection.enqueue(out);
            estimatedTransportBytes += bytes;
            out = security.pendingOutbound();
        } while (out != null);
        enableWrite(channel);
    }

    private void onWrite(final SocketChannel channel) {
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            closeChannel(channel);
            return;
        }
        try {
            while (!connection.txQueue.isEmpty()) {
                final ByteBuffer head = connection.txQueue.peek();
                final int written = channel.write(head);
                if (written < 0) {
                    closeChannel(channel);
                    return;
                }
                connection.queuedOutBytes -= written;
                estimatedTransportBytes -= written;
                if (head.hasRemaining()) {
                    break;
                }
                connection.txQueue.poll();
            }
            connection.onWriteProgress();
            if (connection.txQueue.isEmpty()) {
                if (connection.closeAfterFlush) {
                    // A rejection CLIENT_HELLO_RESP has drained -- drop the connection.
                    closeChannel(channel);
                    return;
                }
                disableWrite(channel);
            }
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    private void onClientHelloFrame(
            final SocketChannel channel,
            final ConnectionState connection,
            final ByteBuffer payload) {
        // Read the version first (its position is stable across protocol revisions) so a
        // version mismatch is reported cleanly before decoding the rest of the hello.
        final int remoteVersion;
        try {
            remoteVersion = ClientHello.peekVersion(payload);
        } catch (final RuntimeException e) {
            closeChannel(channel);
            return;
        }
        if (remoteVersion != TransportProtocol.PROTOCOL_VERSION) {
            respondToClientAndClose(channel, ClientHelloRespStatus.PROTOCOL_MISMATCH);
            return;
        }
        final ClientHello.Decoded hello;
        try {
            hello = ClientHello.decode(payload);
        } catch (final RuntimeException e) {
            closeChannel(channel);
            return;
        }
        if (!withinCapacityBudget(connection)) {
            respondToClientAndClose(channel, ClientHelloRespStatus.SERVER_BUSY);
            return;
        }

        final ClientChannelSecurity security = channelSecurity.get(channel);
        final ClientId certId = security != null ? security.peerClientId() : null;
        final ClientId authenticatedId;
        if (certId != null) {
            // mTLS: the certificate CN is the authoritative identity; the hello's claimed
            // id must match it (anti-impersonation, cf. the peer SAN-vs-hello cross-check).
            if (!certId.equals(hello.clientId)) {
                respondToClientAndClose(channel, ClientHelloRespStatus.ACCESS_DENIED);
                return;
            }
            authenticatedId = certId;
        } else {
            // No client certificate: authenticate the hello (AllowAll / Token).
            final ClientCredential credential =
                    authenticator.authenticate(hello.clientId, hello.credential);
            if (!credential.authenticated()) {
                respondToClientAndClose(channel, ClientHelloRespStatus.ACCESS_DENIED);
                return;
            }
            authenticatedId = credential.clientId();
        }

        enqueueClientHelloResp(channel, connection, ClientHelloRespStatus.OK);
        if (!connections.containsKey(channel)) {
            return;
        }
        connection.authenticatedClientId = authenticatedId;
        connection.helloReceived = true;
    }

    /**
     * Serialise a CLIENT_HELLO_RESP, wrap it through the connection's channel security
     * (identity for plaintext, encrypt for TLS), and enqueue it on the tx path. Routing it
     * through the tx queue (rather than a direct socket write) keeps the TLS record stream
     * intact.
     */
    private void enqueueClientHelloResp(final SocketChannel channel, final ConnectionState connection,
                                        final ClientHelloRespStatus status) {
        // The clock reading is taken here rather than anywhere earlier: everything between it and
        // the send lands in the client's offset as error, and this is the last point the node
        // controls before the bytes are queued.
        final ByteBuffer respPayload = ClientHelloRespCodec.encode(
                status, clusterSize, System.currentTimeMillis());
        final ClientChannelSecurity security = channelSecurity.get(channel);
        final ByteBuffer wire = security.wrap(
                frameCodec.encode(FrameCodec.TYPE_CLIENT_HELLO_RESP, respPayload));
        final int wireBytes = wire.remaining();
        connection.enqueue(wire);
        estimatedTransportBytes += wireBytes;
        enableWrite(channel);
    }

    private void respondToClientAndClose(final SocketChannel channel,
                                         final ClientHelloRespStatus status) {
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            closeChannel(channel);
            return;
        }
        // Send the rejection through the tx path, then close once it drains.
        enqueueClientHelloResp(channel, connection, status);
        connection.closeAfterFlush = true;
    }

    private ResponseSink sinkFor(final SocketChannel channel) {
        return response -> {
            if (loop.inLoop()) {
                sendResponse(channel, response);
            } else {
                loop.execute(() -> sendResponse(channel, response));
            }
        };
    }

    private void sendResponse(final SocketChannel channel, final ClientMessage response) {
        if (closed) {
            return;
        }
        final ConnectionState connection = connections.get(channel);
        if (connection == null || !channel.isOpen()) {
            return;
        }
        try {
            final ClientChannelSecurity security = channelSecurity.get(channel);
            final ByteBuffer encoded = ClientMessageCodec.encode(response);
            final List<FrameCodec.Frame> frames = connection.chunkingEngine.encodePayload(
                    FrameCodec.TYPE_CLIENT_MESSAGE, encoded);
            for (int i = 0; i < frames.size(); i++) {
                final FrameCodec.Frame frame = frames.get(i);
                final ByteBuffer wire = security.wrap(frameCodec.encode(frame.type, frame.payload));
                final int wireBytes = wire.remaining();
                if (connection.queuedOutBytes + wireBytes > config.maxQueuedOutBytes()) {
                    closeChannel(channel);
                    return;
                }
                connection.enqueue(wire);
                estimatedTransportBytes += wireBytes;
            }
            enableWrite(channel);
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    private boolean withinCapacityBudget(final ConnectionState connection) {
        // Re-check after accept(): per-conn budget plus headroom for an
        // expected outbound response. Cheap, on the EventLoop thread.
        final long projected = estimatedTransportBytes + config.maxQueuedOutBytes() / 2;
        return projected <= config.derivedMaxTransportBytes();
    }

    private void ensureRxCapacity(final ConnectionState connection) {
        if (connection.rxBuffer.hasRemaining()) {
            return;
        }
        final int current = connection.rxBuffer.capacity();
        final int next = Math.min(current * 2, config.maxRxBufferBytes());
        if (next <= current) {
            throw new TransportOverloadedException(TransportOverloadedException.Limit.RX_BUFFER_BYTES,
                    "RX buffer limit reached");
        }
        final long delta = next - current;
        if (estimatedTransportBytes + delta > config.derivedMaxTransportBytes()) {
            throw new TransportOverloadedException(TransportOverloadedException.Limit.TRANSPORT_BYTE_BUDGET,
                    "Transport byte budget exceeded");
        }
        final ByteBuffer oldBuffer = connection.rxBuffer;
        final ByteBuffer grown = ByteBuffer.allocate(next);
        oldBuffer.flip();
        grown.put(oldBuffer);
        connection.rxBuffer = grown;
        rxPool.release(oldBuffer);
        estimatedTransportBytes += delta;
    }

    private void enableWrite(final SocketChannel channel) {
        final SelectionKey key = channel.keyFor(selector);
        if (key == null || !key.isValid()) {
            return;
        }
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
    }

    private void disableWrite(final SocketChannel channel) {
        final SelectionKey key = channel.keyFor(selector);
        if (key == null || !key.isValid()) {
            return;
        }
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    private void evictSlowConsumers() {
        for (final Map.Entry<SocketChannel, ConnectionState> entry : new ArrayList<>(connections.entrySet())) {
            final ConnectionState connection = entry.getValue();
            if (connection.isSlowConsumer(slowConsumerTimeoutNanos)
                    || connection.chunkingEngine.isStalled(slowConsumerTimeoutNanos)) {
                closeChannel(entry.getKey());
            }
        }
    }

    private boolean canOpenConnection() {
        if (connections.size() >= config.maxConnections()) {
            return false;
        }
        final long projectedBytes = estimatedTransportBytes + config.initialRxBytes();
        return projectedBytes <= config.derivedMaxTransportBytes();
    }

    private void closeInternal() {
        if (closed) {
            return;
        }
        closed = true;
        evictionTimer.cancel();
        for (final SocketChannel channel : new ArrayList<>(connections.keySet())) {
            closeChannel(channel);
        }
        try {
            final SelectionKey key = serverChannel.keyFor(selector);
            if (key != null) {
                key.cancel();
            }
            serverChannel.close();
        } catch (final Exception ignored) {
        }
        try {
            selector.close();
        } catch (final Exception ignored) {
        }
        rxPool.close();
    }

    private void closeChannel(final SocketChannel channel) {
        try {
            final ConnectionState removed = connections.remove(channel);
            if (removed != null) {
                estimatedTransportBytes -= removed.estimatedBytes();
                if (estimatedTransportBytes < 0) {
                    estimatedTransportBytes = 0;
                }
                rxPool.release(removed.rxBuffer);
            }
            final ClientChannelSecurity removedSecurity = channelSecurity.remove(channel);
            if (removedSecurity != null) {
                removedSecurity.close();
            }
            netRxBuffers.remove(channel);
            final SelectionKey key = selector.isOpen() ? channel.keyFor(selector) : null;
            if (key != null) {
                key.cancel();
            }
            channel.close();
        } catch (final Exception ignored) {
        }
    }
}
