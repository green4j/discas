/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.client.ClientHello;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ClientMessageCodec;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.transport.ClientHelloRespCodec;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.common.identity.ClientId;

import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.ClusterClock;

import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.HeapBufferPool;
import io.github.green4j.discas.common.transport.ConnectionState;
import io.github.green4j.discas.common.transport.TransportProtocol;
import io.github.green4j.discas.common.transport.TransportOverloadedException;
import io.github.green4j.discas.common.transport.TransportSetupException;
import io.github.green4j.discas.common.transport.TransportUnavailableException;
import io.github.green4j.discas.common.transport.TransportErrors;
import io.github.green4j.discas.common.transport.security.ClientChannelSecurity;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A {@link ClientTransport} over TCP: one connection per node the client may address, non-blocking,
 * polled from the client's event loop as an {@link EventLoop.IoDriver}.
 * <p>
 * A connection is opened lazily on the first send to that node and carries no requests until its
 * CLIENT_HELLO has been answered; the response reports the cluster's frozen {@code N} and the
 * coordinator's clock, which is what {@link io.github.green4j.discas.client.ClusterClock} corrects
 * against. A dropped connection is reported to the client at once, so requests riding on it move
 * to another coordinator rather than waiting out a timeout.
 */
public final class TcpClientTransport implements ClientTransport, EventLoop.IoDriver {
    private final EventLoop loop;
    private final ClientTransportConfig config;
    private final Selector selector;
    private final Set<SelectionKey> selectedKeys;
    private final FrameCodec frameCodec;
    private final Map<NodeId, InetSocketAddress> nodeAddresses;
    private final List<NodeId> knownNodes;
    private final Map<SocketChannel, ConnectionState> connections = new HashMap<>();
    private final Map<NodeId, SocketChannel> connectionByPeerId = new HashMap<>();
    private final EventLoop.TimerHandle evictionTimer;
    private final long slowConsumerTimeoutNanos;
    private final HeapBufferPool rxPool;
    private final ClientId clientId;
    private final String token;
    private final ClientSecurityProvider securityProvider;
    private final ClientObserver observer;
    private final Map<SocketChannel, ClientChannelSecurity> channelSecurity = new HashMap<>();
    private final Map<SocketChannel, ByteBuffer> netRxBuffers = new HashMap<>();
    // Outbound application frames (pre-encryption) awaiting handshake + hello completion.
    private final Map<SocketChannel, Deque<ByteBuffer>> pendingAppFrames = new HashMap<>();
    // When each connection's CLIENT_HELLO went out, on the monotonic clock. The response carries
    // the coordinator's wall clock, and an offset derived from it is only as good as the round trip
    // it was measured across -- which is what this remembers. Dropped with the connection.
    private final Map<SocketChannel, Long> helloSentAtNanos = new HashMap<>();
    // The client's clock, handed over at construction; null only for a transport used standalone
    // in a test, which then measures nothing.
    private ClusterClock clusterClock;
    private Consumer<ClientMessage> handler;
    private Consumer<NodeId> connectionLostHandler;
    private volatile boolean closed = false;
    private long estimatedTransportBytes = 0L;
    // Authoritative cluster size N, learned from the first CLIENT_HELLO_RESP; 0 until then.
    // Written and read on the loop thread only (like every other connection-state field here),
    // and connections are established lazily on first send -- so a scan issued before any
    // handshake sees 0 and must evaluate its quorum when responses arrive, not when it is sent.
    private volatile int clusterSize = 0;
    // One warning per transport, not per reconnect -- see onClientHelloRespFrame.
    private boolean clusterSizeMismatchWarned = false;

    public TcpClientTransport(
            final EventLoop loop,
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig config,
            final ClientId clientId,
            final String token) {
        this(loop, nodeAddresses, config, clientId, token, PlaintextClientSecurity.PROVIDER);
    }

    public TcpClientTransport(
            final EventLoop loop,
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig config,
            final ClientId clientId,
            final String token,
            final ClientSecurityProvider securityProvider) {
        this(loop, nodeAddresses, config, clientId, token, securityProvider, ClientObserver.NONE);
    }

    public TcpClientTransport(
            final EventLoop loop,
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig config,
            final ClientId clientId,
            final String token,
            final ClientSecurityProvider securityProvider,
            final ClientObserver observer) {
        this.loop = loop;
        this.observer = observer == null ? ClientObserver.NONE : observer;
        this.config = config;
        this.clientId = clientId;
        this.token = token;
        this.securityProvider = securityProvider;
        this.frameCodec = new FrameCodec(config.maxFrameBytes());
        this.slowConsumerTimeoutNanos =
                Duration.ofMillis(TransportProtocol.SLOW_CONSUMER_TIMEOUT_MS).toNanos();
        this.rxPool = new HeapBufferPool(config.initialRxBytes(), config.maxConnections());
        this.nodeAddresses = new HashMap<>(nodeAddresses);
        this.knownNodes = new ArrayList<>(nodeAddresses.keySet());
        try {
            this.selector = Selector.open();
        } catch (final Exception e) {
            throw new TransportSetupException(TransportSetupException.Fault.INITIALIZATION_FAILED,
                    "Failed to initialize TcpClientTransport", e);
        }
        this.selectedKeys = selector.selectedKeys();
        loop.registerIoDriver(this);
        this.evictionTimer = loop.scheduleRepeat(Duration.ofSeconds(1), this::evictSlowConsumers);
    }

    @Override
    public void send(final NodeId targetNodeId, final ClientMessage message) {
        ensureInLoop();
        if (closed) {
            throw new TransportUnavailableException(TransportUnavailableException.Reason.CLOSED,
                    TransportErrors.ERR_TRANSPORT_CLOSED);
        }
        final SocketChannel channel = ensureConnected(targetNodeId);
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            throw new TransportSetupException(TransportSetupException.Fault.UNKNOWN_TARGET,
                    "Connection state missing for target " + targetNodeId);
        }
        final ByteBuffer encoded = ClientMessageCodec.encode(message);
        final ClientChannelSecurity security = channelSecurity.get(channel);
        final List<FrameCodec.Frame> frames = connection.chunkingEngine.encodePayload(FrameCodec.TYPE_CLIENT_MESSAGE,
                encoded);
        for (int i = 0; i < frames.size(); i++) {
            final FrameCodec.Frame frame = frames.get(i);
            final ByteBuffer frameWire = frameCodec.encode(frame.type, frame.payload);
            if (!security.handshakeFinished() || !connection.helloSent) {
                // Channel security handshake still running (mTLS), or CLIENT_HELLO not yet
                // sent: buffer the plaintext frame and flush it, encrypted, once the
                // handshake completes and the hello has been sent. Frames retain order.
                pendingAppFrames.computeIfAbsent(channel, c -> new ArrayDeque<>()).add(frameWire);
                continue;
            }
            final ByteBuffer wire = security.wrap(frameWire);
            final int wireBytes = wire.remaining();
            if (connection.queuedOutBytes + wireBytes > config.maxQueuedOutBytes()) {
                closeChannel(channel);
                throw new TransportOverloadedException(TransportOverloadedException.Limit.QUEUED_OUT_BYTES,
                        "Backpressure overflow for peer " + targetNodeId);
            }
            connection.enqueue(wire);
            estimatedTransportBytes += wireBytes;
        }
        enableWrite(channel);
    }

    @Override
    public void register(final Consumer<ClientMessage> handler) {
        this.handler = handler;
    }

    @Override
    public void registerConnectionLost(final Consumer<NodeId> handler) {
        this.connectionLostHandler = handler;
    }

    @Override
    public void bindClock(final ClusterClock clock) {
        this.clusterClock = clock;
    }

    /** The monotonic clock the bound {@link ClusterClock} uses, or the system one before binding. */
    private long monotonicNanos() {
        return clusterClock == null ? System.nanoTime() : clusterClock.monotonicNanos();
    }

    @Override
    public List<NodeId> peers() {
        return knownNodes;
    }

    @Override
    public int clusterSize() {
        return clusterSize;
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
        // Never blocking: the loop owns the waiting, because it is also the only thread that can
        // run a queued task or fire a timer. See EventLoop.IoDriver.
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
                if (key.isConnectable()) {
                    onConnect(key);
                }
                if (key.isValid() && key.isReadable()) {
                    onRead((SocketChannel) key.channel());
                }
                if (key.isValid() && key.isWritable()) {
                    onWrite((SocketChannel) key.channel());
                }
                handled++;
            }
        }
        return handled;
    }

    private void onConnect(final SelectionKey key) {
        final SocketChannel channel = (SocketChannel) key.channel();
        try {
            if (channel.finishConnect()) {
                final ConnectionState connection = connections.get(channel);
                if (connection == null) {
                    closeChannel(channel);
                    return;
                }
                connection.markConnected();
                int ops = SelectionKey.OP_READ;
                if (!connection.txQueue.isEmpty()) {
                    ops |= SelectionKey.OP_WRITE;
                }
                key.interestOps(ops);
            }
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    private void onRead(final SocketChannel channel) {
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            closeChannel(channel);
            return;
        }
        if (!connection.connected) {
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
                // Once the security handshake completes, send the deferred CLIENT_HELLO.
                maybeSendClientHello(channel, connection);
                if (!connections.containsKey(channel)) {
                    return;
                }
                if (security.handshakeFinished() && drainAndDispatch(channel, connection)) {
                    return; // channel closed during dispatch
                }
                if (netRx.position() == netBefore) {
                    break; // no forward progress -- leftover net bytes retained by compact()
                }
            }
            netRx.compact();
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    /**
     * Drain complete frames from the application buffer and dispatch them. Returns
     * {@code true} if the channel was closed while dispatching.
     */
    private boolean drainAndDispatch(final SocketChannel channel, final ConnectionState connection) {
        final List<FrameCodec.Frame> frames = frameCodec.drain(connection.rxBuffer);
        for (int i = 0; i < frames.size(); i++) {
            final FrameCodec.Frame frame = frames.get(i);
            if (frame.type == FrameCodec.TYPE_CLIENT_HELLO_RESP) {
                onClientHelloRespFrame(channel, connection, frame.payload);
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
                    estimatedTransportBytes += (connection.chunkingEngine.inboundBytes() - inboundBefore);
                }
            }
            if (payload != null) {
                final ClientMessage message = ClientMessageCodec.decode(payload);
                if (connection.expectedPeerId != null
                        && !message.senderId().equals(connection.expectedPeerId.value())) {
                    closeChannel(channel);
                    return true;
                }
                final Consumer<ClientMessage> localHandler = handler;
                if (localHandler != null) {
                    localHandler.accept(message);
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

    /**
     * Start the channel-security handshake for a freshly-created connection and, if it is
     * already finished (plaintext), send the CLIENT_HELLO immediately.
     */
    private void beginSecurity(final SocketChannel channel, final ConnectionState connection) {
        final ClientChannelSecurity security = channelSecurity.get(channel);
        flushSecurityOutbound(channel, security);
        maybeSendClientHello(channel, connection);
    }

    /** Once the security handshake is finished, send the CLIENT_HELLO exactly once and
     * flush any client messages buffered during the handshake. */
    private void maybeSendClientHello(final SocketChannel channel, final ConnectionState connection) {
        if (connection.helloSent) {
            return;
        }
        final ClientChannelSecurity security = channelSecurity.get(channel);
        if (!security.handshakeFinished()) {
            return;
        }
        connection.helloSent = true;
        // Taken before the frame is built, so everything the client itself spends is inside the
        // measured round trip rather than hidden from it.
        helloSentAtNanos.put(channel, monotonicNanos());
        enqueueClientHelloFrame(connection);
        flushPendingAppFrames(channel, connection);
        enableWrite(channel);
    }

    /** Encrypt and enqueue any application frames buffered during the handshake. */
    private void flushPendingAppFrames(final SocketChannel channel, final ConnectionState connection) {
        final Deque<ByteBuffer> pending = pendingAppFrames.remove(channel);
        if (pending == null || pending.isEmpty()) {
            return;
        }
        final ClientChannelSecurity security = channelSecurity.get(channel);
        ByteBuffer frameWire;
        while ((frameWire = pending.poll()) != null) {
            final ByteBuffer wire = security.wrap(frameWire);
            connection.enqueue(wire);
            estimatedTransportBytes += wire.remaining();
        }
        enableWrite(channel);
    }

    private void onWrite(final SocketChannel channel) {
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            closeChannel(channel);
            return;
        }
        if (!connection.connected) {
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
                disableWrite(channel);
            }
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    private SocketChannel ensureConnected(final NodeId targetNodeId) {
        final SocketChannel existing = connectionByPeerId.get(targetNodeId);
        if (existing != null && existing.isOpen()) {
            return existing;
        }
        final InetSocketAddress address = nodeAddresses.get(targetNodeId);
        if (address == null) {
            throw new IllegalArgumentException("Unknown peer: " + targetNodeId);
        }
        SocketChannel channel = null;
        try {
            if (!canOpenConnection()) {
                throw new TransportOverloadedException(
                        TransportOverloadedException.Limit.CONNECTIONS,
                        "Connection limits exceeded");
            }
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            // Nagle off: a client request is one small frame and the caller is waiting for its
            // reply, so there is never a second write to coalesce it with.
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.connect(address);
            channel.register(selector, SelectionKey.OP_CONNECT);
            final ConnectionState state = new ConnectionState(channel,
                    config.initialRxBytes(), config.maxFrameBytes(),
                    config.chunkPayloadBytes(), config.maxInflightBytes(), false, rxPool);
            state.expectedPeerId = targetNodeId;
            connections.put(channel, state);
            estimatedTransportBytes += state.rxBuffer.capacity();
            channelSecurity.put(channel, securityProvider.forOutbound());
            netRxBuffers.put(channel, ByteBuffer.allocate(config.initialRxBytes()));
            connectionByPeerId.put(targetNodeId, channel);
            // Kick off the channel-security handshake; once finished (immediately for
            // plaintext) the deferred CLIENT_HELLO is enqueued.
            beginSecurity(channel, state);
            return channel;
        } catch (final Exception e) {
            if (channel != null) {
                closeChannel(channel);
            }
            throw new TransportUnavailableException(
                    TransportUnavailableException.Reason.CONNECT_FAILED,
                    TransportErrors.ERR_CONNECT_FAILED_PREFIX + " " + targetNodeId, e);
        }
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
            throw new TransportOverloadedException(
                    TransportOverloadedException.Limit.TRANSPORT_BYTE_BUDGET,
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
        final ConnectionState connection = connections.get(channel);
        if (connection == null || !connection.connected) {
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
            if (connection.connected
                    && (connection.isSlowConsumer(slowConsumerTimeoutNanos)
                            || connection.chunkingEngine.isStalled(slowConsumerTimeoutNanos))) {
                closeChannel(entry.getKey());
            }
        }
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
            selector.close();
        } catch (final Exception ignored) {
        }
        rxPool.close();
    }

    private void closeChannel(final SocketChannel channel) {
        NodeId lostPeer = null;
        try {
            final ConnectionState removed = connections.remove(channel);
            if (removed != null) {
                lostPeer = removed.expectedPeerId;
                estimatedTransportBytes -= removed.estimatedBytes();
                if (estimatedTransportBytes < 0) {
                    observer.transportAccountingUnderflow(estimatedTransportBytes);
                    estimatedTransportBytes = 0;
                }
                rxPool.release(removed.rxBuffer);
            }
            connectionByPeerId.values().removeIf(c -> c == channel);
            final ClientChannelSecurity removedSecurity = channelSecurity.remove(channel);
            if (removedSecurity != null) {
                removedSecurity.close();
            }
            netRxBuffers.remove(channel);
            pendingAppFrames.remove(channel);
            helloSentAtNanos.remove(channel);
            final SelectionKey key = selector.isOpen() ? channel.keyFor(selector) : null;
            if (key != null) {
                key.cancel();
            }
            channel.close();
        } catch (final Exception ignored) {
        }
        // Reported last, once every map is clean, so a handler that re-dispatches immediately
        // finds a transport ready to dial again rather than the remains of the dead connection.
        // Suppressed during close(), which sets `closed` before tearing any channel down: there
        // every pending request is being failed anyway, and reporting would race that teardown.
        if (lostPeer != null && !closed && connectionLostHandler != null) {
            connectionLostHandler.accept(lostPeer);
        }
    }

    private boolean canOpenConnection() {
        if (connections.size() >= config.maxConnections()) {
            return false;
        }
        final long projectedBytes = estimatedTransportBytes + config.initialRxBytes();
        return projectedBytes <= config.derivedMaxTransportBytes();
    }

    private void enqueueClientHelloFrame(final ConnectionState connection) {
        final ByteBuffer helloPayload = ClientHello.encode(
                TransportProtocol.PROTOCOL_VERSION, clientId, token);
        final ByteBuffer helloFrame = channelSecurity.get(connection.channel)
                .wrap(frameCodec.encode(FrameCodec.TYPE_CLIENT_HELLO, helloPayload));
        final int wireBytes = helloFrame.remaining();
        connection.enqueue(helloFrame);
        estimatedTransportBytes += wireBytes;
    }

    private void onClientHelloRespFrame(
            final SocketChannel channel,
            final ConnectionState connection,
            final ByteBuffer payload) {
        final ClientHelloRespCodec.Decoded resp;
        try {
            resp = ClientHelloRespCodec.decode(payload);
        } catch (final RuntimeException malformed) {
            closeChannel(channel);
            return;
        }
        final ClientHelloRespStatus status = resp.status;
        final int remoteClusterSize = resp.clusterSize;
        if (status != ClientHelloRespStatus.OK) {
            observer.serverRejectedHello(connection.expectedPeerId, status);
            closeChannel(channel);
            return;
        }
        if (remoteClusterSize < 1) {
            observer.serverReportedInvalidClusterSize(connection.expectedPeerId, remoteClusterSize);
            closeChannel(channel);
            return;
        }
        // Nodes of one cluster all report the same frozen N. A disagreement means this client
        // is pointed at two different clusters -- refuse the odd one out rather than let it
        // corrupt the scan quorum.
        if (clusterSize != 0 && clusterSize != remoteClusterSize) {
            observer.clusterSizeDisagreement(
                    connection.expectedPeerId, remoteClusterSize, clusterSize);
            closeChannel(channel);
            return;
        }
        clusterSize = remoteClusterSize;
        // Past every refusal above: this connection is usable, which is the event that ends whatever
        // was raised about this node -- including its being unreachable at all.
        observer.serverHandshakeCompleted(connection.expectedPeerId);

        // The coordinator's own clock, and the one moment it can be compared with ours across a
        // round trip we measured. Every reconnect re-measures, which is also how an offset dropped
        // after a step in this client's clock comes back.
        final Long sentAtNanos = helloSentAtNanos.remove(channel);
        if (clusterClock != null && sentAtNanos != null) {
            clusterClock.observeCoordinatorTime(
                    connection.expectedPeerId, resp.coordinatorEpochMs, sentAtNanos);
        }

        // Cross-check the configured node list against the cluster's real size. A client is meant
        // to be given the whole membership: a short list costs failover headroom on every
        // operation (a coordinator that is up but unlisted cannot be tried), and a list shorter
        // than a majority makes scan unable to reach quorum at all.
        //
        // Deliberately a warning, not a rejection. Unlike the peer mesh -- where a size
        // disagreement risks split brain and the connection is refused -- a short client list is
        // safe for get/put/cas/delete, which route to one coordinator that runs the round across
        // all N. Refusing service would turn a configuration wrinkle into an outage, and would
        // break the agent's --nodes-file bootstrap, which starts on a subset by design and reloads
        // to the full membership.
        //
        // Warned once per transport: reconnects re-run this handshake, and a warning per reconnect
        // would bury the signal it is meant to raise.
        if (!clusterSizeMismatchWarned && knownNodes.size() != remoteClusterSize) {
            clusterSizeMismatchWarned = true;
            observer.clusterSizeMismatch(knownNodes.size(), remoteClusterSize);
        }
    }

    private void ensureInLoop() {
        if (!loop.inLoop()) {
            throw new IllegalStateException("TcpClientTransport.send must be called on EventLoop thread");
        }
    }
}
