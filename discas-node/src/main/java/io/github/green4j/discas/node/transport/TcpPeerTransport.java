/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.HeapBufferPool;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.transport.ConnectionState;
import io.github.green4j.discas.common.transport.TransportProtocol;
import io.github.green4j.discas.common.transport.TransportErrors;
import io.github.green4j.discas.common.transport.TransportOverloadedException;
import io.github.green4j.discas.common.transport.TransportSetupException;
import io.github.green4j.discas.common.transport.TransportUnavailableException;
import io.github.green4j.discas.common.transport.security.PeerChannelSecurity;
import io.github.green4j.discas.common.transport.security.PeerCredential;
import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.identity.ClusterId;

import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.PeerMessage;
import io.github.green4j.discas.node.PeerMessageCodec;
import io.github.green4j.discas.node.membership.Members;
import io.github.green4j.discas.node.membership.MembersSnapshot;
import io.github.green4j.discas.node.membership.TcpMemberInfo;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The peer mesh over TCP: one connection per member, non-blocking, polled from the node's event
 * loop as an {@link EventLoop.IoDriver}.
 * <p>
 * A single dialer per pair -- the lower node id dials -- so two members never end up with two
 * connections. A connection carries no protocol traffic until its PEER_HELLO handshake has
 * established the cluster id, both identities, the frozen {@code N}, and each side's promise
 * ceiling claim; under mTLS the TLS handshake completes first. Every rejection is a
 * {@link PeerHelloRespStatus}.
 */
public final class TcpPeerTransport implements PeerTransport, EventLoop.IoDriver {
    private final NodeId nodeId;
    private final ClusterId clusterId;
    private final Members<TcpMemberInfo> members;
    // Cluster size (quorum basis) frozen at startup -- the Proposer's quorum is fixed
    // to this, so a live connectivity reload must never change it (see onMembersChanged).
    private final int clusterSize;
    /**
     * This node's incarnation -- which run of its durable state this is. Sent in both directions of
     * the handshake so either side can refuse a peer whose state was replaced under an identity
     * that is meant to outlive it.
     */
    private final IncarnationId incarnation;
    /**
     * What each member has proved about its own storage, and the guard that reads it: storage may
     * never come back holding less than it left with, checked rather than assumed.
     */
    private final PromiseCeilingHistory promiseCeilings = new PromiseCeilingHistory();
    /**
     * This node's forced promise ceiling, read at every handshake rather than captured once,
     * because it rises. Reports {@link PeerTransport#PROMISE_CEILING_REPLAYING} until the node
     * binds it and its replay has run: this transport is constructed before the node that owns the
     * store, and dials on a timer from the moment the loop starts, so the two genuinely overlap.
     */
    private LongSupplier promiseCeilingSource = () -> PROMISE_CEILING_REPLAYING;
    /**
     * Incarnations this node has handshaked with, for this process lifetime only. Not persisted: a
     * durable member-to-incarnation map would be replicated membership state, and in-memory already
     * covers the case that matters -- one node wiped while its peers stayed up, which is what a
     * restart loop under an orchestrator looks like. Loop-confined, like every other field here.
     */
    private final Map<NodeId, IncarnationId> knownIncarnations = new HashMap<>();
    /**
     * The reverse index, for the opposite operator error: the same storage appearing under two
     * member identities, which is what copying a data directory produces. The clone carries the
     * original's promises and its reserved ballot range, so it is not an empty node being seeded --
     * it is a second node asserting another's commitments as its own.
     */
    private final Map<IncarnationId, NodeId> incarnationOwners = new HashMap<>();
    private final EventLoop loop;
    private final TcpTransportConfig config;
    private final Selector selector;
    private final Set<SelectionKey> selectedKeys;
    private final ServerSocketChannel serverChannel;
    private final FrameCodec frameCodec;
    // Peer addresses held unresolved (host+port); resolved fresh at each dial.
    private final Map<NodeId, TcpMemberInfo> peerAddresses;
    private final List<NodeId> peers;
    // Last members snapshot reconciled onto the loop; used to diff on reload.
    private MembersSnapshot<TcpMemberInfo> reconciledMembers;
    private final Map<SocketChannel, ConnectionState> connections = new HashMap<>();
    private final Map<NodeId, SocketChannel> outboundByPeerId = new HashMap<>();
    private final Map<NodeId, ReconnectState> reconnectStates = new HashMap<>();
    private final PeerSecurityProvider securityProvider;
    private final NodeObserver observer;
    private final Map<SocketChannel, PeerChannelSecurity> channelSecurity = new HashMap<>();
    private final Map<SocketChannel, ByteBuffer> netRxBuffers = new HashMap<>();
    // Outbound application frames (pre-encryption) awaiting handshake completion.
    private final Map<SocketChannel, Deque<ByteBuffer>> pendingAppFrames = new HashMap<>();

    // Reconnect base/cap are TcpTransportConfig tunables; only the dimensionless shaping is fixed.
    private static final double BACKOFF_JITTER = 0.2;
    private static final int BACKOFF_MAX_SHIFT = 16;
    private static final long MAX_HELLO_SKEW_MS = Duration.ofMinutes(5).toMillis();

    private static final String ERR_IN_RECONNECT_BACKOFF = "in reconnect backoff";
    private final EventLoop.TimerHandle evictionTimer;
    private final long slowConsumerTimeoutNanos;
    private final HeapBufferPool rxPool;
    private Consumer<PeerMessage> handler;
    private volatile boolean closed = false;
    private long estimatedTransportBytes = 0L;

    public TcpPeerTransport(
            final NodeId nodeId,
            final ClusterId clusterId,
            final int clusterSize,
            final EventLoop loop,
            final InetSocketAddress peerBindAddress,
            final Members<TcpMemberInfo> members,
            final PeerSecurityProvider securityProvider,
            final TcpTransportConfig config) {
        this(nodeId, clusterId, clusterSize, loop, peerBindAddress, members,
                securityProvider, config, NodeObserver.NONE);
    }

    public TcpPeerTransport(
            final NodeId nodeId,
            final ClusterId clusterId,
            final int clusterSize,
            final EventLoop loop,
            final InetSocketAddress peerBindAddress,
            final Members<TcpMemberInfo> members,
            final PeerSecurityProvider securityProvider,
            final TcpTransportConfig config,
            final NodeObserver observer) {
        this(nodeId, clusterId, clusterSize, loop, ListenSocket.bind(peerBindAddress), members,
                securityProvider, config, observer, IncarnationId.generate());
    }

    /**
     * Takes a socket that is <b>already bound</b>, so its address was known before this transport
     * existed. That is what makes a peer mesh on ephemeral ports possible: bind every node's
     * listener first, build the members map from the addresses they actually got, and only then
     * construct the nodes -- which each validate that map. Binding inside this constructor forces
     * the opposite order, and with it a guessed-in-advance port.
     * <p>
     * Ownership transfers: {@link #close()} closes the socket.
     */
    public TcpPeerTransport(
            final NodeId nodeId,
            final ClusterId clusterId,
            final int clusterSize,
            final EventLoop loop,
            final ListenSocket listenSocket,
            final Members<TcpMemberInfo> members,
            final PeerSecurityProvider securityProvider,
            final TcpTransportConfig config,
            final NodeObserver observer,
            final IncarnationId incarnation) {
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        this.observer = observer == null ? NodeObserver.NONE : observer;
        // N is an explicit, frozen value (not derived from the list); it is the
        // Proposer's quorum basis and is carried as an unsigned byte in the handshake.
        if (clusterSize < 1 || clusterSize > 255) {
            throw new IllegalArgumentException("clusterSize must be in [1, 255], got " + clusterSize);
        }
        this.clusterSize = clusterSize;
        this.incarnation = incarnation;
        this.members = members;
        this.reconciledMembers = members.snapshot();
        // The member list must define exactly N members and include this node.
        validateMembership(reconciledMembers, clusterSize, nodeId);
        this.securityProvider = securityProvider;
        this.loop = loop;
        this.config = config;
        this.frameCodec = new FrameCodec(config.maxFrameBytes());
        this.slowConsumerTimeoutNanos =
                Duration.ofMillis(TransportProtocol.SLOW_CONSUMER_TIMEOUT_MS).toNanos();
        this.rxPool = new HeapBufferPool(config.initialRxBytes(), config.maxConnections());
        this.peerAddresses = new HashMap<>();
        this.peers = new ArrayList<>();
        for (final Map.Entry<NodeId, TcpMemberInfo> e : reconciledMembers.byId().entrySet()) {
            if (!e.getKey().equals(nodeId)) {
                this.peerAddresses.put(e.getKey(), e.getValue());
                this.peers.add(e.getKey());
            }
        }
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
                    "Failed to initialize TcpPeerTransport", e);
        }

        loop.registerIoDriver(this);
        this.evictionTimer = loop.scheduleRepeat(Duration.ofSeconds(1), this::periodicMaintenance);
        // React to membership changes from a reloadable Members list (marshaled
        // onto the loop): drop removed peers, make added peers connectable, and
        // apply re-addressed peers.
        members.addListener(snapshot ->
                loop.execute(() -> onMembersChanged(snapshot)));
    }

    /** @return null if {@code snap} defines exactly N members incl. self, else the reason. */
    private static String membershipMismatch(final MembersSnapshot<TcpMemberInfo> snap, final int clusterSize,
                                             final NodeId self) {
        if (snap.ids().size() != clusterSize) {
            return "member list must define exactly N=" + clusterSize + " nodes, found "
                    + snap.ids().size();
        }
        if (!snap.contains(self)) {
            return "member list must include this node (" + self.value() + ")";
        }
        return null;
    }

    private static void validateMembership(final MembersSnapshot<TcpMemberInfo> snap, final int clusterSize,
                                           final NodeId self) {
        final String reason = membershipMismatch(snap, clusterSize, self);
        if (reason != null) {
            throw new IllegalArgumentException(reason);
        }
    }

    /**
     * Reconcile a reloaded members snapshot against the one last applied: drop
     * removed peers, add new ones, and update re-addressed peers. Runs on the loop.
     */
    private void onMembersChanged(final MembersSnapshot<TcpMemberInfo> newSnapshot) {
        if (closed) {
            return;
        }
        // Reconfiguration guard: N is frozen at startup and cannot change live. The
        // reloaded list must still define exactly N members and include this node;
        // otherwise it is an operator mistake (or the restart-driven part of a real
        // reconfiguration) -- warn and ignore the whole update, keeping the current list.
        final String reason = membershipMismatch(newSnapshot, clusterSize, nodeId);
        if (reason != null) {
            observer.membersReloadRejected(reason);
            return;
        }
        final MembersSnapshot<TcpMemberInfo> previous = reconciledMembers;
        reconciledMembers = newSnapshot;
        // Reported before the reconciliation below rather than after it: this is the event that says
        // the file on disk and the list in force agree again, and it is what clears a rejection.
        observer.membersReloadAccepted(newSnapshot.ids().size());

        // Removed: present before, absent now.
        for (final NodeId peer : previous.ids()) {
            if (peer.equals(nodeId) || newSnapshot.contains(peer)) {
                continue;
            }
            peerAddresses.remove(peer);
            peers.remove(peer);
            promiseCeilings.forget(peer);
            closeConnectionsTo(peer);
        }
        // Added and re-addressed.
        for (final Map.Entry<NodeId, TcpMemberInfo> e : newSnapshot.byId().entrySet()) {
            final NodeId peer = e.getKey();
            if (peer.equals(nodeId)) {
                continue;
            }
            final TcpMemberInfo member = e.getValue();
            final TcpMemberInfo existing = peerAddresses.get(peer);
            if (existing == null) {
                // Added: connectable from now on.
                peerAddresses.put(peer, member);
                if (!peers.contains(peer)) {
                    peers.add(peer);
                }
            } else if (!existing.equals(member)) {
                // Re-addressed: store the new unresolved address. With forceReconnect
                // we drop the live connection to reconnect at the new address now;
                // otherwise the healthy connection stays and the new address is used
                // on the next reconnect.
                peerAddresses.put(peer, member);
                if (config.forceReconnect()) {
                    closeConnectionsTo(peer);
                }
            }
        }
    }

    /** Close and forget any connection to/from a peer, and reset its reconnect state. */
    private void closeConnectionsTo(final NodeId peer) {
        reconnectStates.remove(peer);
        final SocketChannel primary = outboundByPeerId.remove(peer);
        if (primary != null) {
            closeChannel(primary);
        }
        for (final Map.Entry<SocketChannel, ConnectionState> e
                : new ArrayList<>(connections.entrySet())) {
            final ConnectionState cs = e.getValue();
            if (peer.equals(cs.authenticatedPeerId) || peer.equals(cs.expectedPeerId)) {
                closeChannel(e.getKey());
            }
        }
    }

    @Override
    public void send(final NodeId targetNodeId, final PeerMessage message) {
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
        final ByteBuffer encoded = PeerMessageCodec.encode(message);
        final PeerChannelSecurity security = channelSecurity.get(channel);
        final List<FrameCodec.Frame> frames = connection.chunkingEngine.encodePayload(FrameCodec.TYPE_PEER_MESSAGE,
                encoded);
        for (int i = 0; i < frames.size(); i++) {
            final FrameCodec.Frame frame = frames.get(i);
            final ByteBuffer frameWire = frameCodec.encode(frame.type, frame.payload);
            if (!security.handshakeFinished()) {
                // Channel security handshake still in progress (mTLS): buffer the
                // plaintext frame and flush it, encrypted, once the handshake and
                // PEER_HELLO have been sent. Frames retain their order.
                pendingAppFrames.computeIfAbsent(channel, c -> new ArrayDeque<>()).add(frameWire);
                continue;
            }
            final ByteBuffer wire = security.wrap(frameWire);
            final int wireBytes = wire.remaining();
            if (connection.queuedOutBytes + wireBytes > config.maxQueuedOutBytes()) {
                closeChannel(channel);
                throw new TransportOverloadedException(
                        TransportOverloadedException.Limit.QUEUED_OUT_BYTES,
                        "Backpressure overflow for peer " + targetNodeId);
            }
            if (connection.txQueue.size() >= config.maxQueuedOutFrames()) {
                closeChannel(channel);
                throw new TransportOverloadedException(
                        TransportOverloadedException.Limit.QUEUED_OUT_BYTES,
                        "TX frame queue overflow for peer " + targetNodeId
                                + " (cap=" + config.maxQueuedOutFrames() + ")");
            }
            if (connection.currentBytes + wireBytes > config.maxPerPeerBytes()) {
                closeChannel(channel);
                throw new TransportOverloadedException(
                        TransportOverloadedException.Limit.TRANSPORT_BYTE_BUDGET,
                        "Per-peer byte budget exceeded for peer " + targetNodeId);
            }
            connection.enqueue(wire);
            addBytes(connection, wireBytes);
        }
        enableWrite(channel);
    }

    /** Encrypt and enqueue any application frames buffered during the handshake. */
    private void flushPendingAppFrames(final SocketChannel channel, final ConnectionState connection) {
        final Deque<ByteBuffer> pending = pendingAppFrames.remove(channel);
        if (pending == null || pending.isEmpty()) {
            return;
        }
        final PeerChannelSecurity security = channelSecurity.get(channel);
        ByteBuffer frameWire;
        while ((frameWire = pending.poll()) != null) {
            final ByteBuffer wire = security.wrap(frameWire);
            final int wireBytes = wire.remaining();
            connection.enqueue(wire);
            addBytes(connection, wireBytes);
        }
        enableWrite(channel);
    }

    @Override
    public void register(final Consumer<PeerMessage> handler) {
        this.handler = handler;
    }

    @Override
    public void bindPromiseCeiling(final LongSupplier source) {
        this.promiseCeilingSource = source;
    }

    /** This node's own claim, or UNKNOWN while replay has not established one. */
    private long ownPromiseCeiling() {
        return promiseCeilingSource.getAsLong();
    }

    @Override
    public List<NodeId> peers() {
        return peers;
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
                    "Cannot read the bound address of TcpPeerTransport", e);
        }
    }

    @Override
    public int clusterSize() {
        return clusterSize; // frozen at startup, not the live peers().size()+1
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
                    if (key.isConnectable()) {
                        onConnect(key);
                    }
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
            final ConnectionState state = new ConnectionState(channel, config.initialRxBytes(), config.maxFrameBytes(),
                    config.chunkPayloadBytes(), config.maxInflightBytes(), true, rxPool);
            connections.put(channel, state);
            addBytes(state, state.rxBuffer.capacity());
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
                // Deliberately NOT resetting the reconnect backoff here. A socket that connects is
                // not a peer that accepts us: the handshake after it can still be refused for a
                // cluster-size, identity or certificate mismatch, and every one of those is a
                // standing condition. Resetting on connect made those retry at the maintenance tick
                // for as long as the misconfiguration lasted -- one TCP and TLS handshake per second,
                // forever, with no escalation. The reset moved to the OK response.
                int ops = SelectionKey.OP_READ;
                if (!connection.txQueue.isEmpty()) {
                    ops |= SelectionKey.OP_WRITE;
                }
                key.interestOps(ops);
            }
        } catch (final Exception e) {
            final ConnectionState connection = connections.get(channel);
            if (connection != null && connection.expectedPeerId != null) {
                final ReconnectState rs = reconnectStates.computeIfAbsent(
                        connection.expectedPeerId, id -> new ReconnectState());
                registerConnectFailure(connection.expectedPeerId, rs);
            }
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
        final PeerChannelSecurity security = channelSecurity.get(channel);
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
                // Dialer: once the security handshake completes, send the
                // deferred PEER_HELLO (and bind the acceptor's cert identity).
                maybeSendPeerHello(channel, connection);
                if (!connections.containsKey(channel)) {
                    return;
                }
                if (security.handshakeFinished() && drainAndDispatch(channel, connection)) {
                    return; // channel closed during dispatch
                }
                if (netRx.position() == netBefore) {
                    // No forward progress (app buffer full / handshake needs more
                    // net bytes) -- stop to avoid a busy loop; leftover net bytes
                    // are retained by compact() below.
                    break;
                }
            }
            netRx.compact();
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    /**
     * Drain complete frames from the connection's application buffer and
     * dispatch them. Returns {@code true} if the channel was closed while
     * dispatching (the caller must then stop touching it).
     */
    private boolean drainAndDispatch(final SocketChannel channel, final ConnectionState connection) {
        final List<FrameCodec.Frame> frames = frameCodec.drain(connection.rxBuffer);
        for (int i = 0; i < frames.size(); i++) {
            final FrameCodec.Frame frame = frames.get(i);
            if (frame.type == FrameCodec.TYPE_PEER_HELLO) {
                onPeerHelloFrame(channel, connection, frame.payload);
                if (!connections.containsKey(channel)) {
                    return true;
                }
                continue;
            }
            if (frame.type == FrameCodec.TYPE_PEER_HELLO_RESP) {
                onPeerHelloRespFrame(channel, connection, frame.payload);
                if (!connections.containsKey(channel)) {
                    return true;
                }
                continue;
            }
            final ByteBuffer payload;
            if (frame.type == FrameCodec.TYPE_PEER_MESSAGE) {
                payload = frame.payload;
            } else {
                final int inboundBefore = connection.chunkingEngine.inboundBytes();
                try {
                    payload = connection.chunkingEngine.onFrame(frame);
                } finally {
                    final int delta = connection.chunkingEngine.inboundBytes() - inboundBefore;
                    if (delta != 0) {
                        addBytes(connection, delta);
                    }
                }
            }
            if (payload != null) {
                if (!connection.helloReceived) {
                    // Peer port: every connection must complete PEER_HELLO before
                    // any application frame is accepted. Gate before decoding, as
                    // TcpClientServerTransport does -- an unauthenticated peer must not
                    // reach the codec at all.
                    closeChannel(channel);
                    return true;
                }
                final PeerMessage message = PeerMessageCodec.decode(payload);
                if (!message.senderId().equals(connection.authenticatedPeerId)) {
                    closeChannel(channel);
                    return true;
                }
                final Consumer<PeerMessage> localHandler = handler;
                if (localHandler != null) {
                    localHandler.accept(message);
                }
            }
        }
        return false;
    }

    /** Enqueue any network bytes the security handshake needs to send. */
    private void flushSecurityOutbound(final SocketChannel channel, final PeerChannelSecurity security) {
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
            addBytes(connection, bytes);
            out = security.pendingOutbound();
        } while (out != null);
        enableWrite(channel);
    }

    /**
     * Start the channel-security handshake for a freshly-created connection and,
     * if it is already finished (plaintext), send the dialer's PEER_HELLO.
     */
    private void beginSecurity(final SocketChannel channel, final ConnectionState connection) {
        final PeerChannelSecurity security = channelSecurity.get(channel);
        flushSecurityOutbound(channel, security);
        maybeSendPeerHello(channel, connection);
    }

    /**
     * On the dialing side, once the security handshake is finished, verify the
     * acceptor's cert identity (if any) against the peer we intended to reach,
     * then enqueue PEER_HELLO exactly once.
     */
    private void maybeSendPeerHello(final SocketChannel channel, final ConnectionState connection) {
        if (connection.expectedPeerId == null || connection.helloSent) {
            return; // only the dialer sends PEER_HELLO, and only once
        }
        final PeerChannelSecurity security = channelSecurity.get(channel);
        if (!security.handshakeFinished()) {
            return;
        }
        if (!verifyCredential(security.peerCredential(), connection.expectedPeerId)) {
            closeChannel(channel);
            return;
        }
        connection.helloSent = true;
        enqueuePeerHelloFrame(connection);
        // Flush any peer messages buffered while the TLS handshake was running,
        // after PEER_HELLO so the acceptor accepts them.
        flushPendingAppFrames(channel, connection);
        enableWrite(channel);
    }

    /**
     * Cross-check a TLS peer credential (cert SAN) against the expected node id
     * and this node's cluster id. Unauthenticated (plaintext) credentials pass
     * -- membership is then enforced purely at the PEER_HELLO layer.
     */
    private boolean verifyCredential(final PeerCredential credential, final NodeId expectedPeerId) {
        if (!credential.authenticated()) {
            return true;
        }
        if (!clusterId.equals(credential.clusterId())) {
            return false;
        }
        return expectedPeerId == null || expectedPeerId.equals(credential.nodeId());
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
                addBytes(connection, -written);
                if (head.hasRemaining()) {
                    break;
                }
                connection.txQueue.poll();
            }
            connection.onWriteProgress();
            if (connection.txQueue.isEmpty()) {
                if (connection.closeAfterFlush) {
                    // Rejection PEER_HELLO_RESP has drained -- drop the connection.
                    closeChannel(channel);
                    return;
                }
                disableWrite(channel);
            }
        } catch (final Exception e) {
            closeChannel(channel);
        }
    }

    private SocketChannel ensureConnected(final NodeId targetNodeId) {
        final SocketChannel existing = outboundByPeerId.get(targetNodeId);
        if (existing != null && existing.isOpen()) {
            return existing;
        }
        final TcpMemberInfo member = peerAddresses.get(targetNodeId);
        if (member == null) {
            throw new IllegalArgumentException("Unknown peer: " + targetNodeId);
        }
        // Deterministic single connection per pair: the lower node id dials, the
        // higher accepts. If we are the acceptor for this pair, we cannot dial --
        // we send over the connection the peer establishes to us. Until it does,
        // the send is dropped and the caller (consensus) retries.
        if (nodeId.compareTo(targetNodeId) > 0) {
            throw new TransportUnavailableException(
                    TransportUnavailableException.Reason.CONNECT_FAILED,
                    "No inbound connection from peer " + targetNodeId + " yet");
        }
        final ReconnectState reconnect = reconnectStates.computeIfAbsent(
                targetNodeId, id -> new ReconnectState());
        final long now = System.nanoTime();
        if (reconnect.nextAttemptNanos != 0L && now < reconnect.nextAttemptNanos) {
            throw new TransportUnavailableException(
                    TransportUnavailableException.Reason.RECONNECT_BACKOFF,
                    "Peer " + targetNodeId + " " + ERR_IN_RECONNECT_BACKOFF + " for another "
                            + Duration.ofNanos(reconnect.nextAttemptNanos - now).toMillis()
                            + "ms");
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
            // Nagle off. Every message on this mesh is a small request or its reply, and a
            // consensus round is a chain of them, so coalescing has nothing to coalesce and only
            // waits for an ACK that the peer is holding back for the same reason.
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            // Resolve the address fresh for this attempt -- never reuse a cached
            // endpoint, so a peer that moved (e.g. rescheduled pod behind a stable
            // DNS name) is dialed at its current endpoint without a config reload.
            channel.connect(member.resolve());
            channel.register(selector, SelectionKey.OP_CONNECT);
            final ConnectionState state = new ConnectionState(channel, config.initialRxBytes(), config.maxFrameBytes(),
                    config.chunkPayloadBytes(), config.maxInflightBytes(), false, rxPool);
            state.expectedPeerId = targetNodeId;
            state.helloReceived = true;
            connections.put(channel, state);
            addBytes(state, state.rxBuffer.capacity());
            channelSecurity.put(channel, securityProvider.forOutbound());
            netRxBuffers.put(channel, ByteBuffer.allocate(config.initialRxBytes()));
            outboundByPeerId.put(targetNodeId, channel);
            // Kick off the channel security handshake and, once it is finished
            // (immediately for plaintext), enqueue the deferred PEER_HELLO.
            beginSecurity(channel, state);
            return channel;
        } catch (final Exception e) {
            if (channel != null) {
                closeChannel(channel);
            }
            registerConnectFailure(targetNodeId, reconnect);
            throw new TransportUnavailableException(
                    TransportUnavailableException.Reason.CONNECT_FAILED,
                    TransportErrors.ERR_CONNECT_FAILED_PREFIX + " " + targetNodeId, e);
        }
    }

    private void registerConnectFailure(final NodeId targetNodeId, final ReconnectState state) {
        state.consecutiveFailures++;
        final int shift = Math.min(state.consecutiveFailures - 1, BACKOFF_MAX_SHIFT);
        final long base = Math.min(
                config.reconnectBackoffBase().toNanos() << shift,
                config.reconnectBackoffCap().toNanos());
        final double jitterFactor = 1.0
                + (ThreadLocalRandom.current().nextDouble() * 2.0 - 1.0) * BACKOFF_JITTER;
        final long delay = Math.max(1L, (long) (base * jitterFactor));
        state.nextAttemptNanos = System.nanoTime() + delay;
    }

    private void resetReconnectState(final NodeId peerId) {
        final ReconnectState state = reconnectStates.get(peerId);
        if (state != null) {
            state.consecutiveFailures = 0;
            state.nextAttemptNanos = 0L;
        }
    }

    private static final class ReconnectState {
        int consecutiveFailures;
        long nextAttemptNanos;
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
        if (connection.currentBytes + delta > config.maxPerPeerBytes()) {
            throw new TransportOverloadedException(TransportOverloadedException.Limit.TRANSPORT_BYTE_BUDGET,
                    "Per-peer byte budget exceeded");
        }
        final ByteBuffer oldBuffer = connection.rxBuffer;
        final ByteBuffer grown = ByteBuffer.allocate(next);
        oldBuffer.flip();
        grown.put(oldBuffer);
        connection.rxBuffer = grown;
        rxPool.release(oldBuffer);
        addBytes(connection, delta);
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

    private void periodicMaintenance() {
        evictSlowConsumers();
        connectDialablePeers();
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

    /**
     * Proactively (re)establish the connections this node is responsible for
     * dialing (peers with a greater node id), so an otherwise-idle lower node
     * still links to higher peers that cannot dial it. Failed attempts back off
     * via {@link #reconnectStates}.
     */
    private void connectDialablePeers() {
        if (closed) {
            return;
        }
        if (ownPromiseCeiling() == PROMISE_CEILING_REPLAYING) {
            // Nothing to say yet, and a handshake happens once: dialing now would spend this
            // connection's only chance to be checked. The next tick tries again, and replay is
            // what ends the wait. A node that replayed but cannot prove its ceiling does dial --
            // it has to, to be given a floor at all.
            return;
        }
        for (final NodeId peer : peers) {
            if (nodeId.compareTo(peer) >= 0) {
                continue; // the peer dials us
            }
            final SocketChannel existing = outboundByPeerId.get(peer);
            if (existing != null && existing.isOpen()) {
                continue;
            }
            try {
                ensureConnected(peer);
            } catch (final Exception ignored) {
                // backoff / not-ready -- retried on the next tick
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
                estimatedTransportBytes -= removed.currentBytes;
                removed.currentBytes = 0;
                if (estimatedTransportBytes < 0) {
                    throw new IllegalStateException(
                            "estimatedTransportBytes went negative ("
                                    + estimatedTransportBytes + ") on close -- "
                                    + "delta accounting bug");
                }
                rxPool.release(removed.rxBuffer);
                // A dialed connection that reached the peer and then went without ever completing
                // the handshake -- refused, or dropped mid-handshake. Backed off exactly like a
                // refused connect, because a refusal is usually a standing condition and retrying it
                // at the maintenance tick is a connection storm against a peer that will not have us.
                // Guarded on `connected` so the dial-failure path, which registers its own, does not
                // count the same attempt twice.
                if (removed.connected
                        && removed.expectedPeerId != null
                        && removed.authenticatedPeerId == null) {
                    registerConnectFailure(removed.expectedPeerId,
                            reconnectStates.computeIfAbsent(
                                    removed.expectedPeerId, id -> new ReconnectState()));
                }
            }
            outboundByPeerId.values().removeIf(c -> c == channel);
            final PeerChannelSecurity removedSecurity = channelSecurity.remove(channel);
            if (removedSecurity != null) {
                removedSecurity.close();
            }
            netRxBuffers.remove(channel);
            pendingAppFrames.remove(channel);
            final SelectionKey key = selector.isOpen() ? channel.keyFor(selector) : null;
            if (key != null) {
                key.cancel();
            }
            channel.close();
            announceDisconnect(removed);
        } catch (final IllegalStateException e) {
            throw e;
        } catch (final Exception ignored) {
        }
    }

    /**
     * Reports a handshaked peer going away, once its last authenticated channel is gone.
     * <p>
     * Two guards, both load-bearing. A connection with no {@code authenticatedPeerId} never reached
     * {@code peerHandshakeCompleted}, so a failed dial or a rejected handshake must not produce a
     * disconnect for a peer that was never up. And the acceptor's stale-primary swap closes a
     * replaced channel while a live one for the same peer remains -- announcing there would emit a
     * spurious down/up pair, inflating the transition counters and moving the last-change timestamp
     * for what is really one continuous session. So the peer is only reported down when no
     * authenticated channel to it survives.
     */
    private void announceDisconnect(final ConnectionState removed) {
        if (removed == null || removed.authenticatedPeerId == null) {
            return;
        }
        final NodeId peer = removed.authenticatedPeerId;
        for (final ConnectionState remaining : connections.values()) {
            if (peer.equals(remaining.authenticatedPeerId)) {
                return;
            }
        }
        observer.peerDisconnected(peer, "connection closed");
    }

    private void addBytes(final ConnectionState connection, final long delta) {
        connection.currentBytes += delta;
        estimatedTransportBytes += delta;
        if (connection.currentBytes < 0) {
            throw new IllegalStateException(
                    "Per-connection byte counter went negative: " + connection.currentBytes);
        }
    }

    /**
     * Record {@code seen} as {@code peer}'s incarnation, reporting a change from the one on file.
     * <p>
     * <b>A change is not a refusal.</b> It says this member's storage has been replaced -- a disk
     * swap or a wipe, which are identical on the wire and need not be told apart: a member that
     * starts without state recovers its promise floor from a quorum before it serves anything, so
     * it arrives having forgotten promises but knowing a bound above all of them. Refusing here
     * would only keep it from asking, and it would refuse a legitimate disk replacement with no way
     * to lift it. What stays refused is {@link #duplicateIncarnationOwner}: one storage under two
     * member ids is state <em>duplicated</em>, not state missing, and no recovery makes it right.
     * <p>
     * The old incarnation keeps its entry in {@code incarnationOwners}, so a copy of the replaced
     * storage turning up under another id is still caught.
     *
     * @return the incarnation previously on file when it differs, for reporting; otherwise
     *         {@code null}
     */
    private IncarnationId recordIncarnation(final NodeId peer, final IncarnationId seen) {
        if (peer == null || seen == null) {
            return null;   // nothing to compare against yet; other guards cover an absent identity
        }
        final IncarnationId known = knownIncarnations.put(peer, seen);
        incarnationOwners.putIfAbsent(seen, peer);
        return known != null && !known.equals(seen) ? known : null;
    }

    /**
     * The member that already owns {@code seen}, when it is not {@code peer} -- i.e. two identities
     * presenting one storage.
     *
     * @return the owner on file, or {@code null} when this storage is unclaimed or claimed by this
     *         same peer
     */
    private NodeId duplicateIncarnationOwner(final NodeId peer, final IncarnationId seen) {
        if (peer == null || seen == null) {
            return null;
        }
        final NodeId owner = incarnationOwners.get(seen);
        return owner == null || owner.equals(peer) ? null : owner;
    }

    private boolean canOpenConnection() {
        if (connections.size() >= config.maxConnections()) {
            return false;
        }
        final long projectedBytes = estimatedTransportBytes + config.initialRxBytes();
        return projectedBytes <= config.derivedMaxTransportBytes();
    }

    private void enqueuePeerHelloFrame(final ConnectionState connection) {
        final ByteBuffer hello = PeerHelloCodec.encode(
                TransportProtocol.PROTOCOL_VERSION,
                clusterId,
                nodeId,
                ThreadLocalRandom.current().nextLong(),  // nonce (replay uniqueness)
                System.currentTimeMillis(),              // epoch millis (freshness)
                clusterSize,                             // our N (1..255) -- must match the peer's
                incarnation,                             // which run of our storage this is
                ownPromiseCeiling());                    // and how far that run has promised
        final ByteBuffer helloFrame = channelSecurity.get(connection.channel)
                .wrap(frameCodec.encode(FrameCodec.TYPE_PEER_HELLO, hello));
        final int wireBytes = helloFrame.remaining();
        connection.enqueue(helloFrame);
        addBytes(connection, wireBytes);
    }

    private void onPeerHelloFrame(
            final SocketChannel channel,
            final ConnectionState connection,
            final ByteBuffer payload) {
        final int remoteVersion;
        try {
            remoteVersion = PeerHelloCodec.peekVersion(payload);
        } catch (final RuntimeException e) {
            // Malformed handshake -- no response we can sensibly send.
            closeChannel(channel);
            return;
        }
        if (remoteVersion != TransportProtocol.PROTOCOL_VERSION) {
            // Respond before parsing the rest: the payload layout may differ
            // across protocol versions, so only the leading version int is safe
            // to read here.
            respondToPeerAndClose(channel, PeerHelloRespStatus.PROTOCOL_MISMATCH, "");
            return;
        }

        final PeerHelloCodec.Decoded hello;
        try {
            hello = PeerHelloCodec.decode(payload);
        } catch (final RuntimeException e) {
            // Malformed handshake -- no response we can sensibly send.
            closeChannel(channel);
            return;
        }
        final ClusterId remoteClusterId = hello.clusterId;
        final NodeId remotePeerId = hello.nodeId;
        final long epochMs = hello.epochMs;
        final int remoteClusterSize = hello.clusterSize;
        // mTLS pre-check: before consulting the member list, require the peer's
        // certificate SAN (cluster_id/node_id) to match the claimed HELLO identity.
        // Plaintext credentials are unauthenticated and skip this gate.
        final PeerChannelSecurity security = channelSecurity.get(channel);
        final PeerCredential credential = security != null ? security.peerCredential() : PeerCredential.NONE;
        if (credential.authenticated()
                && (!remotePeerId.equals(credential.nodeId())
                        || !remoteClusterId.equals(credential.clusterId()))) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.IDENTITY_MISMATCH,
                    PeerHelloRespStatus.CAUSE_CERT_SAN);
            return;
        }
        // Reconfiguration guard: refuse a peer whose cluster size (quorum basis)
        // differs from ours, so an in-flight N change can never form a mixed-N mesh.
        if (remoteClusterSize != clusterSize) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.CLUSTER_SIZE_MISMATCH,
                    Integer.toString(remoteClusterSize));
            return;
        }
        // Duplication guard: one storage arriving under a second member id is a copied data
        // directory, which is state duplicated rather than missing, and recovery does not make it
        // right. Checked before the change below, which records the sighting.
        final NodeId duplicateOwner = duplicateIncarnationOwner(remotePeerId, hello.incarnationId);
        if (duplicateOwner != null) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.INCARNATION_DUPLICATED,
                    duplicateOwner.value());
            return;
        }
        final IncarnationId previousIncarnation =
                recordIncarnation(remotePeerId, hello.incarnationId);
        if (previousIncarnation != null) {
            // Reported, not refused: this member has new storage and recovers its own promise floor
            // before it serves anything. See recordIncarnation.
            observer.peerIncarnationChanged(remotePeerId, previousIncarnation.value(),
                    hello.incarnationId.value());
        }
        // Common admission check (both modes), against the member list.
        if (!remoteClusterId.equals(clusterId)) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.CLUSTER_MISMATCH, remoteClusterId.value());
            return;
        }
        if (remotePeerId.equals(nodeId)) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.IDENTITY_MISMATCH,
                    PeerHelloRespStatus.CAUSE_SELF_CLAIM);
            return;
        }
        if (!peerAddresses.containsKey(remotePeerId)) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.UNKNOWN_PEER, remotePeerId.value());
            return;
        }
        if (connection.expectedPeerId != null && !connection.expectedPeerId.equals(remotePeerId)) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.IDENTITY_MISMATCH,
                    PeerHelloRespStatus.CAUSE_EXPECTED_PEER);
            return;
        }
        if (connection.authenticatedPeerId != null && !connection.authenticatedPeerId.equals(remotePeerId)) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.IDENTITY_MISMATCH,
                    PeerHelloRespStatus.CAUSE_AUTHENTICATED_PEER);
            return;
        }
        // Rollback guard: the same storage may never come back holding less than it left with.
        // After the admission checks above, because a node that is not a member of this cluster is
        // not a member whose storage we have an opinion about,
        // and answering it about ceilings would both mislabel its rejection and record a member we
        // do not have.
        //
        // Both ends have to be able to state a ceiling for the comparison to mean anything, and a
        // connection is handshaked once -- so a claim that cannot be checked is refused rather than
        // admitted unchecked. Transient either way: the dialer retries, and replay ends the wait.
        if (ownPromiseCeiling() == PROMISE_CEILING_REPLAYING) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.NOT_REPLAYED,
                    PeerHelloRespStatus.CAUSE_OURS);
            return;
        }
        if (hello.promiseCeiling == PROMISE_CEILING_REPLAYING) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.NOT_REPLAYED,
                    PeerHelloRespStatus.CAUSE_THEIRS);
            return;
        }
        final long rolledBackFrom = promiseCeilings.rolledBackFrom(
                remotePeerId, hello.incarnationId, hello.promiseCeiling);
        if (rolledBackFrom != PromiseCeilingHistory.NO_ROLLBACK) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.CEILING_ROLLED_BACK,
                    hello.promiseCeiling + "<" + rolledBackFrom);
            return;
        }
        promiseCeilings.record(remotePeerId, hello.incarnationId, hello.promiseCeiling);

        final long skewMillis = epochMs - System.currentTimeMillis();
        if (Math.abs(skewMillis) > MAX_HELLO_SKEW_MS) {
            respondToPeerAndClose(channel, PeerHelloRespStatus.HELLO_TIMESTAMP_SKEW, Long.toString(epochMs));
            return;
        }
        // Reported rather than merely bounded: see NodeObserver#peerClockSkewObserved. Only for a
        // peer that passed, since a rejected one is already reported as the rejection it is.
        observer.peerClockSkewObserved(remotePeerId, skewMillis);

        // Success -- enqueue OK through the (possibly encrypted) tx path.
        enqueueHelloResp(channel, connection, PeerHelloRespStatus.OK, "");

        connection.authenticatedPeerId = remotePeerId;
        connection.helloReceived = true;

        // Single-dialer model: the lower id dials, the higher accepts. When we
        // are the acceptor (our id is higher), adopt this inbound channel as the
        // primary connection for reaching that peer, so our own sends to it flow
        // over the same link. If a stale primary exists, drop it first.
        if (nodeId.compareTo(remotePeerId) > 0) {
            final SocketChannel stale = outboundByPeerId.get(remotePeerId);
            if (stale != null && stale != channel && stale.isOpen()) {
                closeChannel(stale);
            }
            outboundByPeerId.put(remotePeerId, channel);
        }

        // Acceptor side of the handshake: we have authenticated this peer and will accept its
        // protocol messages, so it now counts towards quorum. Announced after the stale-primary
        // swap above so the observer never sees an up for a channel we are about to replace.
        observer.peerHandshakeCompleted(remotePeerId);
    }

    private void onPeerHelloRespFrame(
            final SocketChannel channel,
            final ConnectionState connection,
            final ByteBuffer payload) {
        final PeerHelloRespCodec.Decoded resp;
        try {
            resp = PeerHelloRespCodec.decode(payload);
        } catch (final RuntimeException e) {
            closeChannel(channel);
            return;
        }
        final PeerHelloRespStatus status = resp.status;
        final String cause = resp.cause;
        final int acceptorClusterSize = resp.clusterSize;
        if (status != PeerHelloRespStatus.OK) {
            observer.peerHandshakeRejected(connection.expectedPeerId, status, cause);
            closeChannel(channel);
            return;
        }
        // Independently verify the acceptor's cluster size matches ours -- the
        // initiator does not rely on the acceptor having checked (reconfiguration guard).
        if (acceptorClusterSize != clusterSize) {
            observer.peerHandshakeRejected(connection.expectedPeerId,
                    PeerHelloRespStatus.CLUSTER_SIZE_MISMATCH, Integer.toString(acceptorClusterSize));
            closeChannel(channel);
            return;
        }
        // And the rollback guard, in this direction for the same reason: the dialer sends the
        // HELLO, so nothing has checked the acceptor's own claim about its storage.
        if (resp.promiseCeiling == PROMISE_CEILING_REPLAYING) {
            observer.peerHandshakeRejected(connection.expectedPeerId,
                    PeerHelloRespStatus.NOT_REPLAYED, PeerHelloRespStatus.CAUSE_THEIRS);
            closeChannel(channel);
            return;
        }
        final long acceptorRolledBackFrom = promiseCeilings.rolledBackFrom(
                connection.expectedPeerId, resp.incarnationId, resp.promiseCeiling);
        if (acceptorRolledBackFrom != PromiseCeilingHistory.NO_ROLLBACK) {
            observer.peerHandshakeRejected(connection.expectedPeerId,
                    PeerHelloRespStatus.CEILING_ROLLED_BACK,
                    resp.promiseCeiling + "<" + acceptorRolledBackFrom);
            closeChannel(channel);
            return;
        }
        promiseCeilings.record(
                connection.expectedPeerId, resp.incarnationId, resp.promiseCeiling);
        // And independently verify the acceptor's incarnation, for the same reason: the dialer
        // sends the HELLO, so the acceptor's guard never runs on this connection.
        final NodeId acceptorDuplicate =
                duplicateIncarnationOwner(connection.expectedPeerId, resp.incarnationId);
        if (acceptorDuplicate != null) {
            observer.peerHandshakeRejected(connection.expectedPeerId,
                    PeerHelloRespStatus.INCARNATION_DUPLICATED, acceptorDuplicate.value());
            closeChannel(channel);
            return;
        }
        final IncarnationId acceptorPrevious =
                recordIncarnation(connection.expectedPeerId, resp.incarnationId);
        if (acceptorPrevious != null) {
            observer.peerIncarnationChanged(connection.expectedPeerId, acceptorPrevious.value(),
                    resp.incarnationId.value());
        }
        // The acceptor confirmed our PEER_HELLO. Authenticate this (single,
        // full-duplex) channel as the expected peer so inbound messages from it
        // are accepted -- the dialer never runs the acceptor's PEER_HELLO path.
        connection.authenticatedPeerId = connection.expectedPeerId;
        // And this, not the socket connecting, is what clears the reconnect backoff: it is the
        // first moment the peer has actually taken us.
        resetReconnectState(connection.expectedPeerId);

        // Dialer side of the handshake: OK received and the cluster size independently verified
        // above, so the peer is usable for consensus and counts towards quorum from here.
        observer.peerHandshakeCompleted(connection.expectedPeerId);
    }

    /**
     * Serialise a PEER_HELLO_RESP, wrap it through the connection's channel
     * security (identity for plaintext, encrypt for TLS), and enqueue it on the
     * tx path. Bypassing direct socket writes keeps the TLS record stream
     * intact.
     */
    private void enqueueHelloResp(final SocketChannel channel, final ConnectionState connection,
                                  final PeerHelloRespStatus status, final String cause) {
        // clusterSize goes out on rejections too, so the connector can verify agreement itself.
        final ByteBuffer resp = PeerHelloRespCodec.encode(
                status, cause, clusterSize, incarnation, ownPromiseCeiling());
        final PeerChannelSecurity security = channelSecurity.get(channel);
        final ByteBuffer wire = security.wrap(
                frameCodec.encode(FrameCodec.TYPE_PEER_HELLO_RESP, resp));
        final int wireBytes = wire.remaining();
        connection.enqueue(wire);
        addBytes(connection, wireBytes);
        enableWrite(channel);
    }

    private void respondToPeerAndClose(final SocketChannel channel, final PeerHelloRespStatus status,
                                       final String cause) {
        final ConnectionState connection = connections.get(channel);
        if (connection == null) {
            closeChannel(channel);
            return;
        }
        // Send the rejection through the tx path, then close once it drains.
        enqueueHelloResp(channel, connection, status, cause);
        connection.closeAfterFlush = true;
    }

    private void ensureInLoop() {
        if (!loop.inLoop()) {
            throw new IllegalStateException("TcpPeerTransport.send must be called on EventLoop thread");
        }
    }
}
