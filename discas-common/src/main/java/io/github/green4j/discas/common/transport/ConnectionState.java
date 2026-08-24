/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Per-connection mutable state. Accessed only on the owning EventLoop
 * thread. Shared by both the peer-side and client-side TCP transports --
 * each side passes its own config-derived sizing primitives to construct
 * it.
 */
public final class ConnectionState {
    public final SocketChannel channel;
    public ByteBuffer rxBuffer;
    public final Deque<ByteBuffer> txQueue = new ArrayDeque<>();
    public long queuedOutBytes = 0;
    public long oldestQueuedAtNanos = 0L;
    public boolean connected;
    public NodeId expectedPeerId = null;
    public NodeId authenticatedPeerId = null;
    /**
     * Trusted client identity bound to this connection once its
     * {@code TYPE_CLIENT_HELLO} has been authenticated (client server only).
     * Authorization keys off this value, never off the self-declared
     * {@code senderId} carried in each client message.
     */
    public ClientId authenticatedClientId = null;
    /**
     * True once the side-appropriate HELLO frame has been validated:
     * {@code TYPE_PEER_HELLO} on the peer server, {@code TYPE_CLIENT_HELLO}
     * on the client server. The peer transport also sets it on the
     * outbound side immediately after enqueuing its own HELLO.
     */
    public boolean helloReceived = false;
    /**
     * True once this (dialing) side has enqueued its PEER_HELLO. With mTLS the
     * hello is deferred until the TLS handshake completes; with plaintext it is
     * enqueued immediately. Only the outbound side sends PEER_HELLO.
     */
    public boolean helloSent = false;
    /**
     * When set, the connection is closed once its tx queue drains. Used to send
     * a rejection PEER_HELLO_RESP through the (possibly encrypted) tx path and
     * then drop the connection, rather than writing directly to the socket.
     */
    public boolean closeAfterFlush = false;
    public final ChunkingEngine chunkingEngine;
    /**
     * Bytes this connection currently contributes to the transport-wide
     * byte budget. Maintained as a delta-sum of every {@code addBytes}
     * / {@code releaseBytes} call below so close just subtracts this
     * single value instead of recomputing from three live fields that
     * may have drifted.
     */
    public long currentBytes = 0;

    public ConnectionState(final SocketChannel channel,
                           final int initialRxBytes,
                           final int maxFrameBytes,
                           final int chunkPayloadBytes,
                           final int maxInflightBytes,
                           final boolean connected,
                           final HeapBufferPool rxPool) {
        this.channel = channel;
        this.rxBuffer = (rxPool != null) ? rxPool.acquire() : ByteBuffer.allocate(initialRxBytes);
        this.chunkingEngine = new ChunkingEngine(maxFrameBytes, chunkPayloadBytes, maxInflightBytes);
        this.connected = connected;
    }

    public void enqueue(final ByteBuffer frame) {
        txQueue.add(frame);
        queuedOutBytes += frame.remaining();
        if (connected && oldestQueuedAtNanos == 0L) {
            oldestQueuedAtNanos = System.nanoTime();
        }
    }

    public void markConnected() {
        connected = true;
        if (!txQueue.isEmpty() && oldestQueuedAtNanos == 0L) {
            oldestQueuedAtNanos = System.nanoTime();
        }
    }

    public void onWriteProgress() {
        if (txQueue.isEmpty()) {
            oldestQueuedAtNanos = 0L;
        }
    }

    public boolean isSlowConsumer(final long timeoutNanos) {
        if (oldestQueuedAtNanos == 0L) {
            return false;
        }
        final long elapsedNanos = System.nanoTime() - oldestQueuedAtNanos;
        return elapsedNanos > timeoutNanos;
    }

    public long estimatedBytes() {
        return rxBuffer.capacity() + queuedOutBytes + chunkingEngine.inboundBytes();
    }
}
