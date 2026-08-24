/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.transport.TransportConfig;
import io.github.green4j.discas.node.PeerMessageCodec;

import java.time.Duration;

/**
 * Configuration for the peer-to-peer TCP transport ({@link TcpPeerTransport}).
 * <p>
 * Every tunable, its default and its validation live on {@link TransportConfig}; this side adds
 * the peer-specific pieces: a per-peer share of the transport budget, a frame-count cap on the
 * tx queue, and the reload reconnect policy.
 */
public final class TcpTransportConfig extends TransportConfig {

    private static final double PER_PEER_HEADROOM = 1.1;
    private static final int MIN_FRAME_QUEUE_CAP = 1024;
    private static final int ASSUMED_AVG_FRAME_BYTES = 256;

    /**
     * When a member's address changes on reload: {@code false} (default) keeps a
     * healthy connection and uses the new address at the next reconnect;
     * {@code true} drops the live connection to reconnect at the new address now.
     */
    private boolean forceReconnect = false;

    /**
     * First delay before redialling a peer that dropped, doubled per consecutive failure up to
     * {@link #reconnectBackoffCap}. Kept short: a peer that bounced should be back in quorum
     * quickly, and the cap is what stops a persistently dead peer being retried in a tight loop.
     */
    private Duration reconnectBackoffBase = Duration.ofMillis(100);

    /** Ceiling the doubling reconnect delay saturates at. */
    private Duration reconnectBackoffCap = Duration.ofSeconds(5);

    private long maxPerPeerBytes;
    private int maxQueuedOutFrames;

    private TcpTransportConfig() {
        super(PeerMessageCodec.MAX_MESSAGE_BYTES, "value-carrying peer message");
    }

    private TcpTransportConfig(final TcpTransportConfig other) {
        super(other);
        this.forceReconnect = other.forceReconnect;
        this.reconnectBackoffBase = other.reconnectBackoffBase;
        this.reconnectBackoffCap = other.reconnectBackoffCap;

        // Per-peer cap on contribution to the transport-wide byte budget. Without this, one
        // greedy peer can monopolize the whole pool and starve the others.
        this.maxPerPeerBytes = (long) Math.ceil(
                ((long) maxRxBufferBytes() + maxQueuedOutBytes() + maxInflightBytes())
                        * PER_PEER_HEADROOM);
        // Frame-count cap on the per-connection tx queue: independent of the byte cap so a
        // flood of tiny frames cannot blow up the deque object.
        this.maxQueuedOutFrames = Math.max(
                MIN_FRAME_QUEUE_CAP,
                maxQueuedOutBytes() / ASSUMED_AVG_FRAME_BYTES);
    }

    /** A config with every default in place. */
    public static TcpTransportConfig defaults() {
        return builder().build();
    }

    /** A builder pre-loaded with the defaults; override only what needs changing. */
    public static Builder builder() {
        return new Builder();
    }

    /** See {@link #forceReconnect} (default {@code false}). */
    public boolean forceReconnect() {
        return forceReconnect;
    }

    /** See {@link #reconnectBackoffBase} (default 100ms). */
    public Duration reconnectBackoffBase() {
        return reconnectBackoffBase;
    }

    /** See {@link #reconnectBackoffCap} (default 5s). */
    public Duration reconnectBackoffCap() {
        return reconnectBackoffCap;
    }

    /** One peer's share of the transport-wide byte budget. */
    public long maxPerPeerBytes() {
        return maxPerPeerBytes;
    }

    /** Frame-count cap on a single connection's tx queue. */
    public int maxQueuedOutFrames() {
        return maxQueuedOutFrames;
    }

    /**
     * Fluent builder. The setters validate individually; the derived per-peer and frame-count
     * caps are computed in {@link #build()}, once every input is known.
     */
    public static final class Builder {

        private final TcpTransportConfig cfg = new TcpTransportConfig();

        private Builder() {
        }

        /** Largest single wire frame. Must leave room to chunk a max-size peer message. */
        public Builder maxFrameBytes(final int value) {
            cfg.setMaxFrameBytes(value);
            return this;
        }

        /** Byte cap on one connection's tx queue; must be at least one max peer message. */
        public Builder maxQueuedOutBytes(final int value) {
            cfg.setMaxQueuedOutBytes(value);
            return this;
        }

        /** Ceiling a connection's receive buffer may grow to while reassembling a message. */
        public Builder maxRxBufferBytes(final int value) {
            cfg.setMaxRxBufferBytes(value);
            return this;
        }

        /** Byte cap on partially-received messages; must be at least one max peer message. */
        public Builder maxInflightBytes(final int value) {
            cfg.setMaxInflightBytes(value);
            return this;
        }

        /** Peer connections this transport will hold, which sizes the rx buffer pool. */
        public Builder maxConnections(final int value) {
            cfg.setMaxConnections(value);
            return this;
        }

        /** @see TcpTransportConfig#forceReconnect() */
        public Builder forceReconnect(final boolean value) {
            cfg.forceReconnect = value;
            return this;
        }

        /** @see TcpTransportConfig#reconnectBackoffBase() */
        public Builder reconnectBackoffBase(final Duration value) {
            cfg.reconnectBackoffBase = requirePositive(value, "reconnectBackoffBase");
            return this;
        }

        /** @see TcpTransportConfig#reconnectBackoffCap() */
        public Builder reconnectBackoffCap(final Duration value) {
            cfg.reconnectBackoffCap = requirePositive(value, "reconnectBackoffCap");
            return this;
        }

        /**
         * @throws IllegalArgumentException if a budget cannot carry a max-size peer message, if
         *                                  the frame size leaves no room for chunking, or if the
         *                                  reconnect cap is below its base.
         */
        public TcpTransportConfig build() {
            // Checked here rather than in the setters: either half of the pair can legitimately be
            // set first, so a setter cannot know whether the pair is inconsistent yet.
            if (cfg.reconnectBackoffCap.compareTo(cfg.reconnectBackoffBase) < 0) {
                throw new IllegalArgumentException(
                        "reconnectBackoffCap (" + cfg.reconnectBackoffCap + ") must be >= "
                                + "reconnectBackoffBase (" + cfg.reconnectBackoffBase + ")");
            }
            return new TcpTransportConfig(cfg);
        }

        private static Duration requirePositive(final Duration value, final String name) {
            if (value == null) {
                throw new IllegalArgumentException(name + " is required");
            }
            if (value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be > 0, was " + value);
            }
            return value;
        }
    }
}
