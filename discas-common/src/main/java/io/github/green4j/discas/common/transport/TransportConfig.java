/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/**
 * The framing and backpressure budget shared by every TCP transport in the project: the peer
 * mesh, the node's client-facing server, and the outbound client.
 * <p>
 * A subclass adds only what is specific to its side. Every default is declared once, here: three
 * are absolute, and two are derived from the largest message the subclass's framing must carry,
 * which is exactly what {@link #validate} requires them to exceed -- the default is that floor plus
 * headroom, so the two cannot disagree. In practice that is roughly 21 MiB of queue and inflight
 * budget on the peer side and 41 MiB on the client side, since a client CAS carries two values
 * where a peer accept carries one.
 */
public abstract class TransportConfig {

    /**
     * Headroom over the largest single message, applied to the queue and inflight defaults.
     * A budget exactly equal to one max-size message leaves no room to make progress while
     * one is in flight.
     */
    private static final double MESSAGE_BUDGET_HEADROOM = 1.25;

    private static final double TRANSPORT_BUDGET_HEADROOM = 1.1;

    /** Largest single message this transport's framing must carry. */
    private final int maxMessageBytes;

    /** What that message is called, for diagnostics ("client message", "peer message"). */
    private final String messageKind;

    // Largest frame put on the wire; a payload above this is chunked.
    private int maxFrameBytes = 256 * 1024;

    // Per-connection receive buffer. Must hold one complete frame (see validate).
    private int maxRxBufferBytes = 512 * 1024;

    // Connections this transport will hold open.
    private int maxConnections = 128;

    // Derived from maxMessageBytes in the constructor below.
    private int maxQueuedOutBytes;
    private int maxInflightBytes;

    private int chunkPayloadBytes;
    private int initialRxBytes;
    private long derivedMaxTransportBytes;

    /**
     * @param maxMessageBytes largest single message the subclass's framing must carry
     * @param messageKind     what that message is called, for diagnostics
     */
    protected TransportConfig(final int maxMessageBytes, final String messageKind) {
        this.maxMessageBytes = maxMessageBytes;
        this.messageKind = messageKind;
        this.maxQueuedOutBytes = withHeadroom(maxMessageBytes);
        this.maxInflightBytes = withHeadroom(maxMessageBytes);
    }

    /**
     * Copy constructor: derives the internal values and validates. The built config shares no
     * state with the builder that produced it.
     */
    protected TransportConfig(final TransportConfig other) {
        this.maxMessageBytes = other.maxMessageBytes;
        this.messageKind = other.messageKind;
        this.maxFrameBytes = other.maxFrameBytes;
        this.maxRxBufferBytes = other.maxRxBufferBytes;
        this.maxConnections = other.maxConnections;
        this.maxQueuedOutBytes = other.maxQueuedOutBytes;
        this.maxInflightBytes = other.maxInflightBytes;

        this.chunkPayloadBytes = FrameCodec.maxPayloadBytesForFrame(maxFrameBytes)
                - Long.BYTES - Integer.BYTES;
        this.initialRxBytes = Math.min(64 * 1024, maxRxBufferBytes);
        this.derivedMaxTransportBytes = (long) Math.ceil(
                (long) maxConnections * ((long) maxRxBufferBytes + maxQueuedOutBytes + maxInflightBytes)
                        * TRANSPORT_BUDGET_HEADROOM);

        validate();
    }

    private static int withHeadroom(final int maxMessageBytes) {
        return (int) Math.min(Integer.MAX_VALUE,
                (long) Math.ceil(maxMessageBytes * MESSAGE_BUDGET_HEADROOM));
    }

    /** Largest frame put on the wire; a payload above this is chunked (default 256 KiB). */
    public final int maxFrameBytes() {
        return maxFrameBytes;
    }

    /** Per-connection outbound queue budget (default: the max message size plus headroom). */
    public final int maxQueuedOutBytes() {
        return maxQueuedOutBytes;
    }

    /** Per-connection receive buffer (default 512 KiB). */
    public final int maxRxBufferBytes() {
        return maxRxBufferBytes;
    }

    /** Per-connection reassembly budget (default: the max message size plus headroom). */
    public final int maxInflightBytes() {
        return maxInflightBytes;
    }

    /** Connections this transport will hold open (default 128). */
    public final int maxConnections() {
        return maxConnections;
    }

    /** Payload bytes one chunk part may carry, after frame and chunk-header overhead. */
    public final int chunkPayloadBytes() {
        return chunkPayloadBytes;
    }

    /** Receive buffer a fresh connection starts with, grown on demand up to the maximum. */
    public final int initialRxBytes() {
        return initialRxBytes;
    }

    /** Transport-wide byte budget across all connections. */
    public final long derivedMaxTransportBytes() {
        return derivedMaxTransportBytes;
    }

    private void validate() {
        requirePositive(maxFrameBytes, "maxFrameBytes");
        requirePositive(maxQueuedOutBytes, "maxQueuedOutBytes");
        requirePositive(maxRxBufferBytes, "maxRxBufferBytes");
        requirePositive(maxInflightBytes, "maxInflightBytes");
        requirePositive(maxConnections, "maxConnections");

        if (chunkPayloadBytes <= 0) {
            throw new IllegalArgumentException("chunkPayloadBytes too small for chunk part overhead");
        }

        if (maxRxBufferBytes < maxFrameBytes + FrameCodec.FRAME_LENGTH_PREFIX_BYTES) {
            throw new IllegalArgumentException(
                    "maxRxBufferBytes must be >= maxFrameBytes + "
                            + FrameCodec.FRAME_LENGTH_PREFIX_BYTES
                            + " to hold one complete frame");
        }

        // A max-size message must both reassemble (inbound) and enqueue (outbound).
        if (maxInflightBytes < maxMessageBytes) {
            throw new IllegalArgumentException(
                    "maxInflightBytes (" + maxInflightBytes + ") must be >= the maximum "
                            + messageKind + " size " + maxMessageBytes
                            + " (derived from KvLimits) so a max-size message can be reassembled");
        }
        if (maxQueuedOutBytes < maxMessageBytes) {
            throw new IllegalArgumentException(
                    "maxQueuedOutBytes (" + maxQueuedOutBytes + ") must be >= the maximum "
                            + messageKind + " size " + maxMessageBytes
                            + " (derived from KvLimits) so a max-size message can be sent");
        }
    }

    protected static int requirePositive(final int value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
        return value;
    }

    // Called by each subclass's Builder, which lives inside the subclass body and so may reach
    // these protected setters on the config instance it is populating.

    protected final void setMaxFrameBytes(final int value) {
        this.maxFrameBytes = requirePositive(value, "maxFrameBytes");
    }

    protected final void setMaxQueuedOutBytes(final int value) {
        this.maxQueuedOutBytes = requirePositive(value, "maxQueuedOutBytes");
    }

    protected final void setMaxRxBufferBytes(final int value) {
        this.maxRxBufferBytes = requirePositive(value, "maxRxBufferBytes");
    }

    protected final void setMaxInflightBytes(final int value) {
        this.maxInflightBytes = requirePositive(value, "maxInflightBytes");
    }

    protected final void setMaxConnections(final int value) {
        this.maxConnections = requirePositive(value, "maxConnections");
    }
}
