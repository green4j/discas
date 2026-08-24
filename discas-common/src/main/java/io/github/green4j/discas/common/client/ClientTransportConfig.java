/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.transport.TransportConfig;

/**
 * Configuration for the client-side TCP transport: both the outbound client and the node's
 * client-facing server. Every tunable, its default and its validation live on
 * {@link TransportConfig}; this side adds only the message family the budget must accommodate,
 * so {@code builder().build()} is already the configuration a node, an agent or an example wants.
 */
public final class ClientTransportConfig extends TransportConfig {

    private ClientTransportConfig() {
        super(ClientMessageCodec.MAX_MESSAGE_BYTES, "client message");
    }

    private ClientTransportConfig(final ClientTransportConfig other) {
        super(other);
    }

    /** A config with every default in place. */
    public static ClientTransportConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final ClientTransportConfig cfg = new ClientTransportConfig();

        private Builder() {
        }

        public Builder maxFrameBytes(final int value) {
            cfg.setMaxFrameBytes(value);
            return this;
        }

        public Builder maxQueuedOutBytes(final int value) {
            cfg.setMaxQueuedOutBytes(value);
            return this;
        }

        public Builder maxRxBufferBytes(final int value) {
            cfg.setMaxRxBufferBytes(value);
            return this;
        }

        public Builder maxInflightBytes(final int value) {
            cfg.setMaxInflightBytes(value);
            return this;
        }

        public Builder maxConnections(final int value) {
            cfg.setMaxConnections(value);
            return this;
        }

        public ClientTransportConfig build() {
            return new ClientTransportConfig(cfg);
        }
    }
}
