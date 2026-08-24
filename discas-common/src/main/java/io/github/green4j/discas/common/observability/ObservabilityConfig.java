/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.observability;

/**
 * Where and whether a process serves its observability endpoints. Shared by the node (health,
 * readiness, metrics) and the agent (metrics); what is actually served is the router each passes to
 * {@link ObservabilityServer}.
 * <p>
 * The bind address defaults to loopback. These endpoints report the cluster's topology, every peer's
 * identity and connection history, so they are not something to put on a public interface by
 * accident. The default is loopback rather than an enforced restriction: a Kubernetes pod whose
 * sidecar scrapes across the pod network has a legitimate reason to bind wider, and refusing it
 * outright would leave that deployment with no way forward. Widening it is a deliberate act.
 * <p>
 * Every default is declared inline on its {@link Builder} field -- that line is the single source
 * of truth for both the value and its documentation.
 */
public final class ObservabilityConfig {

    private final boolean enabled;
    private final String bindAddress;
    private final int port;
    private final int workerCount;

    private ObservabilityConfig(final Builder b) {
        this.enabled = b.enabled;
        this.bindAddress = b.bindAddress;
        this.port = b.port;
        this.workerCount = b.workerCount;
    }

    /**
     * A builder carrying every default. Also the way to read a default without building anything,
     * which is how the CLI renders its help text.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Whether to start the endpoints at all (default {@code true}). */
    public boolean enabled() {
        return enabled;
    }

    /** Interface to bind (default {@code 127.0.0.1}). */
    public String bindAddress() {
        return bindAddress;
    }

    /** Port to bind; {@code 0} takes an ephemeral one (default {@code 9600}). */
    public int port() {
        return port;
    }

    /** HTTP worker threads (default {@code 1}). */
    public int workerCount() {
        return workerCount;
    }

    /** Fluent builder; setters validate individually. */
    public static final class Builder {

        // On by default: an observability endpoint nobody remembered to switch on is worth nothing
        // at three in the morning, and it costs one thread and a loopback socket.
        private boolean enabled = true;

        // Loopback, for the reasons on the class doc.
        private String bindAddress = "127.0.0.1";

        // Clear of the agent's 8500 so a node and an agent can share a host without a collision.
        // A process that may sit beside a node picks its own: the agent's CLI defaults to 9601, so
        // co-locating the two does not need either to be reconfigured.
        private int port = 9600;

        // One worker. Scrapes arrive every few seconds and the handlers do no I/O beyond rendering
        // a small body, so more threads would only add context switches.
        private int workerCount = 1;

        private Builder() {
        }

        /** @see ObservabilityConfig#enabled() */
        public Builder enabled(final boolean value) {
            this.enabled = value;
            return this;
        }

        /** @see ObservabilityConfig#bindAddress() */
        public Builder bindAddress(final String value) {
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("bindAddress is required");
            }
            this.bindAddress = value;
            return this;
        }

        /** @see ObservabilityConfig#port() */
        public Builder port(final int value) {
            if (value < 0 || value > 65535) {
                throw new IllegalArgumentException("port must be in [0, 65535], got " + value);
            }
            this.port = value;
            return this;
        }

        /** @see ObservabilityConfig#workerCount() */
        public Builder workerCount(final int value) {
            if (value < 1) {
                throw new IllegalArgumentException("workerCount must be >= 1, got " + value);
            }
            this.workerCount = value;
            return this;
        }

        /** Returns an immutable config sharing no state with this builder, so it may be reused. */
        public ObservabilityConfig build() {
            return new ObservabilityConfig(this);
        }
    }
}
