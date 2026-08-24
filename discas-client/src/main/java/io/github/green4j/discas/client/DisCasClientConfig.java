/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import java.time.Duration;

/**
 * Timing tunables for {@link DisCasClient}. A fresh config is immediately usable; obtain a builder
 * from {@link #builder()}, override what you need, and call {@code build()}.
 * <p>
 * The deadlines are the embedder's to set, because a caller has a budget of its own: an HTTP
 * front-end that answers {@code 504} after ten seconds cannot use a client whose scan also settles
 * at ten seconds -- the two race, and the caller gets a gateway timeout instead of the partial
 * listing the scan was about to return.
 * <p>
 * Every default is declared inline on its field -- that line is the single source of truth for
 * both the value and its documentation.
 */
public final class DisCasClientConfig {

    // How long one coordinator gets to answer before the request is re-dispatched to the next
    // peer. Not the overall deadline: a request may be attempted once per peer, so the worst case
    // is roughly this times the peer count.
    private Duration perAttemptTimeout = Duration.ofSeconds(5);

    // Hard ceiling on one operation, across every coordinator it is dispatched to. A backstop
    // rather than a retry budget: the client tries each eligible coordinator at most once and
    // never waits, so this only binds when the peer list is long enough that the per-attempt
    // timeouts could otherwise add up past it.
    private Duration requestDeadline = Duration.ofSeconds(30);

    // How long a coordinator that just failed is passed over: base << consecutive failures,
    // clamped to the max, and reset the moment it answers again. This is a memory between calls,
    // not a delay within one -- an operation with no eligible coordinator fails rather than
    // waiting. It buys two things: a caller's re-issued call goes straight to a coordinator that
    // might work, and a caller retrying in a tight loop cannot become a connect storm, since the
    // transport opens a fresh socket per send and rate-limits nothing itself.
    private Duration peerRetryMinBackoff = Duration.ofMillis(50);
    private Duration peerRetryMaxBackoff = Duration.ofSeconds(2);

    // How long a scan waits for answers that may still arrive. Only reached when a peer is
    // reachable but silent -- a scan settles early once a majority answers, once every remaining
    // peer has proved unreachable, or once a majority has become impossible.
    private Duration scanTimeout = Duration.ofSeconds(10);

    // How long close() waits for the event loop to terminate and for in-flight requests to drain.
    private Duration shutdownAwaitTimeout = Duration.ofSeconds(5);

    // Randomised retry window for a contended lock acquisition. Short: lock handoff is the
    // latency the caller feels, and the contending clients are already talking to the same nodes.
    private Duration lockMinBackoff = Duration.ofMillis(20);
    private Duration lockMaxBackoff = Duration.ofMillis(80);

    // Randomised re-poll window for a watch whose key has not changed. Longer than the lock
    // window: a watch is a standing query, so the cost of polling too eagerly is load on every
    // node rather than latency for one caller.
    private Duration watchMinBackoff = Duration.ofMillis(200);
    private Duration watchMaxBackoff = Duration.ofMillis(1000);

    private DisCasClientConfig() {
    }

    /** Copy constructor: the built config shares no state with the builder that produced it. */
    private DisCasClientConfig(final DisCasClientConfig other) {
        this.perAttemptTimeout = other.perAttemptTimeout;
        this.requestDeadline = other.requestDeadline;
        this.peerRetryMinBackoff = other.peerRetryMinBackoff;
        this.peerRetryMaxBackoff = other.peerRetryMaxBackoff;
        this.scanTimeout = other.scanTimeout;
        this.shutdownAwaitTimeout = other.shutdownAwaitTimeout;
        this.lockMinBackoff = other.lockMinBackoff;
        this.lockMaxBackoff = other.lockMaxBackoff;
        this.watchMinBackoff = other.watchMinBackoff;
        this.watchMaxBackoff = other.watchMaxBackoff;
    }

    /** Defaults for every tunable. */
    public static DisCasClientConfig defaults() {
        return new DisCasClientConfig();
    }

    /** A builder pre-loaded with the defaults; override only what the embedder needs. */
    public static Builder builder() {
        return new Builder();
    }

    /** How long one coordinator gets to answer before failover (default 5s). */
    public Duration perAttemptTimeout() {
        return perAttemptTimeout;
    }

    /**
     * Hard ceiling on one operation across every coordinator it is tried on (default 30s).
     * <p>
     * A backstop, not a retry budget -- the client tries each eligible coordinator once and never
     * waits, so what normally ends an operation is running out of coordinators. Must exceed
     * {@link #perAttemptTimeout()}, or the first coordinator could not be given its full turn.
     */
    public Duration requestDeadline() {
        return requestDeadline;
    }

    /**
     * First step of the per-peer skip window, doubling per consecutive failure (default 50ms).
     * <p>
     * Never waited out within an operation; it decides which coordinators a <em>later</em> call
     * passes over. See the field comment for why that is the useful shape.
     */
    public Duration peerRetryMinBackoff() {
        return peerRetryMinBackoff;
    }

    /** Ceiling on the per-peer skip window (default 2s). */
    public Duration peerRetryMaxBackoff() {
        return peerRetryMaxBackoff;
    }

    /** How long a scan waits for answers that may still arrive (default 10s). */
    public Duration scanTimeout() {
        return scanTimeout;
    }

    /** How long {@code close()} waits for the loop to terminate and requests to drain (default 5s). */
    public Duration shutdownAwaitTimeout() {
        return shutdownAwaitTimeout;
    }

    /** Lower bound of the randomised lock-retry window (default 20ms). */
    public Duration lockMinBackoff() {
        return lockMinBackoff;
    }

    /** Upper bound of the randomised lock-retry window (default 80ms). */
    public Duration lockMaxBackoff() {
        return lockMaxBackoff;
    }

    /** Lower bound of the randomised watch re-poll window (default 200ms). */
    public Duration watchMinBackoff() {
        return watchMinBackoff;
    }

    /** Upper bound of the randomised watch re-poll window (default 1000ms). */
    public Duration watchMaxBackoff() {
        return watchMaxBackoff;
    }

    /**
     * Fluent builder. Bounds within a min/max pair are validated in {@link #build()} rather than
     * in the setters, since either half can legitimately be set first.
     */
    public static final class Builder {

        private final DisCasClientConfig cfg = new DisCasClientConfig();

        private Builder() {
        }

        /** @see DisCasClientConfig#perAttemptTimeout() */
        public Builder perAttemptTimeout(final Duration value) {
            cfg.perAttemptTimeout = requirePositive(value, "perAttemptTimeout");
            return this;
        }

        /** @see DisCasClientConfig#requestDeadline() */
        public Builder requestDeadline(final Duration value) {
            cfg.requestDeadline = requirePositive(value, "requestDeadline");
            return this;
        }

        /** @see DisCasClientConfig#peerRetryMinBackoff() */
        public Builder peerRetryMinBackoff(final Duration value) {
            cfg.peerRetryMinBackoff = requirePositive(value, "peerRetryMinBackoff");
            return this;
        }

        /** @see DisCasClientConfig#peerRetryMaxBackoff() */
        public Builder peerRetryMaxBackoff(final Duration value) {
            cfg.peerRetryMaxBackoff = requirePositive(value, "peerRetryMaxBackoff");
            return this;
        }

        /** @see DisCasClientConfig#scanTimeout() */
        public Builder scanTimeout(final Duration value) {
            cfg.scanTimeout = requirePositive(value, "scanTimeout");
            return this;
        }

        /** @see DisCasClientConfig#shutdownAwaitTimeout() */
        public Builder shutdownAwaitTimeout(final Duration value) {
            cfg.shutdownAwaitTimeout = requirePositive(value, "shutdownAwaitTimeout");
            return this;
        }

        /** @see DisCasClientConfig#lockMinBackoff() */
        public Builder lockMinBackoff(final Duration value) {
            cfg.lockMinBackoff = requirePositive(value, "lockMinBackoff");
            return this;
        }

        /** @see DisCasClientConfig#lockMaxBackoff() */
        public Builder lockMaxBackoff(final Duration value) {
            cfg.lockMaxBackoff = requirePositive(value, "lockMaxBackoff");
            return this;
        }

        /** @see DisCasClientConfig#watchMinBackoff() */
        public Builder watchMinBackoff(final Duration value) {
            cfg.watchMinBackoff = requirePositive(value, "watchMinBackoff");
            return this;
        }

        /** @see DisCasClientConfig#watchMaxBackoff() */
        public Builder watchMaxBackoff(final Duration value) {
            cfg.watchMaxBackoff = requirePositive(value, "watchMaxBackoff");
            return this;
        }


        /**
         * Validates the min/max pairs and returns an immutable config that shares no state with
         * this builder, so the builder may be reused.
         */
        public DisCasClientConfig build() {
            // Checked here rather than in the setters: either half of a pair can legitimately be
            // set first, so a setter cannot know whether the pair is inconsistent yet.
            requireOrdered(cfg.lockMinBackoff, cfg.lockMaxBackoff, "lock");
            requireOrdered(cfg.watchMinBackoff, cfg.watchMaxBackoff, "watch");
            requireOrdered(cfg.peerRetryMinBackoff, cfg.peerRetryMaxBackoff, "peerRetry");
            // A deadline at or below one attempt's timeout would cut the first coordinator off
            // before its turn was up, turning every operation into an immediate failure.
            if (cfg.requestDeadline.compareTo(cfg.perAttemptTimeout) <= 0) {
                throw new IllegalArgumentException(
                        "requestDeadline (" + cfg.requestDeadline + ") must be > perAttemptTimeout ("
                                + cfg.perAttemptTimeout + ")");
            }
            return new DisCasClientConfig(cfg);
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

        private static void requireOrdered(final Duration min, final Duration max, final String name) {
            if (min.compareTo(max) > 0) {
                throw new IllegalArgumentException(
                        name + "MinBackoff (" + min + ") must be <= " + name + "MaxBackoff (" + max + ")");
            }
        }
    }
}
