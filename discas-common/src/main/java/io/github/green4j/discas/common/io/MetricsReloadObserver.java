/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import io.github.green4j.discas.common.metrics.Counter;
import io.github.green4j.discas.common.metrics.MetricRegistry;

import java.time.Instant;

/**
 * Counts reload events into a {@link MetricRegistry}, then forwards them.
 * <p>
 * The {@code source} is not a label. It is a free-form description of what was reloaded (a file
 * path, a keystore name), so labelling by it would let an unbounded set of paths become an unbounded
 * set of time series -- and the operational question is "are reloads failing", not "which of the two
 * files failed", which the log record already answers.
 * <p>
 * {@code materialExpiring} is also exposed as a gauge holding the earliest known expiry as unix
 * seconds, because a counter cannot answer the question that matters: not <em>how many times</em>
 * something warned, but <em>how long is left</em>. An alert on
 * {@code discas_reload_material_expires_seconds - time() < threshold} is the useful form.
 */
public class MetricsReloadObserver extends DelegatingReloadObserver {

    private final Counter reloads;
    private final Counter reloadFailures;
    private final Counter watchUnavailable;
    private final Counter checkFailures;
    private final Counter expiryWarnings;

    /**
     * Earliest expiry seen, in unix seconds; {@code 0} until something warns. Written from the
     * file-watch daemon thread and read at scrape time, hence volatile.
     */
    private volatile long earliestExpirySeconds;

    public MetricsReloadObserver(final MetricRegistry registry, final ReloadObserver delegate) {
        super(delegate);
        reloads = registry.counter("discas_reloads_total",
                "Background reloads that succeeded.");
        reloadFailures = registry.counter("discas_reload_failures_total",
                "Background reloads that failed; the last good value was retained.");
        watchUnavailable = registry.counter("discas_reload_watch_unavailable_total",
                "Times a filesystem watch could not be established, degrading to the safety poll.");
        checkFailures = registry.counter("discas_reload_check_failures_total",
                "Scheduled reload checks that threw.");
        expiryWarnings = registry.counter("discas_reload_material_expiring_total",
                "Times watched material was reported as expiring with no replacement.");
        registry.gauge("discas_reload_material_expires_seconds",
                "Unix seconds at which watched material expires; 0 when nothing has warned.",
                () -> earliestExpirySeconds);
    }

    @Override
    public void reloaded(final String source, final String detail) {
        reloads.increment();
        super.reloaded(source, detail);
    }

    @Override
    public void reloadFailed(final String source, final Throwable error) {
        reloadFailures.increment();
        super.reloadFailed(source, error);
    }

    @Override
    public void watchUnavailable(final String source, final Throwable error) {
        watchUnavailable.increment();
        super.watchUnavailable(source, error);
    }

    @Override
    public void checkFailed(final String source, final Throwable error) {
        checkFailures.increment();
        super.checkFailed(source, error);
    }

    @Override
    public void materialExpiring(final String source, final Instant expiresAt) {
        expiryWarnings.increment();
        final long seconds = expiresAt == null ? 0L : expiresAt.getEpochSecond();
        // Keep the soonest deadline: with several watched sources, the one that expires first is
        // the one that takes the cluster down first, and is therefore the one worth alerting on.
        final long current = earliestExpirySeconds;
        if (seconds > 0L && (current == 0L || seconds < current)) {
            earliestExpirySeconds = seconds;
        }
        super.materialExpiring(source, expiresAt);
    }
}
