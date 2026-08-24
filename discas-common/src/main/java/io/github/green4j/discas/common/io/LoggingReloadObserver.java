/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.operator.OperatorAttention;
import io.github.green4j.discas.common.operator.OperatorState;

import java.time.Instant;

/**
 * Writes reload events to a {@link Log}, then forwards them. A successful reload is a human-scale
 * fact and gets an {@code info} line; the rest are conditions, and go through
 * {@link OperatorAttention} so each carries what to do about it and shows as one sample while it
 * lasts. They are split by what the reader has to fix, not by how bad they sound:
 * <ul>
 *   <li>{@link #reloadFailed} is a <b>malformed file</b> -- one caught mid-write is retried
 *       silently and never reaches here -- so it stays raised until a reload of that source
 *       succeeds.</li>
 *   <li>{@link #watchUnavailable} is a <b>degraded but working</b> watch: the safety poll still
 *       picks changes up, just later. The action is the host's watch limits.</li>
 *   <li>{@link #checkFailed} is the check machinery <b>throwing</b>, a defect rather than a file
 *       problem, so it reports as one.</li>
 *   <li>{@link #materialExpiring} ends in peers refusing each other, and is an outage by the time
 *       it would be an error, so it is raised ahead of the deadline.</li>
 * </ul>
 */
public class LoggingReloadObserver extends DelegatingReloadObserver {

    private final Log log;
    private final OperatorAttention attention;

    public LoggingReloadObserver(final Log log, final OperatorAttention attention,
                                 final ReloadObserver delegate) {
        super(delegate);
        this.log = log;
        this.attention = attention;
    }

    /**
     * The one event that says a source is healthy again, so it clears both conditions it can be in
     * -- a rotation is a reload, so an expiring certificate needs nothing else to end it.
     */
    @Override
    public void reloaded(final String source, final String detail) {
        log.info("Reloaded " + source + ": " + detail);
        attention.clear(OperatorState.RELOAD_FAILED, source);
        attention.clear(OperatorState.MATERIAL_EXPIRING, source);
        super.reloaded(source, detail);
    }

    @Override
    public void reloadFailed(final String source, final Throwable error) {
        attention.raise(OperatorState.RELOAD_FAILED, source,
                "the file changed and did not parse; keeping the last good value", error);
        super.reloadFailed(source, error);
    }

    @Override
    public void watchUnavailable(final String source, final Throwable error) {
        attention.raise(OperatorState.RELOAD_NOT_WATCHED, source,
                "the filesystem watch is unavailable; the safety poll is carrying this source",
                error);
        super.watchUnavailable(source, error);
    }

    @Override
    public void checkFailed(final String source, final Throwable error) {
        attention.raise(OperatorState.UNHANDLED_ERROR, source,
                "the scheduled check threw; the next tick retries", error);
        super.checkFailed(source, error);
    }

    @Override
    public void materialExpiring(final String source, final Instant expiresAt) {
        attention.raise(OperatorState.MATERIAL_EXPIRING, source,
                "material expires at " + expiresAt + " and no replacement has arrived");
        super.materialExpiring(source, expiresAt);
    }
}
