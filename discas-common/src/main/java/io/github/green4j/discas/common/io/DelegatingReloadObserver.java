/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.time.Instant;

/**
 * Passes every reload event through to a wrapped observer, so a subclass can override the handful
 * it cares about and leave the rest alone -- and so logging and metrics can both be attached to one
 * reload seam.
 * <p>
 * Subclasses must call {@code super.x(...)} when overriding, or the rest of the chain stops seeing
 * that event.
 */
public abstract class DelegatingReloadObserver implements ReloadObserver {

    private final ReloadObserver delegate;

    protected DelegatingReloadObserver(final ReloadObserver delegate) {
        this.delegate = delegate == null ? ReloadObserver.NONE : delegate;
    }

    /** The next observer in the chain, for a subclass that needs to forward conditionally. */
    protected final ReloadObserver delegate() {
        return delegate;
    }

    @Override
    public void reloaded(final String source, final String detail) {
        delegate.reloaded(source, detail);
    }

    @Override
    public void reloadFailed(final String source, final Throwable error) {
        delegate.reloadFailed(source, error);
    }

    @Override
    public void watchUnavailable(final String source, final Throwable error) {
        delegate.watchUnavailable(source, error);
    }

    @Override
    public void checkFailed(final String source, final Throwable error) {
        delegate.checkFailed(source, error);
    }

    @Override
    public void materialExpiring(final String source, final Instant expiresAt) {
        delegate.materialExpiring(source, expiresAt);
    }
}
