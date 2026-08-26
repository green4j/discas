/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.io.PeriodicDaemon;

import io.github.green4j.discas.common.io.ReloadObserver;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A {@link ClientTokenStore} backed by an arbitrary {@code fetch} callback, re-invoked on a
 * fixed interval -- the integration point for a <b>secret manager</b> (Vault, AWS/GCP Secrets
 * Manager, ...). discas stays dependency-free: the caller supplies the vendor call that returns
 * a {@link ClientTokens} snapshot, and this store handles periodic refresh, change-gating
 * ({@code equals}), and replay-on-subscribe -- the same contract as the file-backed stores.
 * <p>
 * Periodic rather than on request, unlike the file-backed stores: a secret manager has to be asked
 * before it will answer, and nobody on this side can know when its answer changed. The tick runs on
 * the shared {@link PeriodicDaemon} (no new thread). A refresh is skipped if the previous one ran
 * less than {@code interval} ago, so it never fetches faster than requested even if the daemon
 * should ever run the check early.
 */
public final class RefreshingClientTokenStore implements ClientTokenStore, AutoCloseable {

    private final Supplier<ClientTokens> fetch;
    private final long intervalNanos;
    private final CopyOnWriteArrayList<Consumer<ClientTokens>> listeners = new CopyOnWriteArrayList<>();
    private final PeriodicDaemon.Registration registration;

    private final ReloadObserver observer;

    private volatile ClientTokens current;
    private long lastFetchNanos;

    public RefreshingClientTokenStore(final Supplier<ClientTokens> fetch, final Duration interval) {
        this(fetch, interval, ReloadObserver.NONE);
    }

    public RefreshingClientTokenStore(final Supplier<ClientTokens> fetch, final Duration interval,
                                      final ReloadObserver observer) {
        this.observer = observer == null ? ReloadObserver.NONE : observer;
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.fetch = fetch;
        this.intervalNanos = interval.toNanos();
        // Initial load = fail-fast: a bad first fetch propagates out of the constructor.
        this.current = fetch.get();
        if (current == null) {
            throw new IllegalStateException("Initial token fetch returned null");
        }
        this.lastFetchNanos = System.nanoTime();
        this.registration = PeriodicDaemon.shared().register(
                "client tokens", interval, this::refresh, this.observer);
    }

    @Override
    public ClientTokens snapshot() {
        return current;
    }

    @Override
    public synchronized void addListener(final Consumer<ClientTokens> listener) {
        listener.accept(current); // replay-on-subscribe
        listeners.add(listener);
    }

    /** Force an immediate refresh (bypasses the min-interval gate); mainly for tests/ops. */
    public void reloadNow() {
        refresh(true);
    }

    @Override
    public void close() {
        registration.close();
    }

    private void refresh() {
        refresh(false);
    }

    private synchronized void refresh(final boolean force) {
        final long now = System.nanoTime();
        if (!force && now - lastFetchNanos < intervalNanos) {
            return; // don't fetch faster than the requested interval
        }
        lastFetchNanos = now;
        final ClientTokens candidate;
        try {
            candidate = fetch.get();
        } catch (final Exception e) {
            observer.reloadFailed("client tokens", e);
            return; // keep the last good value
        }
        if (candidate == null || candidate.equals(current)) {
            // Silent: every interval fetches, so a report here would be a line in the log every
            // interval for ever. Nobody asked a question -- there is no file anyone can touch.
            return; // value gate: nothing new
        }
        current = candidate;
        for (final Consumer<ClientTokens> listener : listeners) {
            listener.accept(candidate);
        }
        observer.reloaded("client tokens", "applied -- " + candidate.summary());
    }
}
