/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.time.Instant;

/**
 * A {@link ReloadObserver} that writes every event to {@code System.err}. Standalone binaries opt
 * into it so their background-maintenance faults stay visible; the default
 * {@link ReloadObserver#NONE} stays silent, leaving an embedding application free to route the
 * same events into its own logging backend.
 */
public class StderrReloadObserver implements ReloadObserver {

    /** Shared instance; the observer is stateless. */
    public static final StderrReloadObserver INSTANCE = new StderrReloadObserver();

    @Override
    public void reloaded(final String source, final String detail) {
        System.err.println("Reloaded " + source + ": " + detail);
    }

    @Override
    public void reloadFailed(final String source, final Throwable error) {
        System.err.println("Ignoring failed reload of " + source + ": " + error.getMessage());
    }

    @Override
    public void watchUnavailable(final String source, final Throwable error) {
        System.err.println("File-watch: cannot watch " + source + ": " + error.getMessage());
    }

    @Override
    public void checkFailed(final String source, final Throwable error) {
        System.err.println("File-watch: check failed for " + source + ": " + error);
    }

    @Override
    public void materialExpiring(final String source, final Instant expiresAt) {
        System.err.println(source + " expires " + expiresAt
                + " and no renewed material has arrived -- check the issuer/renewal agent.");
    }
}
