/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.time.Instant;

/**
 * Observability seam for the components that hold <b>externally-sourced material</b> -- files read
 * on demand, client-token stores, TLS certificate rotation.
 * <p>
 * These sit below both the node and the client, so neither {@code NodeObserver} nor
 * {@code ClientObserver} can reach them; they share this one instead. The vocabulary is small
 * because the situations are the same wherever the material comes from: a reload was applied, one
 * failed and the last good value was kept, a check threw, or material is running out and no
 * replacement has arrived.
 * <p>
 * Every one of these is a condition the process <b>survives</b> -- that is exactly why they need
 * reporting. A component that keeps serving its last good configuration looks healthy from the
 * outside while quietly running on stale material, and the operator finds out at the worst
 * possible moment (when the certificate finally expires, or when a node was removed from the
 * members file hours ago and nothing noticed).
 * <p>
 * All methods are {@code default} no-ops, so {@link #NONE} reports nothing and an implementation
 * overrides only what it cares about.
 * <p>
 * <b>Threading:</b> callbacks fire on whichever thread asked for the reload, or on the shared
 * periodic thread -- never on an event loop. Implementations must not block: the caller is waiting
 * for its answer, and the periodic thread serves every timed check in the process.
 */
public interface ReloadObserver {

    /** No-op observer: the default. Reports nothing, allocates nothing. */
    ReloadObserver NONE = new ReloadObserver() {
    };

    /**
     * {@code source} was applied, with a short human-readable {@code detail} of what it now holds.
     * The seam's only positive event, and the one an operator reads to confirm that what reached
     * the process is what they wrote. Fired for the initial load as well as for every later
     * revision -- a value in force that was never reported is a value nobody can check.
     * <p>
     * <b>{@code detail} is read by whoever can read the log, which is not always whoever may read
     * the file.</b> It says what an operator needs to recognise their own edit -- which clients,
     * which nodes, how many records, when they expire -- and never the material that makes the file
     * worth protecting: no token, no hash or salt, no key or password.
     */
    default void reloaded(final String source, final String detail) {
    }

    /**
     * {@code source} changed on disk but holds what it already held, so <b>nothing was applied</b>
     * and the value in force is untouched. Worth its own event rather than silence: an operator who
     * has just saved a file is asking whether it took effect, and "your edit changed nothing" is a
     * different answer from both "applied" and "did not parse" -- it usually means they edited a
     * copy, or that the change was cosmetic.
     */
    default void reloadUnchanged(final String source, final String detail) {
    }

    /**
     * A reload of {@code source} was refused because it did not parse; the last good value is
     * retained and the component keeps serving. The initial load is not reported here -- it throws
     * instead, since there is no last good value to fall back on.
     */
    default void reloadFailed(final String source, final Throwable error) {
    }

    /**
     * Something around {@code source} threw: a periodic check, or a consumer of a value that was
     * applied. Neither costs the value in force, and neither is the file's fault -- this is the
     * channel for a defect rather than for malformed material.
     */
    default void checkFailed(final String source, final Throwable error) {
    }

    /**
     * {@code source} expires at {@code expiresAt} and no replacement has arrived. Reported once,
     * ahead of the deadline, so there is time to act -- unlike every other event here, this one is
     * a warning about the future rather than a report of something that already happened.
     */
    default void materialExpiring(final String source, final Instant expiresAt) {
    }
}
