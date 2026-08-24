/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.time.Instant;

/**
 * Observability seam for the components that maintain <b>externally-sourced material in the
 * background</b> -- watched files, client-token stores, TLS certificate rotation.
 * <p>
 * These sit below both the node and the client, so neither {@code NodeObserver} nor
 * {@code ClientObserver} can reach them; they share this one instead. The vocabulary is small
 * because the situations are the same wherever the material comes from: a reload failed and the
 * last good value was kept, a watch could not be established, a scheduled check threw, or material
 * is running out and no replacement has arrived.
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
 * <b>Threading:</b> callbacks fire on the shared file-watch daemon thread or on whichever thread
 * triggered a reload -- never on an event loop. Implementations must still not block, since the
 * daemon thread services every watched file in the process.
 */
public interface ReloadObserver {

    /** No-op observer: the default. Reports nothing, allocates nothing. */
    ReloadObserver NONE = new ReloadObserver() {
    };

    /**
     * A background reload of {@code source} succeeded, with a short human-readable {@code detail}
     * of what it now holds. The seam's only positive event.
     */
    default void reloaded(final String source, final String detail) {
    }

    /**
     * A background reload of {@code source} failed; the last good value is retained and the
     * component keeps serving. The initial load is not reported here -- it throws instead, since
     * there is no last good value to fall back on.
     */
    default void reloadFailed(final String source, final Throwable error) {
    }

    /**
     * A filesystem watch could not be established for {@code source}, so changes are detected by
     * the safety poll alone. Not fatal, but reload latency degrades to the poll interval.
     */
    default void watchUnavailable(final String source, final Throwable error) {
    }

    /** A scheduled check for {@code source} threw. The registration stays; the next tick retries. */
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
