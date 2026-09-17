/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/** A standing watch started with a {@link WatchListener}. */
public interface KeyWatch extends AutoCloseable {

    /**
     * Stops the watch: no poll is sent and no listener call starts after this returns. Callable
     * from any thread, the listener included, and idempotent. The listener is not told.
     */
    @Override
    void close();
}
