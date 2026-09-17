/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/**
 * Told about the changes a standing
 * {@link DisCasClient#watch(java.nio.ByteBuffer, Version, io.github.green4j.discas.common.client.ReadConsistency, WatchListener)}
 * observes.
 *
 * <p><b>Coalescing, not a change log</b>: a call carries the latest committed state as of a poll,
 * and changes that came and went between two polls are never seen (see {@link WatchResult}).
 *
 * <p>Called on the client's event loop, so an implementation must not block. It may close its
 * {@link KeyWatch} from inside a call.
 */
@FunctionalInterface
public interface WatchListener {

    /** The key advanced past the last version reported; {@link WatchResult#changed()} is true. */
    void changed(WatchResult result);

    /**
     * The watch has ended and nothing more is reported: the client was closed, the request was
     * refused as a caller error, or {@link #changed} threw. Not called for {@link KeyWatch#close()}.
     */
    default void failed(final Throwable cause) {
    }
}
