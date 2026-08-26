/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.util.function.Consumer;

/**
 * A reloadable source of an immutable value {@code T}: read the current value once
 * via {@link #snapshot()}, and/or subscribe via {@link #addListener} to be handed the
 * current value immediately (<b>replay-on-subscribe</b>) and every subsequent change.
 * <p>
 * There is a single consumer path -- subscribe -- whether or not the source ever changes: a static
 * source fires the listener exactly once, a file-backed one fires on each reload that changes it.
 * So a consumer never needs to ask which kind it has, and every kind of file-backed material shares
 * {@link ReloadableFileSource}.
 */
public interface Reloadable<T> {

    /**
     * The current immutable value; never {@code null} after construction. Callers should
     * read this once into a local and operate on that value, not re-read it mid-operation.
     */
    T snapshot();

    /**
     * Register a listener handed the current value immediately (replay-on-subscribe,
     * atomically with reloads) and the new value on every later change. The listener runs on
     * whichever thread asked for the reload -- a consumer that touches single-threaded state must
     * marshal onto its own thread first.
     */
    void addListener(Consumer<T> listener);
}
