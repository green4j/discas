/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.util.List;
import java.util.function.Consumer;

/**
 * Closing a list of resources on shutdown, in reverse acquisition order: resources are registered
 * as they are acquired, and a later one may depend on an earlier one. A failure is reported even
 * though shutdown continues regardless.
 */
public final class Closeables {

    private Closeables() {
    }

    /**
     * Close every resource in reverse acquisition order, continuing past failures.
     *
     * @param onFailure receives each failure; shutdown continues either way
     */
    public static void closeAll(final List<? extends AutoCloseable> resources,
                                final Consumer<Exception> onFailure) {
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (final Exception e) {
                onFailure.accept(e);
            }
        }
    }
}
