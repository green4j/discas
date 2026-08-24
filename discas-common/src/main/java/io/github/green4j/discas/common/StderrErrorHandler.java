/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

/**
 * Prints event-loop failures to {@code System.err} and carries on. Reachable as
 * {@link EventLoop.ErrorHandler#STDERR}.
 */
public final class StderrErrorHandler implements EventLoop.ErrorHandler {

    @Override
    public void onError(final String context, final Throwable error) {
        System.err.println(context + " error: " + error.getMessage());
        error.printStackTrace();
    }
}
