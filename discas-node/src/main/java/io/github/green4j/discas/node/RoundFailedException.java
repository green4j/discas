/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

/**
 * A consensus round gave up, carrying <em>why</em> as a {@link RoundFailure} rather than only as
 * message text.
 * <p>
 * The cause has to survive the trip from {@code Proposer} out through the round's future to
 * {@code ClientHandler}, which picks the client-facing error code from it. Recovering that by
 * matching on the message would be exactly the text-driven control flow this type exists to avoid.
 */
public final class RoundFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final RoundFailure failure;

    public RoundFailedException(final RoundFailure failure, final String message) {
        super(message);
        this.failure = failure;
    }

    public RoundFailure failure() {
        return failure;
    }
}
