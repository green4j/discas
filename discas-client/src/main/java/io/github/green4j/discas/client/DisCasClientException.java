/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/**
 * Base for failures where the client never obtained a verdict from any node. Distinct from
 * {@link DisCasOperationException}, which reports a failure a node actually returned.
 * <p>
 * The concrete subtype says what kind of failure it is, and each carries its own reason enum:
 * <ul>
 *   <li>{@link ClientLifecycleException} -- this client is closing or closed; nothing will succeed
 *       on it again.</li>
 *   <li>{@link RequestFailedException} -- this attempt did not get an answer, but the client is
 *       still usable.</li>
 * </ul>
 * Branch on the type, never on {@link #getMessage()}.
 */
public abstract class DisCasClientException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    protected DisCasClientException(final String message) {
        super(message);
    }

    protected DisCasClientException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /** Whether the same call might succeed later on this client. */
    public abstract boolean isTransient();
}
