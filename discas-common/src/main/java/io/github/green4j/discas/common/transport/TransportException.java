/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/**
 * Base for failures raised by the peer and client transports, which share one failure vocabulary.
 * <p>
 * The concrete subtype says <em>what kind</em> of failure it is, and each carries its own reason
 * enum naming the specific case. Callers branch on the type (and, when they care, the reason) --
 * never on {@link #getMessage()}, which is for humans and may be rephrased at any time.
 * <ul>
 *   <li>{@link TransportUnavailableException} -- the link cannot carry a send right now.</li>
 *   <li>{@link TransportOverloadedException} -- the link is up but a limit would be breached.</li>
 *   <li>{@link TransportSetupException} -- a configuration or lifecycle error that will not pass.</li>
 * </ul>
 */
public abstract class TransportException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    protected TransportException(final String message) {
        super(message);
    }

    protected TransportException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Whether a later attempt might succeed. True for an unavailable or overloaded link, false for
     * a setup fault, which needs the configuration or lifecycle corrected first.
     */
    public abstract boolean isTransient();
}
