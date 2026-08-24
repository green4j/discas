/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.client.ClientErrorCode;

/**
 * A node refused or failed an operation. Carries the {@link ClientErrorCode} the node returned
 * so callers can branch on <em>why</em>.
 * <p>
 * The message is the node's human-readable detail and may be rephrased at any time; branch on
 * {@link #code()}, never on {@link #getMessage()}.
 * <p>
 * Failures that never reach a node -- send failures, client shutdown, request timeouts -- are
 * still plain {@link RuntimeException}s: there is no node verdict to report.
 */
public final class DisCasOperationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final ClientErrorCode code;

    public DisCasOperationException(final ClientErrorCode code, final String message) {
        super(message);
        this.code = code == null ? ClientErrorCode.INTERNAL : code;
    }

    public ClientErrorCode code() {
        return code;
    }

    /**
     * Whether the caller caused this and retrying unchanged cannot help -- authorization refused
     * the operation, or the request was malformed.
     */
    public boolean isCallerError() {
        return code == ClientErrorCode.ACCESS_DENIED || code == ClientErrorCode.INVALID_ARGUMENT;
    }
}
