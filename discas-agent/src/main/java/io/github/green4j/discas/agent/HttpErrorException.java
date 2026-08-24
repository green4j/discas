/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

/**
 * A handler-thrown signal that the current request must fail with a specific HTTP error status and
 * reason. Validation and parse helpers throw it (e.g. a required-but-absent or malformed query
 * parameter) instead of making every caller null-check and render the error itself;
 * {@link AbstractHandler#onRequestComplete} catches it once per request and commits the
 * {@code status}/{@code reason} as a JSON error envelope.
 *
 * <p>The status must be a real HTTP error code ({@code >= 400}), validated in the constructor: an
 * "error" exception carrying an OK/redirect status would commit a misleading reply, so building one
 * that way is a programming error rather than a runtime client error.
 *
 * <p>Checked, so the request-handling methods that can raise it must declare it and the compiler
 * ensures {@link AbstractHandler#onRequestComplete} keeps catching it. It carries no
 * stack trace (a control-flow signal, thrown per bad request), so it is cheap to raise and catch.
 */
final class HttpErrorException extends Exception {

    private static final long serialVersionUID = 1L;

    private final int status;

    HttpErrorException(final int status, final String reason) {
        super(reason, null, false, false); // control-flow signal: no suppression, no stack trace
        if (status < 400) {
            throw new IllegalArgumentException(
                    "HttpErrorException status must be an HTTP error code (>= 400), got " + status);
        }
        this.status = status;
    }

    int status() {
        return status;
    }

    String reason() {
        return getMessage();
    }
}
