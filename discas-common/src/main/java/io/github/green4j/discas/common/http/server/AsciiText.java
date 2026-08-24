/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

/**
 * ASCII case folding for the HTTP text the parser compares byte by byte -- method names,
 * header names, header tokens.
 * <p>
 * Deliberately not {@link Character#toLowerCase(int)}: that applies Unicode case mapping, so a
 * non-ASCII byte in a header name would fold onto some other codepoint and compare equal to
 * something it is not. HTTP field names are ASCII by definition (RFC 9110), so folding is the
 * {@code +32} range shift and nothing else. Both idioms were in use here; this is the one that
 * is correct for the input.
 */
final class AsciiText {

    private AsciiText() {
    }

    /** Fold one ASCII byte value to lowercase, leaving every other value untouched. */
    static int toLower(final int value) {
        return (value >= 'A' && value <= 'Z') ? value + 32 : value;
    }
}
