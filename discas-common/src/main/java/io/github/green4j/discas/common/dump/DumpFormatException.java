/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.dump;

import java.io.IOException;

/**
 * A dump file that is not one, or is not all there: wrong magic, an unknown format version, a
 * length that overruns what remains, a count or a CRC that does not match, or bytes past the
 * trailer.
 * <p>
 * An {@link IOException}, since that is what a reader's callers already handle, but a distinct
 * type because the two mean different things to an operator: "could not be read" is a disk or a
 * permission, "not a complete dump" is a backup that must not be restored from.
 */
public class DumpFormatException extends IOException {

    private static final long serialVersionUID = 1L;

    public DumpFormatException(final String message) {
        super(message);
    }

    public DumpFormatException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
