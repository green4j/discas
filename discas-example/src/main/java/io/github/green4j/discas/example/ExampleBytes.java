/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * The encode/decode pair every example uses for its keys and values. UTF-8 explicitly, so a
 * platform whose default charset is something else still round-trips the same bytes as the rest of
 * the system.
 */
final class ExampleBytes {
    private ExampleBytes() {
    }

    static ByteBuffer encode(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    static String decode(final ByteBuffer value) {
        if (value == null) {
            return "null";
        }
        final byte[] bytes = new byte[value.remaining()];
        value.duplicate().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
