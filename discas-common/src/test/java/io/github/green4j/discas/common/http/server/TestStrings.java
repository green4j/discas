/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import java.util.List;

/**
 * Counting in the HTTP server tests: a pipelined exchange is read as one string, so "how many
 * responses came back" is a substring count.
 */
final class TestStrings {

    private TestStrings() {
    }

    /** Occurrences of {@code needle} in {@code haystack}, non-overlapping. */
    static int count(final String haystack, final String needle) {
        int count = 0;
        for (int at = haystack.indexOf(needle); at >= 0; at = haystack.indexOf(needle, at + needle.length())) {
            count++;
        }
        return count;
    }

    /** Occurrences of {@code value} in an event log. */
    static int count(final List<String> events, final String value) {
        int count = 0;
        for (final String e : events) {
            if (e.equals(value)) {
                count++;
            }
        }
        return count;
    }
}
