/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HttpRequestHeader}, driving it directly with a hand-built {@link ByteBuffer}
 * that stands in for a request's input buffer (name/value slices are added as {@code onHeader} would).
 */
@DisplayName("HttpRequestHeader - collect request headers and read them back by name")
class HttpRequestHeaderTest {

    // A byte buffer holding raw "name: value" style slices, with a helper to add each by locating its
    // substrings. The buffer is the single backing store every flyweight windows.
    private static final class Fixture {
        private final ByteBuffer buffer;
        private final String text;

        private Fixture(final String text) {
            this.text = text;
            this.buffer = ByteBuffer.wrap(text.getBytes(StandardCharsets.US_ASCII));
        }

        private void add(final HttpRequestHeader header, final String name, final String value) {
            final int nameOffset = text.indexOf(name);
            final int valueOffset = text.indexOf(value);
            header.add(buffer, nameOffset, name.length(), valueOffset, value.length());
        }
    }

    // Collects each flyweight's snapshot so we can assert on it after the sweep.
    private static List<String> collectStrings(final HttpRequestHeader h, final String name) {
        final List<String> out = new ArrayList<>();
        h.values(name, v -> out.add(v.toString()));
        return out;
    }

    @Test
    @DisplayName("Collects headers and looks up the first value by name")
    void firstValueByName() {
        final Fixture f = new Fixture("Host=example.com Accept=application/json ");
        final HttpRequestHeader h = new HttpRequestHeader();
        f.add(h, "Host", "example.com");
        f.add(h, "Accept", "application/json");

        assertEquals(2, h.size());

        final CharSequence host = h.firstValue("Host");
        assertEquals(11, host.length());
        assertEquals('e', host.charAt(0));
        assertEquals("example.com", host.toString());
        assertEquals("application/json", h.firstValue("Accept").toString());
    }

    @Test
    @DisplayName("Looks up by name case-insensitively, first match wins")
    void caseInsensitiveFirstMatch() {
        final Fixture f = new Fixture("Set-Cookie=a=1 Set-Cookie=b=2 Content-Type=text/plain ");
        final HttpRequestHeader h = new HttpRequestHeader();
        f.add(h, "Set-Cookie", "a=1");
        f.add(h, "Set-Cookie", "b=2");
        f.add(h, "Content-Type", "text/plain");

        assertTrue(h.contains("content-type"));
        assertTrue(h.contains("CONTENT-TYPE"));
        assertEquals("text/plain", h.firstValue("Content-Type").toString());

        // duplicates: firstValue/firstStringValue return the first occurrence
        assertEquals("a=1", h.firstValue("set-cookie").toString());
        assertEquals("a=1", h.firstStringValue("SET-COOKIE"));

        assertFalse(h.contains("Authorization"));
        assertNull(h.firstValue("Authorization"));
        assertNull(h.firstStringValue("Authorization"));
    }

    @Test
    @DisplayName("values/valuesString iterate every occurrence of a name, in order")
    void iteratesAllValues() {
        final Fixture f = new Fixture("Set-Cookie=a=1 Set-Cookie=b=2 Set-Cookie=c=3 Host=example.com ");
        final HttpRequestHeader h = new HttpRequestHeader();
        f.add(h, "Set-Cookie", "a=1");
        f.add(h, "Set-Cookie", "b=2");
        f.add(h, "Set-Cookie", "c=3");
        f.add(h, "Host", "example.com");

        final List<String> flyweights = new ArrayList<>();
        assertEquals(3, h.values("set-cookie", v -> flyweights.add(v.toString())));
        assertEquals(List.of("a=1", "b=2", "c=3"), flyweights);

        final List<String> strings = new ArrayList<>();
        assertEquals(3, h.valuesString("SET-COOKIE", strings::add));
        assertEquals(List.of("a=1", "b=2", "c=3"), strings);

        // single occurrence still reaches the consumer once
        final List<String> host = new ArrayList<>();
        assertEquals(1, h.values("Host", v -> host.add(v.toString())));
        assertEquals(List.of("example.com"), host);
    }

    @Test
    @DisplayName("No matching name reads return null / empty")
    void missingNameReturnsNull() {
        final HttpRequestHeader h = new HttpRequestHeader();
        assertEquals(0, h.size());
        assertFalse(h.contains("Host"));
        assertNull(h.firstValue("Host"));
        assertNull(h.firstStringValue("Host"));
        // A name that is not there never reaches the consumer, through either accessor.
        assertEquals(0, h.values("Host", v -> fail("Consumer must not be called for a missing name")));
        assertEquals(0, h.valuesString("Host", v -> fail("Consumer must not be called for a missing name")));
    }

    @Test
    @DisplayName("Reset clears count and caches; refilling re-points flyweights at new bytes")
    void resetAndReuse() {
        final HttpRequestHeader h = new HttpRequestHeader();

        final Fixture first = new Fixture("Host=one.example ");
        first.add(h, "Host", "one.example");
        assertEquals(1, h.size());
        assertEquals("one.example", h.firstValue("Host").toString());
        final String cachedFirst = h.firstStringValue("Host");
        assertSame(cachedFirst, h.firstStringValue("Host")); // cached, not re-allocated

        h.reset();
        assertEquals(0, h.size());
        assertNull(h.firstValue("Host"));

        final Fixture second = new Fixture("Host=two.example ");
        second.add(h, "Host", "two.example");
        assertEquals(1, h.size());
        assertEquals("two.example", h.firstValue("Host").toString());   // flyweight re-pointed
        assertEquals("two.example", h.firstStringValue("Host"));         // cache re-materialized
        assertEquals("one.example", cachedFirst);                        // old snapshot unaffected
    }

    @Test
    @DisplayName("Grows past initial capacity while preserving earlier entries")
    void growsPreservingEntries() {
        final StringBuilder sb = new StringBuilder();
        final int n = 10;
        for (int i = 0; i < n; i++) {
            sb.append("H").append(i).append('=').append("v").append(i).append(' ');
        }
        final Fixture f = new Fixture(sb.toString());
        final HttpRequestHeader h = new HttpRequestHeader(2); // force several growths
        for (int i = 0; i < n; i++) {
            f.add(h, "H" + i + "=", "v" + i); // include '=' in name token so offsets are unambiguous
        }
        assertEquals(n, h.size());
        for (int i = 0; i < n; i++) {
            assertEquals(List.of("v" + i), collectStrings(h, "H" + i + "="));
        }
    }
}
