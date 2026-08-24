/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link QueryPathParser}, driving it directly with request-target Strings (as
 * {@link HttpServer.HttpRequest#pathString()} would supply) and asserting the decoded path and query
 * parameters read back.
 */
@DisplayName("QueryPathParser - split a request target into decoded path and query parameters")
class QueryPathParserTest {

    private static List<String> collectStrings(final QueryPathParser p, final String name) {
        final List<String> out = new ArrayList<>();
        p.valuesString(name, out::add);
        return out;
    }

    @Test
    @DisplayName("Extracts the path and reads query parameters by name")
    void pathAndParamsByName() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/api/search?q=hello&limit=20");

        assertEquals("/api/search", p.pathString());
        assertEquals(2, p.size());
        assertEquals("hello", p.firstValue("q").toString());
        assertEquals("20", p.firstStringValue("limit"));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {
        "no query at all      | /api/health  | /api/health",
        "a bare question mark | /api/health? | /api/health",
        "an empty source      |              | ",
    }, delimiter = '|', emptyValue = "")
    @DisplayName("A target with no parameters yields its path, zero parameters and no lookups")
    void noParameters(final String name, final String target, final String expectedPath) {
        final QueryPathParser p = new QueryPathParser();
        p.parse(target == null ? "" : target.trim());

        assertEquals(expectedPath == null ? "" : expectedPath.trim(), p.pathString());
        assertEquals(0, p.size());
        assertFalse(p.contains("anything"));
        assertNull(p.firstValue("x"));
        assertNull(p.firstStringValue("x"));
        assertEquals(0, p.values("x", v -> fail("Consumer must not be called for a missing name")));
        assertEquals(0, p.valuesString("x", v -> fail("Consumer must not be called for a missing name")));
    }

    @Test
    @DisplayName("Parameters are readable by index in parse order")
    void paramsByIndex() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/p?a=1&b=2&c=3");

        assertEquals(3, p.size());
        assertEquals("a", p.nameString(0));
        assertEquals("1", p.valueString(0));
        assertEquals("b", p.name(1).toString());
        assertEquals("2", p.value(1).toString());
        assertEquals("c", p.nameString(2));
        assertEquals("3", p.valueString(2));
        assertNull(p.name(3));
        assertNull(p.value(-1));
    }

    @Test
    @DisplayName("Duplicate names: firstValue returns the first; values/valuesString iterate all in order")
    void duplicateNames() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/p?tag=a&tag=b&tag=c&x=y");

        assertEquals("a", p.firstStringValue("tag"));

        final List<String> flyweights = new ArrayList<>();
        assertEquals(3, p.values("tag", v -> flyweights.add(v.toString())));
        assertEquals(List.of("a", "b", "c"), flyweights);

        assertEquals(List.of("a", "b", "c"), collectStrings(p, "tag"));
        assertEquals(List.of("y"), collectStrings(p, "x"));
    }

    @Test
    @DisplayName("Percent- and plus-decoding: query decodes %XX and '+', path decodes %XX but keeps '+'")
    void decoding() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/a+b/c%2Fd?q=hello+world&e=a%3Db%26c");

        assertEquals("/a+b/c/d", p.pathString()); // '+' literal in path, %2F -> '/'
        assertEquals("hello world", p.firstStringValue("q")); // '+' -> space
        assertEquals("a=b&c", p.firstStringValue("e")); // %3D -> '=', %26 -> '&'

        // Multi-byte escapes go through the same decoder, one continuation byte at a time.
        p.parse("/p?name=caf%C3%A9&emoji=%F0%9F%98%80");
        assertEquals("caf\u00e9", p.firstStringValue("name"));       // %C3%A9 -> e-acute
        assertEquals("\uD83D\uDE00", p.firstStringValue("emoji"));  // %F0%9F%98%80 -> grinning face
    }

    @Test
    @DisplayName("Name lookup is case-sensitive")
    void caseSensitiveNames() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/p?Tag=a&tag=b");

        assertTrue(p.contains("Tag"));
        assertTrue(p.contains("tag"));
        assertEquals("a", p.firstStringValue("Tag"));
        assertEquals("b", p.firstStringValue("tag"));
        assertFalse(p.contains("TAG"));
        assertNull(p.firstValue("TAG"));
    }

    @Test
    @DisplayName("A parameter with no '=' has an empty value; an explicit '=' with nothing after is empty")
    void flagAndEmptyValue() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/p?flag&a=");

        assertEquals(2, p.size());
        assertTrue(p.contains("flag"));
        assertEquals("", p.firstStringValue("flag"));
        assertTrue(p.contains("a"));
        assertEquals("", p.firstStringValue("a"));
    }

    @Test
    @DisplayName("Empty parameters from '&&' and a trailing '&' are skipped")
    void emptyParamsSkipped() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/p?a=1&&b=2&");

        assertEquals(2, p.size());
        assertEquals("1", p.firstStringValue("a"));
        assertEquals("2", p.firstStringValue("b"));
    }

    @Test
    @DisplayName("A missing name returns null / 0 and never calls the consumer, parameters or not")
    void missingName() {
        final QueryPathParser p = new QueryPathParser();
        p.parse("/p?a=1");

        assertNull(p.firstValue("z"));
        assertEquals(0, p.values("z", v -> fail("Consumer must not be called for a missing name")));
    }

    @Test
    @DisplayName("Re-parse reuses the instance; earlier String snapshots are unaffected")
    void reparseReuse() {
        final QueryPathParser p = new QueryPathParser();

        p.parse("/one?x=first");
        assertEquals("/one", p.pathString());
        assertEquals("first", p.firstValue("x").toString());
        final String firstSnapshot = p.firstStringValue("x");
        // Materialized once and cached: the gc-free contract is that a second read allocates nothing.
        assertSame(firstSnapshot, p.firstStringValue("x"));
        assertSame(p.pathString(), p.pathString());

        p.parse("/two?x=second");
        assertEquals("/two", p.pathString());               // flyweight re-pointed
        assertEquals("second", p.firstValue("x").toString());
        assertEquals("second", p.firstStringValue("x"));     // cache re-materialized
        assertEquals("first", firstSnapshot);                // old snapshot intact
    }

    @Test
    @DisplayName("Grows past a small initial capacity, preserving earlier parameters")
    void growsPreservingParams() {
        final int n = 10;
        final StringBuilder sb = new StringBuilder("/p?");
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append('&');
            }
            sb.append('k').append(i).append('=').append('v').append(i);
        }
        final QueryPathParser p = new QueryPathParser(2); // force several growths
        p.parse(sb.toString());

        assertEquals(n, p.size());
        for (int i = 0; i < n; i++) {
            assertEquals("v" + i, p.firstStringValue("k" + i));
        }
    }
}
