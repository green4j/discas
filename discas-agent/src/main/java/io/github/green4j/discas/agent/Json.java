/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

/**
 * The agent's JSON writer. Every response body goes through here, so nothing reaches a client
 * without being escaped.
 *
 * <h2>Key naming</h2>
 * Two conventions coexist:
 * <ul>
 *   <li>The KV read shape is <b>Consul-compatible</b> and keeps Consul's names verbatim --
 *       {@code Key}, {@code Value}, {@code Flags}. A Consul client pointed at this agent is
 *       the reason the module exists, so these are protocol, not style.</li>
 *   <li>Everything discas-specific is <b>camelCase</b> and named after the client API's own
 *       accessor for the same thing: {@code swapped}, {@code value}, {@code ok}, {@code error},
 *       {@code status}, {@code clientId}, {@code nodes}, {@code owner}, {@code token},
 *       {@code generation}. A caller must not have to learn two names for one concept depending
 *       on which side of the agent it stands on.</li>
 * </ul>
 */
final class Json {

    private final StringBuilder out = new StringBuilder();
    private boolean first = true;

    private Json() {
    }

    /** Begin an object. */
    static Json object() {
        final Json json = new Json();
        json.out.append('{');
        return json;
    }

    /** A JSON array of strings, each escaped. */
    static String arrayOfStrings(final Iterable<String> values) {
        final StringBuilder sb = new StringBuilder().append('[');
        boolean first = true;
        for (final String value : values) {
            if (!first) {
                sb.append(',');
            }
            escape(sb, value);
            first = false;
        }
        return sb.append(']').toString();
    }

    /** A single escaped JSON string literal (double quotes included). */
    static String string(final String value) {
        final StringBuilder sb = new StringBuilder(value.length() + 2);
        escape(sb, value);
        return sb.toString();
    }

    /** A one-field {@code {"error": "..."}} body -- the shape every failure path returns. */
    static String error(final String message) {
        return object().field("error", message).end();
    }

    Json field(final String name, final String value) {
        if (value == null) {
            return nullField(name);
        }
        name(name);
        escape(out, value);
        return this;
    }

    Json field(final String name, final long value) {
        name(name);
        out.append(value);
        return this;
    }

    Json field(final String name, final boolean value) {
        name(name);
        out.append(value);
        return this;
    }

    private Json nullField(final String name) {
        name(name);
        out.append("null");
        return this;
    }

    /**
     * A field whose value is already JSON (a nested object or array built elsewhere). Never
     * pass caller-supplied text here -- use {@link #field(String, String)}, which escapes.
     */
    Json rawField(final String name, final String rawJson) {
        name(name);
        out.append(rawJson);
        return this;
    }

    /** Close the object and render it. */
    String end() {
        return out.append('}').toString();
    }

    private void name(final String name) {
        if (!first) {
            out.append(',');
        }
        first = false;
        escape(out, name);
        out.append(':');
    }

    /**
     * Append {@code value} as a JSON string literal. Escapes the six characters JSON names
     * plus every other control character; anything at or above {@code 0x20} is emitted as-is,
     * which is valid JSON and stays legible for the UTF-8 bodies the agent returns.
     */
    private static void escape(final StringBuilder sb, final String value) {
        sb.append('"');
        for (int i = 0, n = value.length(); i < n; i++) {
            final char c = value.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }
}
