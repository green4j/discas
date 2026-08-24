/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.observability;

/**
 * The minimum JSON escaping the probe bodies need.
 * <p>
 * These bodies are fixed-shape objects of a few fields, so a JSON library (or even the agent's
 * builder, which lives in another module and is not visible here) would be more machinery than the
 * job warrants. What cannot be skipped is escaping: a node id or a WAL failure reason reaches these
 * bodies unfiltered, and an unescaped quote or backslash produces a response that a probe's JSON
 * parser rejects outright -- turning a readable "here is why I am unhealthy" into a parse error at
 * the worst possible moment.
 */
final class JsonText {

    private JsonText() {
    }

    /** Appends {@code ,"name":"value"} with {@code value} escaped. */
    static void field(final StringBuilder out, final String name, final String value) {
        out.append(",\"").append(name).append("\":\"");
        escape(out, value);
        out.append('"');
    }

    /** Appends {@code "value"} with {@code value} escaped, for use inside an array. */
    static void quoted(final StringBuilder out, final String value) {
        out.append('"');
        escape(out, value);
        out.append('"');
    }

    private static void escape(final StringBuilder out, final String value) {
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            switch (c) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        // Any other control character must be escaped as \ u00XX to stay valid JSON.
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                    break;
            }
        }
    }
}
