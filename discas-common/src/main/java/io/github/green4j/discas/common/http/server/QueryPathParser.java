/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;

import java.util.function.Consumer;

/**
 * A reusable, allocation-lean parser that splits a request target (typically {@link HttpRequest#pathString()})
 * into its <em>path</em> part and its <em>query parameters</em>, percent-decoding both. A handler holds
 * one instance per connection, {@link #parse(CharSequence) parses} once per request, then reads back the
 * decoded {@link #path() path} and the query parameters by {@link #firstValue(CharSequence) name} (with
 * duplicates) or by {@link #name(int) index}. Because a query name may repeat, a name lookup comes in a
 * first-match flavor ({@link #firstValue firstValue}/{@link #firstStringValue firstStringValue}) and an
 * iterate-all flavor ({@link #values values}/{@link #valuesString valuesString}).
 *
 * <h2>Escaping (standard HTTP form rules)</h2>
 * The <b>path</b> is the source up to the first {@code '?'} (or the whole source if there is none);
 * {@code %XX} escapes are decoded, but {@code '+'} stays a literal {@code '+'}. The <b>query</b> is what
 * follows the first {@code '?'}, split on {@code '&'} into parameters, each split on {@code '='} into a
 * name and value; in names and values {@code %XX} is decoded <em>and</em> {@code '+'} becomes a space
 * ({@code application/x-www-form-urlencoded}). Decoded byte runs are interpreted as UTF-8 (so
 * {@code %C3%A9} yields {@code U+00E9}); malformed or truncated escapes yield the replacement char
 * {@code U+FFFD}. Query names are compared <b>case-sensitively</b> (unlike HTTP header names). A
 * parameter with no {@code '='} ({@code ?flag}) has an empty value; an empty parameter ({@code &&}) is
 * skipped.
 *
 * <h2>Zero allocation</h2>
 * The single parse pass decodes the path and every parameter name/value into one reusable {@code char[]}
 * buffer; each is held by a reusable {@link CharSequence} flyweight windowing that buffer, so
 * {@link #path path}, {@link #firstValue firstValue} and {@link #values values} reads allocate nothing.
 * A caller that needs a real {@link String} uses {@link #pathString pathString}/
 * {@link #firstStringValue firstStringValue}/{@link #valueString valueString}/
 * {@link #valuesString valuesString}, which materialize and cache each value once per parse (unlike
 * {@code firstValue(...).toString()}, which allocates a fresh String each call). The only steady-state
 * allocations are those opt-in Strings; the decode buffer grows only when a request target is longer
 * than any parsed before, and the parameter array grows (doubling, amortized) only when a request
 * carries more parameters than the current capacity.
 *
 * <h2>Lifetime</h2>
 * <b>Do not retain</b> a flyweight (or a {@code CharSequence} from {@code path}/{@code firstValue}/
 * {@code values}) beyond the {@code parse(...)} that produced it: the flyweights are re-pointed on the
 * next {@code parse}. Unlike {@link HttpRequestHeader}, the flyweights window the parser's <em>own</em>
 * decode buffer (a private copy), not the live request input buffer, so a raw flyweight read stays valid
 * for the whole request even across a multi-read body -- no snapshot is required to read it late.
 *
 * <p>Not thread-safe; a connection lives on one worker thread, so no synchronization is needed.
 *
 * <p>Hold one per connection, parse the request target once, then read the path and parameters:
 * <pre>{@code
 * private final QueryPathParser query = new QueryPathParser();   // per-connection field
 *
 * public void onRequestComplete(Connection c, HttpRequest r) {
 *     query.parse(r.pathString());                 // e.g. "/search?q=hello+world&tag=a&tag=b"
 *     String path = query.pathString();            // "/search"  ('+' is literal in the path)
 *     String q = query.firstStringValue("q");      // "hello world"  ('+' -> space in the query)
 *     boolean raw = query.contains("raw");         // presence flag (?raw)
 *     query.values("tag", v -> collect(v));        // iterate repeats: "a", then "b"
 * }
 * }</pre>
 */
public final class QueryPathParser {
    private static final int DEFAULT_PARAMS_CAPACITY = 8;
    private static final int DEFAULT_DECODE_CAPACITY = 64;
    private static final char REPLACEMENT = '\uFFFD';

    private final CharArraySeq pathView = new CharArraySeq();
    private CharArraySeq[] nameValues;    // 2 per parameter: [name] at i<<1, [value] at (i<<1)+1
    private char[] decode = new char[DEFAULT_DECODE_CAPACITY]; // reusable decode buffer; flyweights window it
    private int decodeLen;                // bytes used in 'decode' during the current parse
    private int count;                    // number of parsed parameters

    /** A parser with a default initial parameter capacity. */
    public QueryPathParser() {
        this(DEFAULT_PARAMS_CAPACITY);
    }

    /**
     * A parser pre-sized for {@code initialCapacity} parameters. The capacity grows automatically if a
     * request carries more; sizing it to the expected count avoids that growth.
     */
    public QueryPathParser(final int initialCapacity) {
        final int cap = Math.max(1, initialCapacity);
        nameValues = new CharArraySeq[cap << 1];
        for (int i = 0; i < nameValues.length; i++) {
            nameValues[i] = new CharArraySeq();
        }
    }

    /**
     * Parse {@code source} (typically {@link HttpRequest#pathString()}) into its path and query
     * parameters, decoding both per the class-level rules. Clears any previous result and re-points the
     * flyweights, so a single parser instance is reused across all requests on a connection.
     */
    public void parse(final CharSequence source) {
        count = 0;
        decodeLen = 0;
        final int srcLen = source == null ? 0 : source.length();
        if (decode.length < srcLen) {
            decode = new char[srcLen]; // decoded length is always <= source length
        }
        if (srcLen == 0) {
            pathView.set(decode, 0, 0);
            return;
        }

        // Path: [0, '?'), %XX decoded, '+' literal.
        final int pathStart = decodeLen;
        int i = 0;
        while (i < srcLen) {
            final char c = source.charAt(i);
            if (c == '?') {
                break;
            }
            if (c == '%') {
                i = decodePercentRun(source, i, srcLen, false);
            } else {
                append(c);
                i++;
            }
        }
        pathView.set(decode, pathStart, decodeLen - pathStart);

        if (i >= srcLen) {
            return; // no query
        }
        i++; // past '?'

        // Query: '&'-separated params, each '='-separated into name/value; %XX and '+' decoded.
        while (i <= srcLen) {
            final int nameStart = decodeLen;
            // name up to '=', '&', or end
            while (i < srcLen) {
                final char c = source.charAt(i);
                if (c == '=' || c == '&') {
                    break;
                }
                i = appendQueryChar(source, i, srcLen, c);
            }
            final int nameEnd = decodeLen;

            int valueStart = decodeLen;
            int valueEnd = decodeLen;
            if (i < srcLen && source.charAt(i) == '=') {
                i++; // past '='
                valueStart = decodeLen;
                while (i < srcLen) {
                    final char c = source.charAt(i);
                    if (c == '&') {
                        break;
                    }
                    i = appendQueryChar(source, i, srcLen, c);
                }
                valueEnd = decodeLen;
            }

            // Skip a wholly empty parameter (e.g. from "&&" or a trailing '&').
            if (nameEnd != nameStart || valueEnd != valueStart) {
                addParam(nameStart, nameEnd - nameStart, valueStart, valueEnd - valueStart);
            }

            if (i < srcLen && source.charAt(i) == '&') {
                i++; // past '&', parse the next parameter
                continue;
            }
            break;
        }
    }

    /** The decoded path part as a reusable flyweight (empty if the source was empty). */
    public CharSequence path() {
        return pathView;
    }

    /** The decoded path part as a real {@link String}, lazily materialized and cached for this parse. */
    public String pathString() {
        return pathView.stringValue();
    }

    /** The number of parsed query parameters (each occurrence counts, so duplicates are included). */
    public int size() {
        return count;
    }

    /** The decoded name of the parameter at {@code index} as a reusable flyweight, or {@code null}. */
    public CharSequence name(final int index) {
        return index < 0 || index >= count ? null : nameValues[index << 1];
    }

    /** The decoded name of the parameter at {@code index} as a real {@link String}, or {@code null}. */
    public String nameString(final int index) {
        return index < 0 || index >= count ? null : nameValues[index << 1].stringValue();
    }

    /** The decoded value of the parameter at {@code index} as a reusable flyweight, or {@code null}. */
    public CharSequence value(final int index) {
        return index < 0 || index >= count ? null : nameValues[(index << 1) + 1];
    }

    /** The decoded value of the parameter at {@code index} as a real {@link String}, or {@code null}. */
    public String valueString(final int index) {
        return index < 0 || index >= count ? null : nameValues[(index << 1) + 1].stringValue();
    }

    /** Whether a query parameter named {@code name} (case-sensitive) was parsed. */
    public boolean contains(final CharSequence name) {
        return firstIndexOf(name) >= 0;
    }

    /**
     * The value of the first parameter named {@code name} (case-sensitive) as a reusable flyweight, or
     * {@code null} if there is no such parameter.
     */
    public CharSequence firstValue(final CharSequence name) {
        final int index = firstIndexOf(name);
        return index < 0 ? null : nameValues[(index << 1) + 1];
    }

    /**
     * The value of the first parameter named {@code name} (case-sensitive) as a real {@link String}, or
     * {@code null} if there is no such parameter. Lazily materialized and cached for the current parse.
     */
    public String firstStringValue(final CharSequence name) {
        final int index = firstIndexOf(name);
        return index < 0 ? null : nameValues[(index << 1) + 1].stringValue();
    }

    /**
     * Invokes {@code consumer} once, in parse order, with the reusable value flyweight of every
     * parameter named {@code name} (case-sensitive); returns the number of matches (0 leaves
     * {@code consumer} uncalled). The flyweight passed is valid only for the duration of the callback.
     */
    public int values(final CharSequence name, final Consumer<CharSequence> consumer) {
        int matched = 0;
        for (int i = 0; i < count; i++) {
            if (nameValues[i << 1].equalsChars(name)) {
                consumer.accept(nameValues[(i << 1) + 1]);
                matched++;
            }
        }
        return matched;
    }

    /**
     * Like {@link #values(CharSequence, Consumer)}, but passes each value as a real {@link String},
     * lazily materialized and cached for the current parse.
     */
    public int valuesString(final CharSequence name, final Consumer<String> consumer) {
        int matched = 0;
        for (int i = 0; i < count; i++) {
            if (nameValues[i << 1].equalsChars(name)) {
                consumer.accept(nameValues[(i << 1) + 1].stringValue());
                matched++;
            }
        }
        return matched;
    }

    private int firstIndexOf(final CharSequence name) {
        for (int i = 0; i < count; i++) {
            if (nameValues[i << 1].equalsChars(name)) {
                return i;
            }
        }
        return -1;
    }

    private void addParam(final int nameOff, final int nameLen, final int valueOff, final int valueLen) {
        if ((count << 1) == nameValues.length) {
            makeRoom();
        }
        nameValues[count << 1].set(decode, nameOff, nameLen);
        nameValues[(count << 1) + 1].set(decode, valueOff, valueLen);
        count++;
    }

    private void makeRoom() {
        final int oldLen = nameValues.length;
        final int newLen = oldLen << 1;
        final CharArraySeq[] newSeqs = new CharArraySeq[newLen];
        System.arraycopy(nameValues, 0, newSeqs, 0, oldLen);
        for (int i = oldLen; i < newLen; i++) {
            newSeqs[i] = new CharArraySeq();
        }
        nameValues = newSeqs;
    }

    private void append(final char c) {
        decode[decodeLen++] = c;
    }

    // Decode one query character at 'i' ('+' -> space, '%XX' -> UTF-8, else literal), append it, and
    // return the next source index to read.
    private int appendQueryChar(final CharSequence source, final int i, final int srcLen, final char c) {
        if (c == '+') {
            append(' ');
            return i + 1;
        }
        if (c == '%') {
            return decodePercentRun(source, i, srcLen, true);
        }
        append(c);
        return i + 1;
    }

    // Decode a maximal run of "%XX" escapes starting at 'start' (which points at a '%') as UTF-8 bytes,
    // appending the resulting char(s), and return the source index just past the run. In the query, a
    // '+' interleaved in the run is a space byte (0x20). A malformed/truncated escape appends U+FFFD.
    private int decodePercentRun(final CharSequence source, final int start, final int srcLen,
                                 final boolean plusIsSpace) {
        int i = start;
        int codePoint = 0;
        int remaining = 0; // continuation bytes still expected for the in-progress code point
        while (i < srcLen) {
            final char c = source.charAt(i);
            final int b;
            if (c == '%') {
                if (i + 2 >= srcLen) {
                    flushIncomplete(remaining);
                    append(REPLACEMENT);
                    return srcLen;
                }
                final int hi = hexValue(source.charAt(i + 1));
                final int lo = hexValue(source.charAt(i + 2));
                if (hi < 0 || lo < 0) {
                    flushIncomplete(remaining);
                    append(REPLACEMENT);
                    return i + 1; // resync past just the '%'; reprocess the rest as literals
                }
                b = (hi << 4) | lo;
                i += 3;
            } else if (plusIsSpace && c == '+') {
                b = ' ';
                i++;
            } else {
                break; // a literal (non-escaped) char ends the byte run
            }

            if (remaining > 0) {
                if ((b & 0xC0) == 0x80) {
                    codePoint = (codePoint << 6) | (b & 0x3F);
                    if (--remaining == 0) {
                        appendCodePoint(codePoint);
                    }
                    continue;
                }
                append(REPLACEMENT); // expected a continuation byte, got something else
                remaining = 0;
            }
            if ((b & 0x80) == 0) {
                append((char) b); // ASCII byte
            } else if ((b & 0xE0) == 0xC0) {
                codePoint = b & 0x1F;
                remaining = 1;
            } else if ((b & 0xF0) == 0xE0) {
                codePoint = b & 0x0F;
                remaining = 2;
            } else if ((b & 0xF8) == 0xF0) {
                codePoint = b & 0x07;
                remaining = 3;
            } else {
                append(REPLACEMENT); // invalid leading byte
            }
        }
        flushIncomplete(remaining);
        return i;
    }

    private void flushIncomplete(final int remaining) {
        if (remaining > 0) {
            append(REPLACEMENT); // a multi-byte sequence ran out of continuation bytes
        }
    }

    private void appendCodePoint(final int codePoint) {
        if (codePoint > 0x10FFFF || (codePoint >= 0xD800 && codePoint <= 0xDFFF)) {
            append(REPLACEMENT);
        } else if (codePoint <= 0xFFFF) {
            append((char) codePoint);
        } else {
            final int cp = codePoint - 0x10000;
            append((char) (0xD800 + (cp >> 10)));
            append((char) (0xDC00 + (cp & 0x3FF)));
        }
    }

    private static int hexValue(final char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        return -1;
    }

    /**
     * A reusable {@link CharSequence} flyweight over a window {@code [off, off+len)} of the parser's
     * decode {@code char[]}. Zero-allocation to read; {@link #toString()} / {@link #subSequence} are the
     * opt-in allocations for a caller that wants a real {@code String}. The {@code char[]} analog of
     * {@code ParametrizedRouter}'s {@code PathCharSequence}.
     */
    private static final class CharArraySeq implements CharSequence {
        private char[] base = new char[0];
        private int off;
        private int len;
        private String cached;

        private void set(final char[] base, final int off, final int len) {
            this.base = base;
            this.off = off;
            this.len = len;
            this.cached = null;
        }

        private String stringValue() {
            if (cached == null) {
                cached = new String(base, off, len);
            }
            return cached;
        }

        private boolean equalsChars(final CharSequence other) {
            if (other == null || other.length() != len) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                if (base[off + i] != other.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(final int index) {
            if (index < 0 || index >= len) {
                throw new IndexOutOfBoundsException("index " + index + ", length " + len);
            }
            return base[off + index];
        }

        @Override
        public CharSequence subSequence(final int start, final int end) {
            if (start < 0 || end > len || start > end) {
                throw new IndexOutOfBoundsException("[" + start + ", " + end + "), length " + len);
            }
            return stringValue().substring(start, end);
        }

        @Override
        public String toString() {
            return stringValue();
        }
    }
}
