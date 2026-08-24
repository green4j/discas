/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * A reusable, allocation-lean collector for a request's HTTP headers, read back by (case-insensitive)
 * name. A handler holds one instance per connection, {@link #reset() resets} it at the start of each
 * request, feeds it every header from {@link ConnectionHandler#onHeader onHeader} via
 * {@link #add(ByteBuffer, int, int, int, int) add}, then looks headers up by name. Because HTTP allows
 * a header name to repeat, a name lookup comes in a first-match flavor ({@link #firstValue firstValue}/
 * {@link #firstStringValue firstStringValue}) and an iterate-all flavor ({@link #values values}/
 * {@link #valuesString valuesString}).
 *
 * <h2>Zero allocation</h2>
 * Header names and values are captured as offset+length windows into the request's input
 * {@link ByteBuffer} (the same buffer every header of a request is delivered on), each held directly
 * by a reusable ASCII {@link CharSequence} flyweight; so {@link #firstValue firstValue} and
 * {@link #values values} reads allocate nothing. A caller that needs a real {@link String} uses
 * {@link #firstStringValue firstStringValue}/{@link #valuesString valuesString}, which materialize and
 * cache each value once per request (unlike {@code firstValue(...).toString()}, which allocates a fresh
 * String each call). The only steady-state allocations are those opt-in Strings; the backing array
 * grows (doubling, amortized) only when a request carries more headers than the current capacity.
 *
 * <h2>Lifetime</h2>
 * <b>Do not retain</b> a flyweight from {@code firstValue}/{@code values} beyond the request that
 * produced it: the flyweights are re-pointed on the next request. Moreover, because they window the
 * <em>live</em> input buffer (a byte queue that is compacted once a request body arrives in a later
 * read), a raw flyweight read is only reliably valid while those header bytes are intact -- i.e. through
 * {@link ConnectionHandler#onHeadersComplete onHeadersComplete}. To read a header later (e.g. in
 * {@link ConnectionHandler#onRequestComplete onRequestComplete} after a multi-read body), snapshot it
 * with {@link #firstStringValue}/{@link #valuesString} during the header phase.
 *
 * <p>Not thread-safe; a connection lives on one worker thread, so no synchronization is needed.
 *
 * <p>Hold one per connection, {@code reset()} it per request, feed every {@code onHeader}, then look
 * up by name (case-insensitive):
 * <pre>{@code
 * private final HttpRequestHeader headers = new HttpRequestHeader();   // per-connection field
 *
 * public void onRequestLine(Connection c, HttpRequest r) { headers.reset(); }
 *
 * public void onHeader(Connection c, HttpRequest r, ByteBuffer buf,
 *                      int nameOff, int nameLen, int valOff, int valLen) {
 *     headers.add(buf, nameOff, nameLen, valOff, valLen);
 * }
 *
 * public void onHeadersComplete(Connection c, HttpRequest r) {
 *     if (!headers.contains("Authorization")) { c.beginResponse().error(401); return; }
 *     String ct = headers.firstStringValue("Content-Type");   // snapshot to read after the body
 *     headers.values("Cookie", v -> parseCookie(v));          // iterate repeated headers
 * }
 * }</pre>
 */
public final class HttpRequestHeader {
    private static final int DEFAULT_CAPACITY = 16;

    private ByteBufferCharSequence[] nameValues;   // 2 per header: [name] at i<<1, [value] at (i<<1)+1
    private int count;

    /** A collector with a default initial capacity. */
    public HttpRequestHeader() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * A collector pre-sized for {@code initialCapacity} headers. The capacity grows automatically if a
     * request carries more; sizing it to the expected header count avoids that growth.
     */
    public HttpRequestHeader(final int initialCapacity) {
        final int cap = Math.max(1, initialCapacity);
        nameValues = new ByteBufferCharSequence[cap << 1];
        for (int i = 0; i < nameValues.length; i++) {
            nameValues[i] = new ByteBufferCharSequence();
        }
    }

    /** Clear all collected headers for reuse on the next request. Call at {@code onRequestLine}. */
    public void reset() {
        count = 0;
    }

    /**
     * Collect one header, as delivered to {@link ConnectionHandler#onHeader onHeader}. The
     * {@code buffer} is the same for every header of a request; the offsets/lengths are windows into it
     * (name and, whitespace-trimmed, value). Grows the backing arrays if needed.
     */
    public void add(final ByteBuffer buffer, final int nameOffset, final int nameLength,
                    final int valueOffset, final int valueLength) {
        if ((count << 1) == nameValues.length) {
            makeRoom();
        }
        nameValues[count << 1].set(buffer, nameOffset, nameLength);
        nameValues[(count << 1) + 1].set(buffer, valueOffset, valueLength);
        count++;
    }

    /** The number of collected headers (each occurrence counts, so duplicates are included). */
    public int size() {
        return count;
    }

    /** Whether a header named {@code name} (case-insensitive ASCII) was collected. */
    public boolean contains(final CharSequence name) {
        return firstIndexOf(name) >= 0;
    }

    /**
     * The value of the first header named {@code name} (case-insensitive ASCII) as a reusable flyweight,
     * or {@code null} if there is no such header.
     */
    public CharSequence firstValue(final CharSequence name) {
        final int index = firstIndexOf(name);
        return index < 0 ? null : nameValues[(index << 1) + 1];
    }

    /**
     * The value of the first header named {@code name} (case-insensitive ASCII) as a real
     * {@link String}, or {@code null} if there is no such header. Lazily materialized and cached for the
     * current request.
     */
    public String firstStringValue(final CharSequence name) {
        final int index = firstIndexOf(name);
        return index < 0 ? null : nameValues[(index << 1) + 1].stringValue();
    }

    /**
     * Invokes {@code consumer} once, in insertion order, with the reusable value flyweight of every
     * header named {@code name} (case-insensitive ASCII); returns the number of matches (0 leaves
     * {@code consumer} uncalled). The flyweight passed is valid only for the duration of the callback
     * -- see the class-level lifetime note.
     */
    public int values(final CharSequence name, final Consumer<CharSequence> consumer) {
        int matched = 0;
        for (int i = 0; i < count; i++) {
            if (nameValues[i << 1].equalsIgnoreCaseAscii(name)) {
                consumer.accept(nameValues[(i << 1) + 1]);
                matched++;
            }
        }
        return matched;
    }

    /**
     * Like {@link #values(CharSequence, Consumer)}, but passes each value as a real {@link String},
     * lazily materialized and cached for the current request.
     */
    public int valuesString(final CharSequence name, final Consumer<String> consumer) {
        int matched = 0;
        for (int i = 0; i < count; i++) {
            if (nameValues[i << 1].equalsIgnoreCaseAscii(name)) {
                consumer.accept(nameValues[(i << 1) + 1].stringValue());
                matched++;
            }
        }
        return matched;
    }

    private int firstIndexOf(final CharSequence name) {
        for (int i = 0; i < count; i++) {
            if (nameValues[i << 1].equalsIgnoreCaseAscii(name)) {
                return i;
            }
        }
        return -1;
    }

    private void makeRoom() {
        final int oldLen = nameValues.length;
        final int newLen = oldLen << 1;
        final ByteBufferCharSequence[] newSeqs = new ByteBufferCharSequence[newLen];
        System.arraycopy(nameValues, 0, newSeqs, 0, oldLen);
        for (int i = oldLen; i < newLen; i++) {
            newSeqs[i] = new ByteBufferCharSequence();
        }
        nameValues = newSeqs;
    }

    /**
     * A reusable ASCII {@link CharSequence} flyweight over a window {@code [off, off+len)} of a backing
     * {@link ByteBuffer} (the request's input buffer). Zero-allocation to read; {@link #toString()} /
     * {@link #subSequence} are the opt-in allocations for a caller that wants a real {@code String}.
     * Modeled on {@code ParametrizedRouter}'s {@code PathCharSequence}, but byte-backed.
     */
    private static final class ByteBufferCharSequence implements CharSequence {
        private ByteBuffer base;
        private int off;
        private int len;
        private String cached;

        private void set(final ByteBuffer base, final int off, final int len) {
            this.base = base;
            this.off = off;
            this.len = len;
            this.cached = null;
        }

        private String stringValue() {
            if (cached == null) {
                final byte[] bytes = new byte[len];
                for (int i = 0; i < len; i++) {
                    bytes[i] = base.get(off + i);
                }
                // UTF-8 for the same reason as the request line -- see HttpServer#slice.
                cached = new String(bytes, StandardCharsets.UTF_8);
            }
            return cached;
        }

        private boolean equalsIgnoreCaseAscii(final CharSequence other) {
            if (other == null || other.length() != len) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                final int a = AsciiText.toLower(base.get(off + i) & 0xFF);
                if (a != AsciiText.toLower(other.charAt(i))) {
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
            return (char) (base.get(off + index) & 0xFF);
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
