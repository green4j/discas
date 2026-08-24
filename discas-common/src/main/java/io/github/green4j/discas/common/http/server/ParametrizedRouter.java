/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.Connection;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionHandler;
import io.github.green4j.discas.common.http.server.HttpServer.ConnectionInitializer;
import io.github.green4j.discas.common.http.server.HttpServer.HttpRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A <em>parametrized</em> path router for {@link HttpServer}: a {@link ConnectionInitializer} holding
 * a shared, immutable, compiled route table. Per connection it yields an internal routing
 * {@link ConnectionHandler} that, once the request path is available (at
 * {@link ConnectionHandler#onRequestLine onRequestLine}), matches it against the registered
 * <em>parametrized expressions</em> (e.g. {@code /api/{id}/users}), captures the {@code {...}} segment
 * values, and forwards the request to the single matched {@link Handler}.
 *
 * <h2>Single exact match</h2>
 * Unlike {@link PrefixRouter} (which broadcasts to every matching prefix), this router selects
 * <em>exactly one</em> route: the whole request path must match a registered expression segment for
 * segment. A static segment is preferred over a parameter at the same position, so with
 * {@code /api/health} and {@code /api/{id}} both registered, {@code /api/health} takes the static
 * route and {@code /api/42} takes the parameter route ({@code id=42}). A query string ({@code ?...})
 * is ignored. When no expression matches, the request goes to the {@linkplain Builder#fallback
 * fallback} handler (a bodyless {@code 404 Not Found} by default).
 *
 * <h2>Captured parameters</h2>
 * On a match the router fires {@link Handler#onMatched} with a {@link PathParameters} view of the
 * captured values, <em>in place of</em> {@code onRequestLine} (which the router does not separately
 * forward -- {@code onMatched} is the matched delegate's request-line-phase entry). The remaining
 * lifecycle callbacks ({@code onHeader}, {@code onHeadersComplete}, {@code onBodyChunk},
 * {@code onRequestComplete}) forward to that same delegate unchanged. The {@code PathParameters}
 * instance and its value flyweights stay valid for the whole request (they window the request's
 * {@code pathString()}), so a delegate may stash the reference in {@code onMatched} and read it later
 * in {@code onRequestComplete}; it must not be retained past the request.
 *
 * <p>Each route is registered with a {@link HandlerInitializer} (a factory), so every connection gets
 * its own delegate instance that may hold per-connection state. A delegate is created lazily on the
 * first request that matches its route on a given connection; its {@code onOpen} is replayed at that
 * point and it persists for the life of the connection.
 * {@code onError}/{@code onException}/{@code onClose} are forwarded (best-effort) to every delegate
 * opened on the connection.
 *
 * <p>The compiled table is a flat-array DFA (an {@code int[]} state-jump table plus parallel
 * {@code String[]} static- and parameter-segment arrays, binary-searched on the source state),
 * modeled on {@code newa-rest}'s {@code PathMatcher} and sharing its primitives with
 * {@link PrefixRouter} via {@code RouterSupport}. It is immutable and shared across all
 * connections/threads; matching reads only those arrays and writes results into a per-connection
 * buffer, so it needs no synchronization. Matching runs over the request path {@code String} (from
 * {@link HttpRequest#pathString()}, lazily materialized and cached once per request) and allocates
 * nothing beyond it.
 *
 * <p>Register expressions at startup and pass the router to the server as its
 * {@link ConnectionInitializer}. A matched {@link Handler} receives the captured values in
 * {@link Handler#onMatched onMatched} (read them by name or index):
 * <pre>{@code
 * ParametrizedRouter router = ParametrizedRouter.builder()
 *         .route("/api/{id}/users", c -> new ParametrizedRouter.Handler() {
 *             private PathParameters params;
 *
 *             public void onMatched(Connection conn, HttpRequest r, PathParameters p) {
 *                 params = p;                       // valid for the whole request; stash and read later
 *             }
 *
 *             public void onRequestComplete(Connection conn, HttpRequest r) {
 *                 String id = params.valueString("id");
 *                 conn.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive())
 *                     .setBodyAscii("users of " + id + "\n")
 *                     .ok();
 *             }
 *         })
 *         .route("/api/health", healthInit)        // static segment preferred over {id}
 *         .fallback(fallbackInit)                  // no match (default: 404)
 *         .build();
 *
 * new HttpServer(cfg, new AcceptHandler() { }, router).start();
 * }</pre>
 */
public final class ParametrizedRouter implements ConnectionInitializer {
    private static final int INITIAL_STATE = 0;
    private static final String EMPTY_STRING = "";

    /**
     * A matched delegate: a {@link ConnectionHandler} with the extra {@link #onMatched} entry that
     * carries the captured {@link PathParameters}. {@code onMatched} fires in the request-line phase
     * (in place of {@code onRequestLine}, which the router does not forward); the other
     * {@code ConnectionHandler} callbacks forward normally.
     */
    public interface Handler extends ConnectionHandler {
        /**
         * The request path matched this handler's route. Fired once per matching request, in the
         * request-line phase, with a {@link PathParameters} view of the captured {@code {...}} values.
         * The {@code parameters} view is valid for the whole request (so it may be stashed and read in
         * {@code onRequestComplete}) but must not be retained past it.
         */
        void onMatched(Connection connection, HttpRequest request, PathParameters parameters);
    }

    /**
     * Factory for a {@link Handler}: invoked once per connection on first match of its route, mirroring
     * {@link ConnectionInitializer}. Return a fresh handler for per-connection state, or a shared one.
     */
    @FunctionalInterface
    public interface HandlerInitializer {
        Handler initConnection(Connection connection);
    }

    /** Start building a router. Register routes with {@link Builder#route}, then {@link Builder#build}. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Assembles routes into an immutable {@link ParametrizedRouter}. Not thread-safe; build once at
     * startup. The mutable-builder / immutable-router split mirrors {@link PrefixRouter} and the
     * reference {@code PathMatcher}: the router's hot-path form is flat arrays, so it cannot be mutated
     * after {@link #build()}.
     */
    public static final class Builder {
        private final TreeSet<Jump> jumps = new TreeSet<>();
        private final List<HandlerInitializer> handlers = new ArrayList<>();
        private int currentState = INITIAL_STATE;
        private int maxParams = 0;
        private ConnectionInitializer fallback;

        private Builder() {
        }

        /**
         * Register {@code delegate} for the parametrized path {@code expression} (e.g. {@code "/api/v1"}
         * or {@code "/api/{id}/users"}). Segments wrapped in braces ({@code {name}}) are parameters; the
         * rest are static. The expression is matched by whole path segments, in full (this is an exact
         * router, not a prefix router). Registering the same expression twice -- or a parameter whose
         * name conflicts with an existing parameter at the same position -- is an error. {@code delegate}
         * is a factory: a fresh handler is created per connection on first match.
         */
        public Builder route(final String expression, final HandlerInitializer delegate) {
            Objects.requireNonNull(expression, "expression");
            Objects.requireNonNull(delegate, "delegate");

            final List<Segment> segments = parseExpression(expression);
            if (segments.isEmpty()) {
                throw new IllegalArgumentException("Empty path expression: " + expression);
            }

            int from = INITIAL_STATE;
            Jump lastJump = null;
            int params = 0;
            final Jump probe = new Jump();
            for (int s = 0, n = segments.size(); s < n; s++) {
                final Segment segment = segments.get(s);
                probe.from = from;
                probe.segment = segment.name;
                probe.isParameter = segment.isParameter;
                if (segment.isParameter) {
                    params++;
                }
                final SortedSet<Jump> existing = jumps.subSet(probe, true, probe, true);
                if (existing.isEmpty()) {
                    probe.to = ++currentState;
                    final Jump stored = probe.copy();
                    jumps.add(stored);
                    lastJump = stored;
                    from = stored.to;
                } else {
                    lastJump = existing.first();
                    // Only one parameter edge may leave a given state (two parameters compare equal
                    // regardless of name); a differing name at the same position is ambiguous.
                    if (segment.isParameter && !lastJump.segment.equals(segment.name)) {
                        throw new IllegalArgumentException("Ambiguous parameter {" + segment.name
                                + "} conflicts with {" + lastJump.segment + "} in the path expression: "
                                + expression);
                    }
                    from = lastJump.to;
                }
            }

            if (lastJump.isLeaf()) {
                throw new IllegalArgumentException("Ambiguous (duplicate) path expression: " + expression);
            }
            lastJump.routeIndex = handlers.size();
            handlers.add(delegate);
            if (params > maxParams) {
                maxParams = params;
            }
            return this;
        }

        /**
         * The handler for requests matching no registered expression. Optional -- the default is a
         * bodyless {@code 404 Not Found} (respecting keep-alive). Like a route, this is a per-connection
         * factory; the fallback receives no {@link PathParameters}, so it is a plain
         * {@link ConnectionInitializer}.
         */
        public Builder fallback(final ConnectionInitializer delegate) {
            this.fallback = Objects.requireNonNull(delegate, "delegate");
            return this;
        }

        public ParametrizedRouter build() {
            final int size = jumps.size();
            final int[] stateJumps = new int[size << 1];
            final String[] staticSegments = new String[size];
            final String[] parameterSegments = new String[size];
            int i = 0;
            for (final Jump jump : jumps) { // TreeSet iterates sorted by (from, static-before-parameter)
                stateJumps[i << 1] = jump.from;
                stateJumps[(i << 1) + 1] = jump.isLeaf()
                        ? RouterSupport.routeIndexToTo(jump.routeIndex, jump.to)
                        : jump.to;
                if (jump.isParameter) {
                    parameterSegments[i] = jump.segment;
                } else {
                    staticSegments[i] = jump.segment;
                }
                i++;
            }
            final HandlerInitializer[] routeInitializers =
                    handlers.toArray(new HandlerInitializer[0]);
            final ConnectionInitializer fb = fallback != null ? fallback : RouterSupport.defaultFallback();
            return new ParametrizedRouter(stateJumps, staticSegments, parameterSegments,
                    routeInitializers, fb, maxParams);
        }
    }

    // A single edge of the DFA under construction: consuming a path segment (static 'segment', or any
    // value when isParameter) moves the match from state 'from' to state 'to'. A leaf edge
    // (routeIndex >= 0) terminates a registered expression yet still records 'to' so matching can
    // continue past it into a longer expression.
    private static final class Jump implements Comparable<Jump> {
        private int from = INITIAL_STATE;
        private int to = INITIAL_STATE;
        private String segment;
        private boolean isParameter;
        private int routeIndex = -1;

        private boolean isLeaf() {
            return routeIndex > -1;
        }

        private Jump copy() {
            final Jump c = new Jump();
            c.from = from;
            c.to = to;
            c.segment = segment;
            c.isParameter = isParameter;
            c.routeIndex = routeIndex;
            return c;
        }

        @Override
        public int compareTo(final Jump o) {
            final int byFrom = Integer.compare(from, o.from);
            if (byFrom != 0) {
                return byFrom;
            }
            if (!isParameter) {
                if (!o.isParameter) {
                    return CharSequence.compare(segment, o.segment); // two statics: by name
                }
                return -1; // a static sorts before a parameter (statics are tried first)
            }
            if (o.isParameter) {
                return 0; // two parameters at the same state are equal regardless of name
            }
            return 1; // a parameter sorts after a static
        }
    }

    // A parsed path-expression segment: a static name, or a parameter name (isParameter).
    private static final class Segment {
        private final String name;
        private final boolean isParameter;

        private Segment(final String name, final boolean isParameter) {
            this.name = name;
            this.isParameter = isParameter;
        }
    }

    // Immutable, shared across all connections/threads.
    private final int[] stateJumps;            // [from, to-or-packed-leaf] pairs, sorted by 'from'
    private final String[] staticSegments;     // parallel to stateJumps pairs: static name, or null
    private final String[] parameterSegments;  // parallel: parameter name on a parameter edge, else null
    private final HandlerInitializer[] routeInitializers; // by route index
    private final ConnectionInitializer fallbackInitializer;
    private final int maxParams;

    private ParametrizedRouter(final int[] stateJumps,
                               final String[] staticSegments,
                               final String[] parameterSegments,
                               final HandlerInitializer[] routeInitializers,
                               final ConnectionInitializer fallbackInitializer,
                               final int maxParams) {
        this.stateJumps = stateJumps;
        this.staticSegments = staticSegments;
        this.parameterSegments = parameterSegments;
        this.routeInitializers = routeInitializers;
        this.fallbackInitializer = fallbackInitializer;
        this.maxParams = maxParams;
    }

    @Override
    public ConnectionHandler initConnection(final Connection connection) {
        return new Routing();
    }

    /**
     * Walk {@code path} segment by segment, following the single matching edge (static preferred over
     * parameter) and recording captured parameter values into {@code out}. Returns the matched route
     * index if the full path lands on a leaf, or {@code -1} for no exact match. Reads only immutable
     * state; writes only {@code out}.
     */
    private int match(final String path, final MatchedParameters out) {
        out.reset(path);
        int state = INITIAL_STATE;
        final int len = path.length();
        int i = 0;
        while (i < len) {
            final char c = path.charAt(i);
            if (c == '?') {
                break; // query string: not part of the path
            }
            if (c == '/') {
                i++;
                continue;
            }
            final int segStart = i;
            while (i < len) {
                final char cc = path.charAt(i);
                if (cc == '/' || cc == '?') {
                    break;
                }
                i++;
            }
            final int segEnd = i;

            if (RouterSupport.isRouteIndex(state)) {
                state = RouterSupport.toTo(state); // at a leaf but descending further: unwrap
            }
            final int first = RouterSupport.findFirstFromIndex(stateJumps, state);
            if (first < 0) {
                return -1; // no outgoing edge from here -> no exact match
            }
            boolean advanced = false;
            for (int j = first; j < staticSegments.length; j++) {
                if (stateJumps[j << 1] != state) {
                    break; // past this source state's edges
                }
                final String stat = staticSegments[j];
                if (stat != null) {
                    if (RouterSupport.segmentEquals(stat, path, segStart, segEnd)) {
                        state = stateJumps[(j << 1) + 1];
                        advanced = true;
                        break;
                    }
                } else {
                    // The lone parameter edge for this state (statics are ordered first): capture it.
                    out.add(parameterSegments[j], segStart, segEnd);
                    state = stateJumps[(j << 1) + 1];
                    advanced = true;
                    break;
                }
            }
            if (!advanced) {
                return -1; // this segment matches no edge
            }
        }
        return RouterSupport.isRouteIndex(state) ? RouterSupport.toRouteIndex(state) : -1;
    }

    private static final int PS_INITIAL = 0;
    private static final int PS_STATIC = 1;
    private static final int PS_PARAM = 2;

    // Split a registered expression into its segments; braces delimit a parameter, everything else is
    // a static segment. Leading/trailing/doubled slashes are skipped. Rejects malformed input.
    private static List<Segment> parseExpression(final String expr) {
        final List<Segment> segments = new ArrayList<>();
        final StringBuilder name = new StringBuilder();
        int state = PS_INITIAL;
        for (int i = 0; i < expr.length(); i++) {
            final char c = expr.charAt(i);
            switch (state) {
                case PS_INITIAL:
                    if (c == '/') {
                        break;
                    }
                    if (c == '{') {
                        name.setLength(0);
                        state = PS_PARAM;
                        break;
                    }
                    if (isNameChar(c)) {
                        name.setLength(0);
                        name.append(c);
                        state = PS_STATIC;
                        break;
                    }
                    throw unexpectedChar(i, c);
                case PS_STATIC:
                    if (c == '/') {
                        segments.add(new Segment(name.toString(), false));
                        state = PS_INITIAL;
                        break;
                    }
                    if (isNameChar(c)) {
                        name.append(c);
                        break;
                    }
                    throw unexpectedChar(i, c);
                case PS_PARAM:
                    if (c == '}') {
                        if (name.length() == 0) {
                            throw new IllegalArgumentException("Empty parameter name in: " + expr);
                        }
                        segments.add(new Segment(name.toString(), true));
                        state = PS_INITIAL;
                        break;
                    }
                    if (isNameChar(c)) {
                        name.append(c);
                        break;
                    }
                    throw unexpectedChar(i, c);
                default:
                    throw new IllegalStateException();
            }
        }
        switch (state) {
            case PS_STATIC:
                segments.add(new Segment(name.toString(), false));
                break;
            case PS_PARAM:
                throw new IllegalArgumentException("Unterminated parameter '{" + name + "' in: " + expr);
            default:
                break;
        }
        return segments;
    }

    private static IllegalArgumentException unexpectedChar(final int i, final char c) {
        return new IllegalArgumentException("Unexpected char '" + c + "' at pos " + i);
    }

    private static boolean isNameChar(final char c) {
        if (Character.isLetterOrDigit(c)) {
            return true;
        }
        switch (c) {
            case '-':
            case '.':
            case '_':
            case '~':
                return true;
            default:
                return false;
        }
    }

    /**
     * Per-connection, mutable {@link PathParameters} implementation: a reusable capture buffer whose
     * value flyweights window the request's {@code pathString()}. Reset at the start of each match and
     * read only after a successful one, so partial captures from a failed walk are harmless.
     */
    private static final class MatchedParameters implements PathParameters {
        private final String[] names;
        private final PathCharSequence[] values;
        private String path = EMPTY_STRING;
        private int count;

        private MatchedParameters(final int maxParams) {
            names = new String[maxParams];
            values = new PathCharSequence[maxParams];
            for (int i = 0; i < maxParams; i++) {
                values[i] = new PathCharSequence();
            }
        }

        private void reset(final String pathString) {
            this.path = pathString;
            this.count = 0;
        }

        private void add(final String name, final int start, final int end) {
            names[count] = name;
            values[count].set(path, start, end - start);
            count++;
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public String name(final int index) {
            if (index < 0 || index >= count) {
                return null;
            }
            return names[index];
        }

        @Override
        public int nameToIndex(final String name) {
            for (int i = 0; i < count; i++) {
                if (names[i].equals(name)) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public CharSequence value(final int index) {
            if (index < 0 || index >= count) {
                return null;
            }
            return values[index];
        }

        @Override
        public CharSequence value(final String name) {
            final int index = nameToIndex(name);
            return index < 0 ? null : values[index];
        }

        @Override
        public String valueString(final int index) {
            if (index < 0 || index >= count) {
                return null;
            }
            return values[index].stringValue();
        }

        @Override
        public String valueString(final String name) {
            final int index = nameToIndex(name);
            return index < 0 ? null : values[index].stringValue();
        }
    }

    /**
     * A reusable {@link CharSequence} flyweight over a window {@code [off, off+len)} of a backing
     * {@code String} (the request path). Zero-allocation to read; {@link #toString()} /
     * {@link #subSequence} are the opt-in allocations for a caller that wants a real {@code String}.
     */
    private static final class PathCharSequence implements CharSequence {
        private String base = EMPTY_STRING;
        private int off;
        private int len;
        private String cached;

        private void set(final String base, final int off, final int len) {
            this.base = base;
            this.off = off;
            this.len = len;
            this.cached = null;
        }

        private String stringValue() {
            if (cached == null) {
                cached = base.substring(off, off + len);
            }
            return cached;
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
            return base.charAt(off + index);
        }

        @Override
        public CharSequence subSequence(final int start, final int end) {
            if (start < 0 || end > len || start > end) {
                throw new IndexOutOfBoundsException("[" + start + ", " + end + "), length " + len);
            }
            return base.substring(off + start, off + end);
        }

        @Override
        public String toString() {
            return stringValue();
        }
    }

    /**
     * Per-connection routing handler: matches each request's path and forwards its callbacks to the
     * single matched delegate (or the fallback). Confined to the connection's worker thread, so its
     * state needs no synchronization.
     */
    private final class Routing implements ConnectionHandler {
        private final Handler[] delegates = new Handler[routeInitializers.length];
        private final MatchedParameters params = new MatchedParameters(maxParams);
        private ConnectionHandler fallbackDelegate;
        private int matchedRoute = -1;      // route index for the in-flight request, or -1
        private boolean matchedFallback;

        @Override
        public void onOpen(final Connection connection) {
            // Delegates are created lazily on their first matching request; their onOpen is replayed
            // then. Nothing to do until a path is known.
        }

        @Override
        public void onRequestLine(final Connection connection, final HttpRequest request) {
            matchedRoute = match(request.pathString(), params);
            matchedFallback = matchedRoute < 0;
            if (matchedFallback) {
                ensureFallback(connection).onRequestLine(connection, request);
                return;
            }
            // onMatched is the matched delegate's request-line-phase entry (in place of onRequestLine).
            ensureDelegate(connection, matchedRoute).onMatched(connection, request, params);
        }

        @Override
        public void onHeader(final Connection connection, final HttpRequest request,
                             final ByteBuffer buffer, final int nameOffset, final int nameLength,
                             final int valueOffset, final int valueLength) {
            if (matchedFallback) {
                fallbackDelegate.onHeader(connection, request, buffer,
                        nameOffset, nameLength, valueOffset, valueLength);
                return;
            }
            delegates[matchedRoute].onHeader(connection, request, buffer,
                    nameOffset, nameLength, valueOffset, valueLength);
        }

        @Override
        public void onHeadersComplete(final Connection connection, final HttpRequest request) {
            if (matchedFallback) {
                fallbackDelegate.onHeadersComplete(connection, request);
                return;
            }
            delegates[matchedRoute].onHeadersComplete(connection, request);
        }

        @Override
        public void onBodyChunk(final Connection connection, final HttpRequest request,
                                final ByteBuffer buffer, final int offset, final int length) {
            if (matchedFallback) {
                fallbackDelegate.onBodyChunk(connection, request, buffer, offset, length);
                return;
            }
            delegates[matchedRoute].onBodyChunk(connection, request, buffer, offset, length);
        }

        @Override
        public void onRequestComplete(final Connection connection, final HttpRequest request) {
            if (matchedFallback) {
                fallbackDelegate.onRequestComplete(connection, request);
                return;
            }
            delegates[matchedRoute].onRequestComplete(connection, request);
        }

        @Override
        public void onError(final Connection connection, final int status, final String reason) {
            // A protocol error may arrive before any match (e.g. a malformed request line). Offer it
            // to every opened delegate, in index order, short-circuiting once one renders a response.
            for (int i = 0; i < delegates.length; i++) {
                final Handler d = delegates[i];
                if (d != null) {
                    final long before = connection.responsesBegun();
                    try {
                        d.onError(connection, status, reason);
                    } catch (final Throwable ignore) {
                        // best-effort
                    }
                    if (connection.responsesBegun() != before) {
                        return;
                    }
                }
            }
            if (fallbackDelegate != null) {
                try {
                    fallbackDelegate.onError(connection, status, reason);
                } catch (final Throwable ignore) {
                    // best-effort
                }
            }
        }

        @Override
        public void onException(final Connection connection, final Throwable cause) {
            for (int i = 0; i < delegates.length; i++) {
                final Handler d = delegates[i];
                if (d != null) {
                    try {
                        d.onException(connection, cause);
                    } catch (final Throwable ignore) {
                        // best-effort
                    }
                }
            }
            if (fallbackDelegate != null) {
                try {
                    fallbackDelegate.onException(connection, cause);
                } catch (final Throwable ignore) {
                    // best-effort
                }
            }
        }

        @Override
        public void onClose(final Connection connection) {
            for (int i = 0; i < delegates.length; i++) {
                final Handler d = delegates[i];
                if (d != null) {
                    try {
                        d.onClose(connection);
                    } catch (final Throwable ignore) {
                        // best-effort
                    }
                }
            }
            if (fallbackDelegate != null) {
                try {
                    fallbackDelegate.onClose(connection);
                } catch (final Throwable ignore) {
                    // best-effort
                }
            }
        }

        private Handler ensureDelegate(final Connection connection, final int routeIndex) {
            Handler d = delegates[routeIndex];
            if (d == null) {
                d = routeInitializers[routeIndex].initConnection(connection);
                delegates[routeIndex] = d;
                d.onOpen(connection); // replay the connection-open event for the newly attached delegate
            }
            return d;
        }

        private ConnectionHandler ensureFallback(final Connection connection) {
            if (fallbackDelegate == null) {
                fallbackDelegate = fallbackInitializer.initConnection(connection);
                fallbackDelegate.onOpen(connection);
            }
            return fallbackDelegate;
        }
    }
}
