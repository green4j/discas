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
 * A path-<em>prefix</em> router for {@link HttpServer}: a {@link ConnectionInitializer} holding a
 * shared, immutable, compiled route table. Per connection it yields an internal routing
 * {@link ConnectionHandler} that, once the request path is available (at
 * {@link ConnectionHandler#onRequestLine onRequestLine}), matches it against the registered prefixes
 * and forwards the request's callbacks to <em>every</em> matching handler.
 *
 * <h2>Broadcast / filter-chain semantics</h2>
 * A request is dispatched to <em>all</em> routes whose prefix matches its path -- a filter chain,
 * ordered outer&rarr;inner (shortest prefix first). For example, with routes {@code "/"},
 * {@code "/api"} and {@code "/api/v1/users"} registered, a request for {@code /api/v1/users/42}
 * fires all three, in that order. Matching is per path <em>segment</em>: {@code "/api"} matches
 * {@code /api}, {@code /api/} and {@code /api/v1} but not {@code /apix}. A query string
 * ({@code ?...}) is ignored.
 *
 * <p>Lifecycle callbacks ({@code onOpen}, {@code onRequestLine}, {@code onHeader},
 * {@code onHeadersComplete}, {@code onBodyChunk}) are broadcast to every matched handler.
 * {@code onRequestComplete} runs outer&rarr;inner but <em>short-circuits</em> as soon as a handler
 * begins a response (via any {@code beginResponse*} on the connection), so an outer filter can reject
 * a request -- say a {@code 401} -- before the inner handler runs; if none of the matched handlers
 * responds, nothing is sent. When no prefix matches, the request goes to the {@linkplain
 * Builder#fallback fallback} handler (a bodyless {@code 404 Not Found} by default).
 *
 * <p>Each route is registered with a {@link ConnectionInitializer} (a factory), so every connection
 * gets its own delegate instance that may hold per-connection state. A delegate is created lazily on
 * the first request that matches its route on a given connection; its {@code onOpen} is replayed at
 * that point (it is the only lifecycle event that precedes the match), and it persists for the life
 * of the connection, so a route matched by several keep-alive requests keeps its state and its
 * {@code onOpen} fires exactly once. {@code onError}/{@code onException}/{@code onClose} are
 * forwarded (best-effort) to every delegate opened on the connection.
 *
 * <p>The compiled table is a flat-array DFA (an {@code int[]} state-jump table plus a parallel
 * {@code String[]} of static segments, binary-searched on the source state), modeled on the
 * technique in {@code newa-rest}'s {@code PathMatcher}. It is immutable and shared across all
 * connections/threads; matching reads only those arrays and writes results into a per-connection
 * buffer, so it needs no synchronization. Matching runs over the request path {@code String}
 * (from {@link HttpRequest#pathString()}, lazily materialized and cached once per request).
 *
 * <p>Build the table once at startup and pass the router to the server as its
 * {@link ConnectionInitializer}. Here an outer {@code /api} filter authorizes every {@code /api/*}
 * request and the inner {@code /api/time} handler answers only if the filter let it through:
 * <pre>{@code
 * ConnectionInitializer apiFilter = c -> new ConnectionHandler() {
 *     public void onRequestComplete(Connection conn, HttpRequest r) {
 *         if (!authorized(r)) conn.beginResponse().error(401);   // begins a response => short-circuits
 *     }
 * };
 * ConnectionInitializer apiTime = c -> new ConnectionHandler() {
 *     public void onRequestComplete(Connection conn, HttpRequest r) {
 *         conn.beginResponse(ContentType.TEXT_PLAIN, r.keepAlive()).setBodyAscii(now()).ok();
 *     }
 * };
 *
 * PrefixRouter router = PrefixRouter.builder()
 *         .route("/api", apiFilter)         // matches /api and everything under it
 *         .route("/api/time", apiTime)      // also matches /api/time (both fire, outer first)
 *         .fallback(featureHandler)         // everything else (default: 404)
 *         .build();
 *
 * new HttpServer(cfg, new AcceptHandler() { }, router).start();
 * }</pre>
 */
public final class PrefixRouter implements ConnectionInitializer {

    private static final int INITIAL_STATE = 0;

    /** Start building a router. Register routes with {@link Builder#route}, then {@link Builder#build}. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Assembles routes into an immutable {@link PrefixRouter}. Not thread-safe; build once at startup.
     * The mutable-builder / immutable-router split mirrors the reference {@code PathMatcher}: the
     * router's hot-path form is flat arrays, so it cannot be mutated after {@link #build()}.
     */
    public static final class Builder {
        private final TreeSet<Jump> jumps = new TreeSet<>();
        private final List<ConnectionInitializer> handlers = new ArrayList<>();
        private int currentState = INITIAL_STATE;
        private int maxDepth = 0;
        private int rootRouteIndex = -1;
        private ConnectionInitializer fallback;

        private Builder() {
        }

        /**
         * Register {@code delegate} for the path {@code prefix} (e.g. {@code "/api"} or
         * {@code "/api/v1/users"}). The prefix is matched by whole path segments. {@code "/"} (or the
         * empty string) registers a catch-all that matches every request. Registering the same prefix
         * twice is an error. {@code delegate} is a factory: a fresh handler is created per connection
         * on first match.
         */
        public Builder route(final String prefix, final ConnectionInitializer delegate) {
            Objects.requireNonNull(prefix, "prefix");
            Objects.requireNonNull(delegate, "delegate");

            final List<String> segments = splitSegments(prefix);
            if (segments.isEmpty()) { // "/" or "" -> catch-all leaf at the initial state
                if (rootRouteIndex >= 0) {
                    throw new IllegalArgumentException("Ambiguous root route: " + prefix);
                }
                rootRouteIndex = handlers.size();
                handlers.add(delegate);
                return this;
            }

            int from = INITIAL_STATE;
            Jump lastJump = null;
            final Jump probe = new Jump();
            for (int s = 0, n = segments.size(); s < n; s++) {
                final String segment = segments.get(s);
                probe.from = from;
                probe.segment = segment;
                final SortedSet<Jump> existing = jumps.subSet(probe, true, probe, true);
                if (existing.isEmpty()) {
                    probe.to = ++currentState;
                    final Jump stored = probe.copy();
                    jumps.add(stored);
                    lastJump = stored;
                    from = stored.to;
                } else {
                    lastJump = existing.first();
                    from = lastJump.to;
                }
            }

            if (lastJump.isLeaf()) {
                throw new IllegalArgumentException("Ambiguous (duplicate) prefix: " + prefix);
            }
            lastJump.routeIndex = handlers.size();
            handlers.add(delegate);
            if (segments.size() > maxDepth) {
                maxDepth = segments.size();
            }
            return this;
        }

        /**
         * The handler for requests matching no registered prefix. Optional -- the default is a bodyless
         * {@code 404 Not Found} (respecting keep-alive). Like a route, this is a per-connection factory.
         */
        public Builder fallback(final ConnectionInitializer delegate) {
            this.fallback = Objects.requireNonNull(delegate, "delegate");
            return this;
        }

        public PrefixRouter build() {
            final int size = jumps.size();
            final int[] stateJumps = new int[size << 1];
            final String[] staticSegments = new String[size];
            int i = 0;
            for (final Jump jump : jumps) { // TreeSet iterates sorted by (from, segment)
                stateJumps[i << 1] = jump.from;
                stateJumps[(i << 1) + 1] = jump.isLeaf()
                        ? RouterSupport.routeIndexToTo(jump.routeIndex, jump.to)
                        : jump.to;
                staticSegments[i] = jump.segment;
                i++;
            }
            final ConnectionInitializer[] routeInitializers =
                    handlers.toArray(new ConnectionInitializer[0]);
            final ConnectionInitializer fb = fallback != null ? fallback : RouterSupport.defaultFallback();
            final int matchBufferSize = maxDepth + (rootRouteIndex >= 0 ? 1 : 0);
            return new PrefixRouter(stateJumps, staticSegments, routeInitializers,
                    rootRouteIndex, fb, matchBufferSize);
        }
    }

    // A single edge of the DFA under construction: consuming path segment 'segment' moves the match
    // from state 'from' to state 'to'. A leaf edge (routeIndex >= 0) terminates a registered prefix
    // yet still records 'to' so matching can continue past it into a longer prefix.
    private static final class Jump implements Comparable<Jump> {
        private int from = INITIAL_STATE;
        private int to = INITIAL_STATE;
        private String segment;
        private int routeIndex = -1;

        private boolean isLeaf() {
            return routeIndex > -1;
        }

        private Jump copy() {
            final Jump c = new Jump();
            c.from = from;
            c.to = to;
            c.segment = segment;
            c.routeIndex = routeIndex;
            return c;
        }

        @Override
        public int compareTo(final Jump o) {
            final int byFrom = Integer.compare(from, o.from);
            if (byFrom != 0) {
                return byFrom;
            }
            return CharSequence.compare(segment, o.segment);
        }
    }

    // Immutable, shared across all connections/threads.
    private final int[] stateJumps;          // [from, to-or-packed-leaf] pairs, sorted by 'from'
    private final String[] staticSegments;   // parallel to stateJumps pairs: the segment for each edge
    private final ConnectionInitializer[] routeInitializers; // by route index
    private final int rootRouteIndex;        // catch-all route index, or -1
    private final ConnectionInitializer fallbackInitializer;
    private final int matchBufferSize;

    private PrefixRouter(final int[] stateJumps,
                         final String[] staticSegments,
                         final ConnectionInitializer[] routeInitializers,
                         final int rootRouteIndex,
                         final ConnectionInitializer fallbackInitializer,
                         final int matchBufferSize) {
        this.stateJumps = stateJumps;
        this.staticSegments = staticSegments;
        this.routeInitializers = routeInitializers;
        this.rootRouteIndex = rootRouteIndex;
        this.fallbackInitializer = fallbackInitializer;
        this.matchBufferSize = matchBufferSize;
    }

    @Override
    public ConnectionHandler initConnection(final Connection connection) {
        return new Routing();
    }

    /**
     * Walk {@code path} segment by segment, following static edges, and record into {@code out} the
     * route index of every prefix leaf passed, in outer&rarr;inner (shortest-first) order. Returns the
     * number of matches written. Reads only immutable state; writes only {@code out}.
     */
    private int match(final String path, final int[] out) {
        int count = 0;
        int state = INITIAL_STATE;
        if (rootRouteIndex >= 0) {
            out[count++] = rootRouteIndex; // catch-all is the outermost match
        }
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
                state = RouterSupport.toTo(state); // at a leaf but descending further: unwrap the continuation state
            }
            final int first = RouterSupport.findFirstFromIndex(stateJumps, state);
            if (first < 0) {
                return count; // no outgoing edge from here -> no deeper match
            }
            boolean advanced = false;
            for (int j = first; j < staticSegments.length; j++) {
                if (stateJumps[j << 1] != state) {
                    break; // past this source state's edges
                }
                if (RouterSupport.segmentEquals(staticSegments[j], path, segStart, segEnd)) {
                    state = stateJumps[(j << 1) + 1];
                    advanced = true;
                    break;
                }
            }
            if (!advanced) {
                return count; // this segment matches no edge
            }
            if (RouterSupport.isRouteIndex(state)) {
                out[count++] = RouterSupport.toRouteIndex(state); // passed a registered prefix -> record it
            }
        }
        return count;
    }

    // Split a registered prefix into its non-empty path segments (empties from leading/trailing/
    // doubled slashes are dropped); a bare "/" or "" yields an empty list (the catch-all).
    private static List<String> splitSegments(final String prefix) {
        final List<String> segments = new ArrayList<>();
        final int len = prefix.length();
        int i = 0;
        while (i < len) {
            if (prefix.charAt(i) == '/') {
                i++;
                continue;
            }
            final int start = i;
            while (i < len && prefix.charAt(i) != '/') {
                i++;
            }
            segments.add(prefix.substring(start, i));
        }
        return segments;
    }

    /**
     * Per-connection routing handler: matches each request's path and fans its callbacks out to the
     * matching delegates. Confined to the connection's worker thread, so its state needs no
     * synchronization.
     */
    private final class Routing implements ConnectionHandler {
        private final ConnectionHandler[] delegates = new ConnectionHandler[routeInitializers.length];
        private final int[] matched = new int[matchBufferSize];
        private ConnectionHandler fallbackDelegate;
        private int matchedCount;
        private boolean matchedFallback;

        @Override
        public void onOpen(final Connection connection) {
            // Delegates are created lazily on their first matching request; their onOpen is replayed
            // then. Nothing to do until a path is known.
        }

        @Override
        public void onRequestLine(final Connection connection, final HttpRequest request) {
            matchedCount = match(request.pathString(), matched);
            matchedFallback = matchedCount == 0;
            if (matchedFallback) {
                ensureFallback(connection).onRequestLine(connection, request);
                return;
            }
            for (int i = 0; i < matchedCount; i++) {
                ensureDelegate(connection, matched[i]).onRequestLine(connection, request);
            }
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
            for (int i = 0; i < matchedCount; i++) {
                delegates[matched[i]].onHeader(connection, request, buffer,
                        nameOffset, nameLength, valueOffset, valueLength);
            }
        }

        @Override
        public void onHeadersComplete(final Connection connection, final HttpRequest request) {
            if (matchedFallback) {
                fallbackDelegate.onHeadersComplete(connection, request);
                return;
            }
            for (int i = 0; i < matchedCount; i++) {
                delegates[matched[i]].onHeadersComplete(connection, request);
            }
        }

        @Override
        public void onBodyChunk(final Connection connection, final HttpRequest request,
                                final ByteBuffer buffer, final int offset, final int length) {
            if (matchedFallback) {
                fallbackDelegate.onBodyChunk(connection, request, buffer, offset, length);
                return;
            }
            for (int i = 0; i < matchedCount; i++) {
                delegates[matched[i]].onBodyChunk(connection, request, buffer, offset, length);
            }
        }

        @Override
        public void onRequestComplete(final Connection connection, final HttpRequest request) {
            if (matchedFallback) {
                fallbackDelegate.onRequestComplete(connection, request);
                return;
            }
            for (int i = 0; i < matchedCount; i++) {
                final long before = connection.responsesBegun();
                delegates[matched[i]].onRequestComplete(connection, request);
                if (connection.responsesBegun() != before) {
                    return; // this handler took ownership of the reply: stop the chain
                }
            }
        }

        @Override
        public void onError(final Connection connection, final int status, final String reason) {
            // A protocol error may arrive before any match (e.g. a malformed request line). Offer it
            // to every opened delegate, in index order, short-circuiting once one renders a response.
            for (int i = 0; i < delegates.length; i++) {
                final ConnectionHandler d = delegates[i];
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
                final ConnectionHandler d = delegates[i];
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
                final ConnectionHandler d = delegates[i];
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

        private ConnectionHandler ensureDelegate(final Connection connection, final int routeIndex) {
            ConnectionHandler d = delegates[routeIndex];
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
