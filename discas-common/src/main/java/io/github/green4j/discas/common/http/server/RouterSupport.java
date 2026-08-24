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

/**
 * Shared, package-visible machinery for the flat-array DFA routers ({@link PrefixRouter} and
 * {@link ParametrizedRouter}). Both compile their route tables into an {@code int[]} state-jump table
 * plus parallel {@code String[]} segment arrays (modeled on {@code newa-rest}'s {@code PathMatcher})
 * and, at match time, binary-search that table by source state and pack a route index into a leaf
 * {@code to} so a state can be both a leaf and have outgoing edges. Those primitives live here so the
 * two routers stay in lock-step and neither carries a private copy.
 *
 * <p>All methods are static and stateless (reading only their arguments), so the compiled arrays they
 * operate on remain safe to share across connections/threads without synchronization.
 */
abstract class RouterSupport {

    private RouterSupport() {
    }

    // A leaf 'to' is stored negative, carrying both the route index (high bits) and the continuation
    // state (low 16 bits), so a state can terminate a registered route yet still be descended past
    // into a longer route.

    static int routeIndexToTo(final int routeIndex, final int to) {
        return (-(routeIndex + 1)) << 16 | to;
    }

    static boolean isRouteIndex(final int to) {
        return to < 0;
    }

    static int toRouteIndex(final int to) {
        return -(to >> 16) - 1;
    }

    static int toTo(final int to) {
        return to & 0xFFFF;
    }

    /**
     * Binary search {@code stateJumps} (pairs of {@code [from, to]}, sorted by {@code from}) for the
     * first edge whose source state equals {@code from}, or {@code -1} if none. Siblings sharing
     * {@code from} follow contiguously and are scanned linearly by the caller.
     */
    static int findFirstFromIndex(final int[] stateJumps, final int from) {
        int low = 0;
        int high = (stateJumps.length >>> 1) - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final int midVal = stateJumps[mid << 1];
            if (midVal < from) {
                low = mid + 1;
            } else if (midVal > from) {
                high = mid - 1;
            } else {
                int m = mid;
                while (m > 0 && stateJumps[(m - 1) << 1] == midVal) {
                    m--;
                }
                return m;
            }
        }
        return -1;
    }

    /** Zero-allocation compare of a static segment against {@code path[start, end)}. */
    static boolean segmentEquals(final String segment, final String path,
                                 final int start, final int end) {
        final int n = end - start;
        if (segment.length() != n) {
            return false;
        }
        for (int k = 0; k < n; k++) {
            if (segment.charAt(k) != path.charAt(start + k)) {
                return false;
            }
        }
        return true;
    }

    // Default fallback shared by both routers: a bodyless 404, keep-alive per the request.
    private static final ConnectionInitializer DEFAULT_FALLBACK = connection -> new ConnectionHandler() {
        @Override
        public void onRequestComplete(final Connection c, final HttpRequest r) {
            c.beginResponse(r.keepAlive()).commit(404);
        }
    };

    /** The default fallback: a per-connection factory yielding a bodyless {@code 404 Not Found}. */
    static ConnectionInitializer defaultFallback() {
        return DEFAULT_FALLBACK;
    }
}
