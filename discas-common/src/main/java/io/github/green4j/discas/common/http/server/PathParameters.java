/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

/**
 * Read-only view of the path variables captured by {@link ParametrizedRouter} when a request path
 * matches a parametrized route expression (e.g. {@code /api/{id}/users} against {@code /api/42/users}
 * yields {@code id=42}).
 *
 * <h2>Zero allocation</h2>
 * Each {@link #value(int) value} is a reusable {@link CharSequence} flyweight over the request's
 * {@code pathString()} (an offset+length window), so reading parameters allocates nothing beyond the
 * single {@code pathString()} materialization the request already pays. A caller that needs a real
 * {@link String} uses {@link #valueString(int) valueString(...)}, which materializes and caches it
 * once per request (unlike {@code value(...).toString()}, which allocates a fresh String each call).
 *
 * <p><b>Do not retain</b> a {@code PathParameters} instance, its {@link #value value} flyweights, or
 * their characters beyond the request that produced them: the instance and its flyweights are owned
 * by the connection and re-pointed on the next request. Copy out (via {@code toString()}, or by
 * parsing on the spot) anything that must outlive the current request.
 *
 * <p>Received in {@link ParametrizedRouter.Handler#onMatched onMatched}; read by name or index:
 * <pre>{@code
 * // Route "/api/{id}/users" matched by "/api/42/users":
 * public void onMatched(Connection c, HttpRequest r, PathParameters params) {
 *     CharSequence idView = params.value("id");     // flyweight: "42" (zero allocation)
 *     String id = params.valueString("id");         // materialized String, cached per request
 *     for (int i = 0; i < params.size(); i++) {
 *         // params.name(i) -> "id", params.value(i) -> "42"
 *     }
 * }
 * }</pre>
 */
public interface PathParameters {

    /** The number of captured parameters (the count of {@code {...}} segments in the matched route). */
    int size();

    /** The declared name of the parameter at {@code index} (its {@code {name}} in the route expression). */
    String name(int index);

    /** The index of the parameter declared as {@code name}, or {@code -1} if there is no such parameter. */
    int nameToIndex(String name);

    /**
     * The captured value of the parameter at {@code index} as a reusable flyweight over the request
     * path, or {@code null} if {@code index} is out of range. Valid only for the current request.
     */
    CharSequence value(int index);

    /**
     * The captured value of the parameter declared as {@code name}, or {@code null} if there is no
     * such parameter. A convenience over {@link #nameToIndex(String)} + {@link #value(int)}.
     */
    CharSequence value(String name);

    /**
     * The captured value of the parameter at {@code index} as a real {@link String}, or
     * {@code null} if {@code index} is out of range. Lazily materialized and cached for the
     * current request, so repeated reads of the same parameter allocate at most one String.
     */
    String valueString(int index);

    /**
     * The captured value of the parameter declared as {@code name} as a real {@link String},
     * or {@code null} if there is no such parameter. Cached like {@link #valueString(int)}.
     */
    String valueString(String name);
}
