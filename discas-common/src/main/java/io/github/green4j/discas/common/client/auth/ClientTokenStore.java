/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.io.Reloadable;

/**
 * A reloadable source of provisioned client tokens. One {@link Reloadable} contract --
 * snapshot plus replay-on-subscribe -- serves both a static in-memory store and a file
 * store, so token rotation on disk takes effect on the next reload, with no restart.
 */
public interface ClientTokenStore extends Reloadable<ClientTokens> {
}
