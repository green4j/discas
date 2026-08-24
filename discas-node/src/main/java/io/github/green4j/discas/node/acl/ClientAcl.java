/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.io.Reloadable;

/**
 * A hot-reloadable source of the client authorization table. One {@link Reloadable} contract --
 * snapshot plus replay-on-subscribe -- serves both a static in-memory source and a watched file
 * source, so ACL edits are picked up with no restart.
 */
public interface ClientAcl extends Reloadable<ClientAclSnapshot> {
}
