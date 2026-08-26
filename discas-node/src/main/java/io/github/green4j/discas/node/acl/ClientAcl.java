/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.io.Reloadable;

/**
 * A reloadable source of the client authorization table. One {@link Reloadable} contract --
 * snapshot plus replay-on-subscribe -- serves both a static in-memory source and a file source, so
 * an ACL edit takes effect on the next reload, with no restart.
 */
public interface ClientAcl extends Reloadable<ClientAclSnapshot> {
}
