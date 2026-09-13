/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

/** What a node does when an audit record does not fit in the buffer. */
public enum AuditOverflow {

    /** Drop the record, count it, carry on. The loop waits for nothing. */
    DROP("drop"),

    /**
     * Wait for the drain to free the space. The waiting thread is the event loop, so consensus and
     * every other client of this node wait with it: a trail with no holes, at the node's expense.
     */
    WAIT("wait");

    private static final AuditOverflow[] VALUES = values();

    private final String configName;

    AuditOverflow(final String configName) {
        this.configName = configName;
    }

    public String configName() {
        return configName;
    }

    public static AuditOverflow fromConfigName(final String name) {
        final AuditOverflow[] all = VALUES;
        for (int i = 0; i < all.length; i++) {
            if (all[i].configName.equalsIgnoreCase(name)) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown audit overflow policy '" + name + "'");
    }
}
