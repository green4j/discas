/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

/**
 * The audit a node runs: a sink and the settings it starts with. The node owns everything between
 * them -- the ring, the drain thread and their lifecycle.
 */
public final class NodeAudit {

    /** No audit: no ring is allocated and no thread is started. */
    public static final NodeAudit NONE = new NodeAudit(AuditLog.NONE, null);

    private final AuditLog log;
    private final AuditConfigSnapshot settings;

    private NodeAudit(final AuditLog log, final AuditConfigSnapshot settings) {
        this.log = log;
        this.settings = settings;
    }

    public static NodeAudit of(final AuditLog log, final AuditConfigSnapshot settings) {
        if (log == null || log == AuditLog.NONE || settings == null) {
            return NONE;
        }
        return new NodeAudit(log, settings);
    }

    public AuditLog log() {
        return log;
    }

    public AuditConfigSnapshot settings() {
        return settings;
    }

    public boolean enabled() {
        return settings != null;
    }
}
