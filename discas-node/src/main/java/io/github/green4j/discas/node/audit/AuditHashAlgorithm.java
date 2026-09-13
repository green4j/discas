/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

/**
 * The digest an audit record carries for a key or a value, named in every line that carries one:
 * a reader compares it against a candidate of their own, which a bare hex string does not allow.
 */
public enum AuditHashAlgorithm {

    NONE("none", null),
    SHA_256("sha-256", "SHA-256"),
    SHA_512("sha-512", "SHA-512");

    private static final AuditHashAlgorithm[] VALUES = values();

    private final String configName;
    private final String jcaName;

    AuditHashAlgorithm(final String configName, final String jcaName) {
        this.configName = configName;
        this.jcaName = jcaName;
    }

    /** As written in the configuration file and printed in the line. */
    public String configName() {
        return configName;
    }

    /** The JCA name {@code MessageDigest.getInstance} takes, or {@code null} for {@link #NONE}. */
    public String jcaName() {
        return jcaName;
    }

    static AuditHashAlgorithm fromOrdinal(final int ordinal) {
        return VALUES[ordinal];
    }

    public static AuditHashAlgorithm fromConfigName(final String name) {
        final AuditHashAlgorithm[] all = VALUES;
        for (int i = 0; i < all.length; i++) {
            if (all[i].configName.equalsIgnoreCase(name)) {
                return all[i];
            }
        }
        throw new IllegalArgumentException("Unknown audit hash algorithm '" + name + "'");
    }
}
