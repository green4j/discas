/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.starter;

import java.util.Locale;

/**
 * How a node authenticates clients connecting to its client port, selected by
 * {@code --client-auth} / {@code DISCAS_CLIENT_AUTH}.
 * <p>
 * Authentication (who is this client?) is separate from authorization (what may it touch?),
 * which is configured independently by {@code --client-acl-file}. A node can run any auth mode
 * with or without an ACL.
 */
public enum ClientAuthMode {

    /**
     * Accept the client id claimed in CLIENT_HELLO at face value. The default, and appropriate
     * only on a trusted network -- any client can claim any id, so an ACL bound on top of this
     * is advisory rather than enforced.
     */
    ALLOWALL,

    /**
     * Require a shared token, verified against a PBKDF2 hash from a hot-reloaded token store
     * ({@code --client-token-file} or {@code --client-token-dir}). Combine with
     * {@code --client-tls} so the token is not sent in the clear.
     */
    TOKEN,

    /**
     * Require a client certificate; the certificate CN is the authoritative client id and the
     * claimed hello id must match it. Implies {@code --client-tls} with a trust store.
     */
    MTLS;

    /**
     * Parse the {@code --client-auth} value, case-insensitively.
     *
     * @throws IllegalArgumentException if {@code value} is not a known mode
     */
    public static ClientAuthMode parse(final String value) {
        for (final ClientAuthMode mode : values()) {
            if (mode.name().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("client-auth must be one of allowall|token|mtls, got '"
                + value + "'");
    }

    /** The lower-case spelling used on the command line. */
    String cliName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
