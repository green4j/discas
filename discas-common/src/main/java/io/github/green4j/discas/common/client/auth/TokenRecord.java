/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import java.util.Arrays;

/**
 * One provisioned authentication token for a client: the PBKDF2 salt, iteration count,
 * derived hash, and an expiry. A client may hold several records at once so a new token
 * can be introduced before an old one is retired (overlap rotation).
 * <p>
 * Value-based {@link #equals(Object)} lets a file-backed store skip re-publishing when a
 * reload parses to identical records.
 */
public final class TokenRecord {

    private final byte[] salt;
    private final int iterations;
    private final byte[] hash;
    private final long notAfterEpochMs;

    public TokenRecord(final byte[] salt, final int iterations, final byte[] hash,
                       final long notAfterEpochMs) {
        this.salt = salt.clone();
        this.iterations = iterations;
        this.hash = hash.clone();
        this.notAfterEpochMs = notAfterEpochMs;
    }

    /**
     * The three fields {@link TokenSpecs} needs to write this record back out. Package-private
     * because the salt and the hash are the record's substance: anything outside this package that
     * wants them wants the text form, which {@link TokenSpecs#format(TokenRecord)} gives it.
     */
    byte[] salt() {
        return salt.clone();
    }

    int iterations() {
        return iterations;
    }

    byte[] hash() {
        return hash.clone();
    }

    /** True if this token is no longer valid at {@code nowEpochMs}. */
    public boolean expired(final long nowEpochMs) {
        return nowEpochMs >= notAfterEpochMs;
    }

    public long notAfterEpochMs() {
        return notAfterEpochMs;
    }

    /** Constant-time check that {@code token} matches this record's stored hash. */
    public boolean matches(final String token) {
        return Pbkdf2.verify(token, salt, iterations, hash);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TokenRecord)) {
            return false;
        }
        final TokenRecord other = (TokenRecord) o;
        return iterations == other.iterations
                && notAfterEpochMs == other.notAfterEpochMs
                && Arrays.equals(salt, other.salt)
                && Arrays.equals(hash, other.hash);
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(iterations);
        result = 31 * result + Long.hashCode(notAfterEpochMs);
        result = 31 * result + Arrays.hashCode(salt);
        result = 31 * result + Arrays.hashCode(hash);
        return result;
    }
}
