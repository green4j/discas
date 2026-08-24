/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;

/**
 * PBKDF2WithHmacSHA256 hashing for client authentication tokens: a token is treated
 * like a password, salted and stretched, and only its derived hash is stored. Verifying
 * a presented token re-derives the hash and compares it in <b>constant time</b> via
 * {@link MessageDigest#isEqual}.
 */
public final class Pbkdf2 {

    /** Default iteration count for newly-provisioned tokens. */
    public static final int DEFAULT_ITERATIONS = 210_000;

    private static final String ALGORITHM = "PBKDF2WithHmacSHA256";
    private static final int KEY_LENGTH_BITS = 256;
    private static final int SALT_BYTES = 16;
    private static final SecureRandom RANDOM = new SecureRandom();

    private Pbkdf2() {
    }

    /** A fresh random salt for provisioning a new token record. */
    public static byte[] newSalt() {
        final byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        return salt;
    }

    /** Derive the PBKDF2 hash of {@code token} under {@code salt} and {@code iterations}. */
    public static byte[] hash(final String token, final byte[] salt, final int iterations) {
        final PBEKeySpec spec =
                new PBEKeySpec(token.toCharArray(), salt, iterations, KEY_LENGTH_BITS);
        try {
            return SecretKeyFactory.getInstance(ALGORITHM).generateSecret(spec).getEncoded();
        } catch (final GeneralSecurityException e) {
            throw new RuntimeException("PBKDF2 derivation failed", e);
        } finally {
            spec.clearPassword();
        }
    }

    /**
     * Constant-time verification of {@code token} against a stored hash. Uses
     * {@link MessageDigest#isEqual(byte[], byte[])} -- never {@code Arrays.equals}.
     */
    public static boolean verify(final String token, final byte[] salt, final int iterations,
                                 final byte[] expectedHash) {
        final byte[] computed = hash(token, salt, iterations);
        return MessageDigest.isEqual(expectedHash, computed);
    }
}
