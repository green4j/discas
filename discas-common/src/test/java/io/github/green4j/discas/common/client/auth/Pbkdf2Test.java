/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Pbkdf2 -- salted hashing and constant-time verify")
class Pbkdf2Test {

    @Test
    @DisplayName("The correct token verifies; a wrong token does not")
    void verifyMatchesOnlyCorrectToken() {
        final byte[] salt = Pbkdf2.newSalt();
        final byte[] hash = Pbkdf2.hash("s3cret", salt, Pbkdf2.DEFAULT_ITERATIONS);

        assertTrue(Pbkdf2.verify("s3cret", salt, Pbkdf2.DEFAULT_ITERATIONS, hash));
        assertFalse(Pbkdf2.verify("wrong", salt, Pbkdf2.DEFAULT_ITERATIONS, hash));
    }

    @Test
    @DisplayName("Distinct salts produce distinct hashes for the same token")
    void distinctSaltsDiverge() {
        final byte[] saltA = Pbkdf2.newSalt();
        final byte[] saltB = Pbkdf2.newSalt();
        final byte[] hashA = Pbkdf2.hash("same", saltA, Pbkdf2.DEFAULT_ITERATIONS);
        final byte[] hashB = Pbkdf2.hash("same", saltB, Pbkdf2.DEFAULT_ITERATIONS);

        assertNotEquals(Arrays.toString(hashA), Arrays.toString(hashB));
    }
}
