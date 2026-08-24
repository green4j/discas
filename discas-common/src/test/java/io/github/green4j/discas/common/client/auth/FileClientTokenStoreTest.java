/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("FileClientTokenStore -- parse, reload, overlap rotation")
class FileClientTokenStoreTest {

    private static final ClientId WEB = ClientId.of("web-1");
    private static final long FAR_FUTURE = Long.MAX_VALUE;

    // The stored spec carries its own iteration count, so a cheap one here costs nothing: the
    // real DEFAULT_ITERATIONS is exercised by Pbkdf2Test.
    private static final int TEST_ITERATIONS = 1_000;

    private static String spec(final String token, final long notAfter) {
        final byte[] salt = Pbkdf2.newSalt();
        final byte[] hash = Pbkdf2.hash(token, salt, TEST_ITERATIONS);
        final Base64.Encoder b64 = Base64.getEncoder();
        return "pbkdf2$" + TEST_ITERATIONS + "$"
                + b64.encodeToString(salt) + "$" + b64.encodeToString(hash) + "$" + notAfter;
    }

    private static boolean accepts(final ClientTokenStore store, final String token) {
        return new TokenClientAuthenticator(store, () -> 0L)
                .authenticate(WEB, token).authenticated();
    }

    @Test
    @DisplayName("Parses client.<id> records and authenticates the provisioned token")
    void parsesAndAuthenticates(@TempDir final Path dir) throws Exception {
        final Path file = dir.resolve("tokens.conf");
        Files.writeString(file, "client." + WEB.value() + "=" + spec("good", FAR_FUTURE) + "\n");

        try (FileClientTokenStore store = new FileClientTokenStore(file)) {
            assertTrue(accepts(store, "good"));
            assertFalse(accepts(store, "bad"));
        }
    }

    @Test
    @DisplayName("Overlap rotation: a second token added on one line is also accepted")
    void overlapOnReload(@TempDir final Path dir) throws Exception {
        final Path file = dir.resolve("tokens.conf");
        Files.writeString(file, "client." + WEB.value() + "=" + spec("old", FAR_FUTURE) + "\n");

        try (FileClientTokenStore store = new FileClientTokenStore(file)) {
            assertTrue(accepts(store, "old"));

            Thread.sleep(10L);
            Files.writeString(file, "client." + WEB.value() + "="
                    + spec("old", FAR_FUTURE) + " ; " + spec("new", FAR_FUTURE) + "\n");
            store.reloadNow();

            assertTrue(accepts(store, "old"), "Old token still valid during overlap");
            assertTrue(accepts(store, "new"), "New token valid after reload");
        }
    }

    @Test
    @DisplayName("Rotation completes: dropping the old token stops it working")
    void oldTokenRemovedOnReload(@TempDir final Path dir) throws Exception {
        final Path file = dir.resolve("tokens.conf");
        Files.writeString(file, "client." + WEB.value() + "=" + spec("old", FAR_FUTURE) + "\n");

        try (FileClientTokenStore store = new FileClientTokenStore(file)) {
            assertTrue(accepts(store, "old"));

            Thread.sleep(10L);
            Files.writeString(file, "client." + WEB.value() + "=" + spec("new", FAR_FUTURE) + "\n");
            store.reloadNow();

            assertFalse(accepts(store, "old"), "Retired token no longer accepted");
            assertTrue(accepts(store, "new"));
        }
    }
}
