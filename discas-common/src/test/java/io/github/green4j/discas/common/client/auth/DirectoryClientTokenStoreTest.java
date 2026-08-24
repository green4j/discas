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

@DisplayName("DirectoryClientTokenStore -- one file per client, add/remove on rescan")
class DirectoryClientTokenStoreTest {

    private static final long FAR_FUTURE = Long.MAX_VALUE;

    // The stored spec carries its own iteration count, so a cheap one here costs nothing: the
    // real DEFAULT_ITERATIONS is exercised by Pbkdf2Test.
    private static final int TEST_ITERATIONS = 1_000;

    private static String spec(final String token) {
        final byte[] salt = Pbkdf2.newSalt();
        final byte[] hash = Pbkdf2.hash(token, salt, TEST_ITERATIONS);
        final Base64.Encoder b64 = Base64.getEncoder();
        return "pbkdf2$" + TEST_ITERATIONS + "$"
                + b64.encodeToString(salt) + "$" + b64.encodeToString(hash) + "$" + FAR_FUTURE;
    }

    private static boolean accepts(final ClientTokenStore store, final ClientId id, final String token) {
        return new TokenClientAuthenticator(store, () -> 0L).authenticate(id, token).authenticated();
    }

    @Test
    @DisplayName("Parses <clientId>.token files and reflects added/removed clients on rescan")
    void parsesAndRescans(@TempDir final Path dir) throws Exception {
        Files.writeString(dir.resolve("web-1.token"), spec("one") + "\n");

        try (DirectoryClientTokenStore store = new DirectoryClientTokenStore(dir)) {
            assertTrue(accepts(store, ClientId.of("web-1"), "one"));
            assertFalse(accepts(store, ClientId.of("web-2"), "two"), "web-2 not provisioned yet");

            // Add a second client by dropping a file.
            Files.writeString(dir.resolve("web-2.token"), spec("two") + "\n");
            store.reloadNow();
            assertTrue(accepts(store, ClientId.of("web-2"), "two"));
            assertTrue(accepts(store, ClientId.of("web-1"), "one"), "web-1 still present");

            // Revoke web-1 by deleting its file.
            Files.delete(dir.resolve("web-1.token"));
            store.reloadNow();
            assertFalse(accepts(store, ClientId.of("web-1"), "one"), "web-1 removed with its file");
            assertTrue(accepts(store, ClientId.of("web-2"), "two"));
        }
    }

    @Test
    @DisplayName("Overlap rotation: two records in one client file are both accepted")
    void overlapInOneFile(@TempDir final Path dir) throws Exception {
        Files.writeString(dir.resolve("web-1.token"), spec("old") + "\n" + spec("new") + "\n");

        try (DirectoryClientTokenStore store = new DirectoryClientTokenStore(dir)) {
            assertTrue(accepts(store, ClientId.of("web-1"), "old"));
            assertTrue(accepts(store, ClientId.of("web-1"), "new"));
        }
    }
}
