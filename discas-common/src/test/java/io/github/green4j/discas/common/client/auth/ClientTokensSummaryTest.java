/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.Hex;
import io.github.green4j.discas.common.identity.ClientId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The one thing worth pinning about a summary, and the same thing the effective-configuration table
 * is pinned on: <b>the secret is not in it</b>. Nothing here checks wording or layout -- those are
 * for an operator to read and for us to improve.
 *
 * <p>A token store is not a file with a secret in it, it is the secret, and its summary goes to a
 * log that more people can read than can read the file. The material is checked in every encoding
 * it could plausibly be rendered in, because a leak through {@code Base64} would pass a test that
 * only looked for the raw bytes.
 */
@DisplayName("ClientTokens -- the reload summary of a file that is itself the secret")
class ClientTokensSummaryTest {

    @Test
    @DisplayName("No part of a credential reaches the summary, in any encoding")
    void noPartOfACredentialReachesTheSummary() {
        final byte[] salt = "s3cr3t-salt".getBytes(StandardCharsets.UTF_8);
        final byte[] hash = "s3cr3t-hash".getBytes(StandardCharsets.UTF_8);
        final ClientTokens tokens = new ClientTokens(Map.of(ClientId.of("web-1"),
                List.of(new TokenRecord(salt, 210000, hash, 1_800_000_000_000L))));

        final String summary = tokens.summary();

        for (final String material : List.of(
                new String(salt, StandardCharsets.UTF_8),
                new String(hash, StandardCharsets.UTF_8),
                Base64.getEncoder().encodeToString(salt),
                Base64.getEncoder().encodeToString(hash),
                Base64.getUrlEncoder().withoutPadding().encodeToString(salt),
                Base64.getUrlEncoder().withoutPadding().encodeToString(hash),
                Hex.encode(salt),
                Hex.encode(hash))) {
            assertFalse(summary.contains(material),
                    "credential material reached the reload log: " + summary);
        }
    }
}
