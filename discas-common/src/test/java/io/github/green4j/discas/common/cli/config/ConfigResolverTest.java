/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli.config;

import io.github.green4j.discas.common.cli.GetOpts;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ConfigResolver -- CLI > ENV > default precedence")
final class ConfigResolverTest {

    private static final Map<String, String> NO_ENV = Map.of();

    private static GetOpts newOpts(final String... args) {
        final GetOpts opts = new GetOpts("prog", "test")
                .stringOpt("name", null, "a name")
                .stringOpt("count", null, "a count")
                .stringOpt("token", null, "a secret")
                .stringOpt("flag", null, "a flag").choices("true", "false").optionalArg("true");
        opts.parseOrThrow(args);
        return opts;
    }

    @Test
    void cliOverridesEnvOverridesDefault() {
        final Map<String, String> env = Map.of(
                "DISCAS_NAME", "env-name",
                "DISCAS_COUNT", "7");
        final ConfigResolver r = new ConfigResolver("prog", newOpts("--name", "cli-name"), env);

        assertEquals("cli-name", r.optional("name", "def"));
        assertEquals(ConfigSource.CLI, r.sourceOf("name"));

        assertEquals(7, r.integer("count", 1));
        assertEquals(ConfigSource.ENV, r.sourceOf("count"));

        assertTrue(r.bool("flag", true));
        assertEquals(ConfigSource.DEFAULT, r.sourceOf("flag"));
    }

    @Test
    void requiredMissingThrows() {
        final ConfigResolver r = new ConfigResolver("prog", newOpts(), NO_ENV);
        assertThrows(IllegalArgumentException.class, () -> r.required("name"));
    }

    @Test
    void secretIsMaskedInDescribe() {
        final ConfigResolver r = new ConfigResolver("prog", newOpts("--token", "s3cr3t"), NO_ENV);
        assertEquals("s3cr3t", r.secret("token"));
        // The property is that the secret is absent from what gets printed -- a mask that changes
        // shape is not a regression, a secret that leaks is.
        assertFalse(r.describe().contains("s3cr3t"));
    }

    @Test
    void unsetSecretHasNoValue() {
        final ConfigResolver r = new ConfigResolver("prog", newOpts(), NO_ENV);
        assertNull(r.secret("token"));
        assertEquals(ConfigSource.DEFAULT, r.sourceOf("token"));
    }

    @Test
    void integerRejectsNonInteger() {
        final ConfigResolver r = new ConfigResolver("prog", newOpts("--count", "abc"), NO_ENV);
        assertThrows(IllegalArgumentException.class, () -> r.integer("count", 1));
    }
}
