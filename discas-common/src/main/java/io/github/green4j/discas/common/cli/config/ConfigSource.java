/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli.config;

/**
 * Where an effective configuration value came from. Precedence is
 * {@code CLI > ENV > DEFAULT}: a command-line flag overrides the matching
 * {@code DISCAS_*} environment variable, which in turn overrides the built-in
 * default. Surfaced per property by {@link ConfigResolver#describe()} so an
 * operator can see exactly which source won for each value.
 */
public enum ConfigSource {
    CLI,
    ENV,
    DEFAULT
}
