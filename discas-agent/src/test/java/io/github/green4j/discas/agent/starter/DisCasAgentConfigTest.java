/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent.starter;

import io.github.green4j.discas.common.cli.config.ConfigSource;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("DisCasAgentConfig -- CLI/env resolution")
final class DisCasAgentConfigTest {

    private static final Map<String, String> NO_ENV = Map.of();

    @TempDir
    Path dir;

    @Test
    void defaultsApplyWhenOnlyRequiredGiven() {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--nodes", "1=127.0.0.1:7001,2=127.0.0.1:7002"}, NO_ENV);

        assertEquals("agent", cfg.clientId.value());
        assertEquals(2, cfg.nodes.size());
        assertEquals(new InetSocketAddress("127.0.0.1", 7001), cfg.nodes.get(NodeId.of("1")));
        assertEquals("127.0.0.1", cfg.httpBind.getHostString());
        assertEquals(8500, cfg.httpBind.getPort());
        assertEquals(10, cfg.requestTimeout.getSeconds());
        assertNull(cfg.token);
        assertEquals(ConfigSource.DEFAULT, cfg.sourceOf("client-id"));
        assertEquals(ConfigSource.CLI, cfg.sourceOf("nodes"));
    }

    @Test
    void cliOverridesEnvOverridesDefault() {
        final Map<String, String> env = Map.of(
                "DISCAS_NODES", "9=10.0.0.9:9000",
                "DISCAS_CLIENT_ID", "env-agent",
                "DISCAS_HTTP_BIND", "0.0.0.0:9500");

        // client-id comes from CLI, http-bind from ENV, nodes from CLI (overriding ENV).
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--client-id", "cli-agent", "--nodes", "1=127.0.0.1:7001"}, env);

        assertEquals("cli-agent", cfg.clientId.value());
        assertEquals(ConfigSource.CLI, cfg.sourceOf("client-id"));

        assertEquals(1, cfg.nodes.size());
        assertTrue(cfg.nodes.containsKey(NodeId.of("1")));
        assertEquals(ConfigSource.CLI, cfg.sourceOf("nodes"));

        assertEquals("0.0.0.0", cfg.httpBind.getHostString());
        assertEquals(9500, cfg.httpBind.getPort());
        assertEquals(ConfigSource.ENV, cfg.sourceOf("http-bind"));
    }

    @Test
    void envSuppliesRequiredNodes() {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {}, Map.of("DISCAS_NODES", "1=127.0.0.1:7001"));
        assertEquals(1, cfg.nodes.size());
        assertEquals(ConfigSource.ENV, cfg.sourceOf("nodes"));
    }

    @Test
    void tokenIsMaskedInEffectiveConfig() {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--nodes", "1=127.0.0.1:7001", "--token", "s3cr3t"}, NO_ENV);
        assertEquals("s3cr3t", cfg.token);
        assertFalse(cfg.describe().contains("s3cr3t"),
                "The effective-config table must never echo the token");
    }

    @Test
    void nodesFileIsParsedIntoNodes() throws Exception {
        final Path file = dir.resolve("nodes.conf");
        Files.writeString(file, "node.1 = 127.0.0.1:7001\nnode.2 = 10.0.0.2:7002\n");

        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--nodes-file", file.toString()}, NO_ENV);

        assertTrue(cfg.nodesFromFile());
        assertEquals(file, cfg.nodesFile);
        assertEquals(2, cfg.nodes.size());
        assertEquals(new InetSocketAddress("127.0.0.1", 7001), cfg.nodes.get(NodeId.of("1")));
        assertEquals(new InetSocketAddress("10.0.0.2", 7002), cfg.nodes.get(NodeId.of("2")));
        assertEquals(ConfigSource.CLI, cfg.sourceOf("nodes-file"));
    }

    @Test
    void malformedNodesFileFails() throws Exception {
        final Path file = dir.resolve("bad.conf");
        Files.writeString(file, "node.1 = not-a-host-port\n");
        assertThrows(IllegalArgumentException.class,
                () -> DisCasAgentConfig.resolve(new String[] {"--nodes-file", file.toString()}, NO_ENV));
    }

    @Test
    void tlsDefaultsOff() {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--nodes", "1=127.0.0.1:7001"}, NO_ENV);
        assertFalse(cfg.tls);
        assertNull(cfg.tlsKeystore);
        assertNull(cfg.tlsTruststore);
        assertTrue(cfg.tlsCertRotation);
    }

    @Test
    void mtlsResolvesAllPathsAndMasksPasswords() {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(new String[] {
                "--nodes", "1=127.0.0.1:7001",
                "--tls",
                "--tls-keystore", "/etc/discas/agent.p12", "--tls-keystore-password", "ksPw",
                "--tls-truststore", "/etc/discas/ca.p12", "--tls-truststore-password", "tsPw"}, NO_ENV);

        assertTrue(cfg.tls);
        assertEquals("/etc/discas/agent.p12", cfg.tlsKeystore.toString());
        assertEquals("/etc/discas/ca.p12", cfg.tlsTruststore.toString());
        assertArrayEquals("ksPw".toCharArray(), cfg.tlsKeystorePassword);
        assertArrayEquals("tsPw".toCharArray(), cfg.tlsTruststorePassword);
        assertEquals("****", cfg.displayValueOf("tls-keystore-password"));
        assertEquals("****", cfg.displayValueOf("tls-truststore-password"));
    }

    @Test
    void serverAuthTlsWithTokenResolves() {
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(new String[] {
                "--nodes", "1=127.0.0.1:7001",
                "--tls", "--tls-truststore", "/etc/discas/ca.p12", "--tls-truststore-password", "tsPw",
                "--token", "s3cr3t"}, NO_ENV);
        assertTrue(cfg.tls);
        assertNull(cfg.tlsKeystore);
        assertEquals("s3cr3t", cfg.token);
    }

    static Stream<Arguments> rejectedCommandLines() {
        return Stream.of(
                Arguments.of("no --nodes at all", new String[] {}),
                Arguments.of("a malformed inline entry",
                        new String[] {"--nodes", "1@127.0.0.1:7001"}),
                // --nodes and --nodes-file are two ways to say the same thing, and could disagree.
                Arguments.of("both --nodes and --nodes-file", new String[] {
                    "--nodes", "1=127.0.0.1:7001", "--nodes-file", "/tmp/nodes.conf"}),
                Arguments.of("TLS without a trust store", new String[] {
                    "--nodes", "1=127.0.0.1:7001", "--tls", "--token", "s3cr3t"}),
                Arguments.of("a trust store without its password", new String[] {
                    "--nodes", "1=127.0.0.1:7001", "--tls",
                    "--tls-truststore", "/etc/discas/ca.p12", "--token", "s3cr3t"}),
                Arguments.of("an mTLS keystore without its password", new String[] {
                    "--nodes", "1=127.0.0.1:7001", "--tls",
                    "--tls-keystore", "/etc/discas/agent.p12",
                    "--tls-truststore", "/etc/discas/ca.p12", "--tls-truststore-password", "tsPw"}),
                // Server-auth-only TLS proves the node, not the agent: it still needs a token.
                Arguments.of("server-auth TLS without a token", new String[] {
                    "--nodes", "1=127.0.0.1:7001", "--tls",
                    "--tls-truststore", "/etc/discas/ca.p12", "--tls-truststore-password", "tsPw"}),
                Arguments.of("TLS material without the --tls flag", new String[] {
                    "--nodes", "1=127.0.0.1:7001",
                    "--tls-truststore", "/etc/discas/ca.p12", "--tls-truststore-password", "tsPw"}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedCommandLines")
    void commandLineRejected(final String name, final String[] args) {
        assertThrows(IllegalArgumentException.class, () -> DisCasAgentConfig.resolve(args, NO_ENV));
    }
}
