/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.starter;

import io.github.green4j.discas.common.cli.config.ConfigSource;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.node.transport.TcpTransportConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("DisCasNodeConfig - dual CLI/env resolution, precedence, derivation, masking")
class DisCasNodeConfigTest {

    private static final String MEMBERS =
            "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003";

    /** A minimal, valid command line (everything else derived/defaulted). */
    private static String[] baseArgs() {
        return new String[] {
                "--node-id", "1",
                "--cluster-id", "demo",
                "--members", MEMBERS,
                "--client-bind", "127.0.0.1:7001",
                "--wal-dir", "/tmp/discas-1"
        };
    }

    @Test
    @DisplayName("CLI value overrides the matching DISCAS_* env var")
    void cliWinsOverEnv() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(
                baseArgs(),
                Map.of("DISCAS_NODE_ID", "2", "DISCAS_CLUSTER_ID", "from-env"));

        assertEquals("1", cfg.nodeId.value());
        assertEquals(ConfigSource.CLI, cfg.sourceOf("node-id"));
        // cluster-id was only on the command line here, env value must be ignored.
        assertEquals("demo", cfg.clusterId.value());
        assertEquals(ConfigSource.CLI, cfg.sourceOf("cluster-id"));
    }

    @Test
    @DisplayName("Env var supplies a value absent from the command line")
    void envUsedWhenCliAbsent() {
        // node-id + cluster-id come purely from the environment.
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(
                new String[] {
                        "--members", MEMBERS,
                        "--client-bind", "127.0.0.1:7001",
                        "--wal-dir", "/tmp/discas-1"
                },
                Map.of("DISCAS_NODE_ID", "2", "DISCAS_CLUSTER_ID", "demo"));

        assertEquals("2", cfg.nodeId.value());
        assertEquals(ConfigSource.ENV, cfg.sourceOf("node-id"));
        assertEquals("demo", cfg.clusterId.value());
        assertEquals(ConfigSource.ENV, cfg.sourceOf("cluster-id"));
    }

    @Test
    @DisplayName("Untouched properties fall back to their defaults")
    void defaultsWhenNeitherSourceSets() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(baseArgs(), Map.of());

        // Asserted against the transport's own default rather than a value restated here, so
        // the two cannot drift.
        assertEquals(TcpTransportConfig.defaults().maxConnections(),
                cfg.peerTransportConfig.maxConnections());
        assertEquals(ConfigSource.DEFAULT, cfg.sourceOf("peer-max-connections"));
        assertEquals(ClientTransportConfig.defaults().maxConnections(),
                cfg.clientTransportConfig.maxConnections());
        assertEquals(ConfigSource.DEFAULT, cfg.sourceOf("client-max-connections"));
        assertFalse(cfg.tls);
        assertEquals(ConfigSource.DEFAULT, cfg.sourceOf("tls"));
    }

    @Test
    @DisplayName("Integer env values are parsed and typed")
    void envIntegerParsed() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(
                baseArgs(),
                Map.of("DISCAS_PEER_MAX_CONNECTIONS", "7"));

        assertEquals(7, cfg.peerTransportConfig.maxConnections());
        assertEquals(ConfigSource.ENV, cfg.sourceOf("peer-max-connections"));
    }

    @Test
    @DisplayName("cluster-size defaults to the member count; peer-bind to this node's entry")
    void derivedDefaults() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(baseArgs(), Map.of());

        assertEquals(3, cfg.clusterSize);
        assertEquals(ConfigSource.DEFAULT, cfg.sourceOf("cluster-size"));
        assertEquals("127.0.0.1", cfg.peerBind.getHostString());
        assertEquals(9001, cfg.peerBind.getPort());
        assertEquals(ConfigSource.DEFAULT, cfg.sourceOf("peer-bind"));
    }

    @Test
    @DisplayName("Explicit peer-bind and cluster-size override the derived defaults")
    void explicitOverridesDerived() {
        final String[] args = new String[] {
                "--node-id", "1",
                "--cluster-id", "demo",
                "--members", MEMBERS,
                "--client-bind", "127.0.0.1:7001",
                "--wal-dir", "/tmp/discas-1",
                "--peer-bind", "10.0.0.5:6000",
                "--cluster-size", "5"
        };
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(args, Map.of());

        assertEquals(5, cfg.clusterSize);
        assertEquals(ConfigSource.CLI, cfg.sourceOf("cluster-size"));
        assertEquals("10.0.0.5", cfg.peerBind.getHostString());
        assertEquals(6000, cfg.peerBind.getPort());
        assertEquals(ConfigSource.CLI, cfg.sourceOf("peer-bind"));
    }

    @Test
    @DisplayName("describe() masks secret values and never prints the raw password")
    void describeMasksSecrets() {
        final String[] args = new String[] {
                "--node-id", "1",
                "--cluster-id", "demo",
                "--members", MEMBERS,
                "--client-bind", "127.0.0.1:7001",
                "--wal-dir", "/tmp/discas-1",
                "--tls",
                "--tls-keystore", "/etc/discas/ks.p12",
                "--tls-keystore-password", "s3cr3t",
                "--tls-truststore", "/etc/discas/ts.p12",
                "--tls-truststore-password", "s3cr3t"
        };
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(args, Map.of());

        final String table = cfg.describe();
        assertFalse(table.contains("s3cr3t"), "Raw secret must not appear in the config dump");
        assertEquals("****", cfg.displayValueOf("tls-keystore-password"));
        assertTrue(table.contains("tls-keystore-password"));
    }

    @Test
    @DisplayName("The tombstone collection knobs reach NodeConfig, from a flag and from the environment")
    void tombstoneCollectionIsTunable() {
        // Worth asserting because nothing else would notice: a name that does not match the option
        // it was declared under reads nothing and silently leaves the default in place.
        final String[] args = new String[] {
                "--node-id", "1",
                "--cluster-id", "demo",
                "--members", MEMBERS,
                "--client-bind", "127.0.0.1:7001",
                "--wal-dir", "/tmp/discas-1",
                "--tombstone-sweep-interval-seconds", "30"
        };
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(
                args, Map.of("DISCAS_PEER_RESPONSE_TIMEOUT_MS", "2500"));

        assertEquals(Duration.ofSeconds(30), cfg.nodeConfig.tombstoneSweepInterval());
        assertEquals(Duration.ofMillis(2500), cfg.nodeConfig.peerResponseTimeout());
    }

    @Test
    @DisplayName("Help text documents the DISCAS_* env var for every option")
    void helpDocumentsEnvVarForEachOption() {
        // Collapse whitespace so the check is robust to help-text word-wrapping, which may
        // split "(env DISCAS_X)" across lines.
        final String help = DisCasNodeConfig.helpText().replaceAll("\\s+", " ");
        final String[] longNames = {
                "node-id", "cluster-id", "cluster-size",
                "members-file", "members",
                "peer-bind", "client-bind",
                "wal-dir", "wal-max-file-bytes", "snapshot-retention",
                "repair-interval-seconds",
                "peer-max-frame-bytes", "peer-max-queued-out-bytes", "peer-max-rx-buffer-bytes",
                "peer-max-inflight-bytes", "peer-max-connections", "peer-force-reconnect",
                "tls", "tls-keystore", "tls-keystore-password",
                "tls-truststore", "tls-truststore-password", "tls-cert-rotation",
                "client-auth", "client-token-file", "client-token-dir", "client-acl-file",
                "client-tls", "client-tls-keystore", "client-tls-keystore-password",
                "client-tls-truststore", "client-tls-truststore-password",
                "client-tls-cert-rotation"
        };
        for (final String longName : longNames) {
            final String env = "DISCAS_" + longName.toUpperCase(Locale.ROOT).replace('-', '_');
            assertTrue(help.contains("(env " + env + ")"),
                    "Help must document env var " + env + " for option --" + longName);
        }
        // The built-in help flag is not environment-settable.
        assertFalse(help.contains("(env DISCAS_HELP)"), "--help must not advertise an env var");
    }

    @Test
    @DisplayName("Client access defaults to allowall, no ACL and no client TLS")
    void clientAccessDefaults() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(baseArgs(), Map.of());

        assertEquals(ClientAuthMode.ALLOWALL, cfg.clientAuth);
        assertFalse(cfg.clientTls);
        assertNull(cfg.clientAclFile);
        assertNull(cfg.clientTokenFile);
        assertNull(cfg.clientTokenDir);
        assertTrue(cfg.clientAuthorizationDisabled(),
                "An unset --client-acl-file must report authorization as disabled so the "
                        + "starter can warn");
    }

    @Test
    @DisplayName("client-auth is resolved from the environment and is case-insensitive")
    void clientAuthFromEnv() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(
                baseArgs(),
                Map.of("DISCAS_CLIENT_AUTH", "token",
                        "DISCAS_CLIENT_TOKEN_FILE", "/etc/discas/tokens.properties"));

        assertEquals(ClientAuthMode.TOKEN, cfg.clientAuth);
        assertEquals(ConfigSource.ENV, cfg.sourceOf("client-auth"));
        assertEquals("/etc/discas/tokens.properties", cfg.clientTokenFile.toString());
    }

    @Test
    @DisplayName("client-auth=mtls implies client TLS without a separate --client-tls")
    void mtlsImpliesClientTls() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(withArgs(
                "--client-auth", "mtls",
                "--client-tls-keystore", "/tmp/k",
                "--client-tls-keystore-password", "pw",
                "--client-tls-truststore", "/tmp/ts",
                "--client-tls-truststore-password", "pw2"), Map.of());
        assertTrue(cfg.clientTls, "Mtls must imply client TLS without a separate --client-tls");
        assertEquals(ClientAuthMode.MTLS, cfg.clientAuth);
    }

    @Test
    @DisplayName("Server-auth-only client TLS needs a keystore but no trust store")
    void serverAuthOnlyClientTls() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(withArgs(
                "--client-tls", "true",
                "--client-tls-keystore", "/tmp/k",
                "--client-tls-keystore-password", "pw"), Map.of());
        assertTrue(cfg.clientTls);
        assertNull(cfg.clientTlsTruststore, "Only mTLS needs to verify the client side");
    }

    @Test
    @DisplayName("Client TLS passwords are masked in the effective-config table")
    void clientTlsPasswordsMasked() {
        final DisCasNodeConfig cfg = DisCasNodeConfig.resolve(withArgs(
                "--client-tls", "true",
                "--client-tls-keystore", "/tmp/k",
                "--client-tls-keystore-password", "sup3rsecret"), Map.of());

        assertFalse(cfg.describe().contains("sup3rsecret"),
                "The effective-config table must never echo a password");
    }

    static Stream<Arguments> rejectedCommandLines() {
        return Stream.of(
                Arguments.of("both --members-file and --members",
                        withArgs("--members-file", "/tmp/members.conf")),
                Arguments.of("no --client-bind from either source", new String[] {
                    "--node-id", "1", "--cluster-id", "demo", "--members", MEMBERS,
                    "--wal-dir", "/tmp/discas-1"}),
                Arguments.of("node-id absent from the members list", new String[] {
                    "--node-id", "9", "--cluster-id", "demo", "--members", MEMBERS,
                    "--client-bind", "127.0.0.1:7001", "--wal-dir", "/tmp/discas-1"}),
                Arguments.of("peer TLS without key/trust stores", withArgs("--tls")),
                // client-auth=token needs exactly one store: neither of these is one.
                Arguments.of("client-auth=token with no token store",
                        withArgs("--client-auth", "token")),
                Arguments.of("client-auth=token with both token stores",
                        withArgs("--client-auth", "token",
                                "--client-token-file", "/tmp/t", "--client-token-dir", "/tmp/d")),
                // A store nobody will read is a misconfiguration, not something to ignore.
                Arguments.of("a token store without client-auth=token",
                        withArgs("--client-token-file", "/tmp/t")),
                Arguments.of("client TLS stores while client TLS is off",
                        withArgs("--client-tls-keystore", "/tmp/k")),
                Arguments.of("client-auth=mtls without a trust store",
                        withArgs("--client-auth", "mtls",
                                "--client-tls-keystore", "/tmp/k",
                                "--client-tls-keystore-password", "pw")),
                Arguments.of("client TLS without a keystore",
                        withArgs("--client-tls", "true")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedCommandLines")
    @DisplayName("A command line that cannot describe a runnable node is rejected")
    void commandLineRejected(final String name, final String[] args) {
        assertThrows(IllegalArgumentException.class, () -> DisCasNodeConfig.resolve(args, Map.of()));
    }

    /** {@link #baseArgs()} plus {@code extra}. */
    private static String[] withArgs(final String... extra) {
        final String[] base = baseArgs();
        final String[] out = new String[base.length + extra.length];
        System.arraycopy(base, 0, out, 0, base.length);
        System.arraycopy(extra, 0, out, base.length, extra.length);
        return out;
    }
}
