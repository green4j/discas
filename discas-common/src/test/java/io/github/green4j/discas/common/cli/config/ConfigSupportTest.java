/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli.config;

import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("ConfigSupport -- env names, address and member parsing")
final class ConfigSupportTest {

    @TempDir
    Path dir;

    @Test
    void parseMembersFileReadsNodeProperties() throws Exception {
        final Path file = dir.resolve("nodes.conf");
        Files.writeString(file, "node.1 = 127.0.0.1:7001\nnode.2 = 10.0.0.2:7002\n");

        final Map<NodeId, InetSocketAddress> parsed = ConfigSupport.parseMembersFile(file, "nodes");

        assertEquals(2, parsed.size());
        assertEquals(new InetSocketAddress("127.0.0.1", 7001), parsed.get(NodeId.of("1")));
        assertEquals(new InetSocketAddress("10.0.0.2", 7002), parsed.get(NodeId.of("2")));
    }

    @Test
    void parseMembersPropertiesIgnoresNonNodeKeys() {
        final byte[] bytes = "cluster_id = demo\nnode.1 = 127.0.0.1:7001\n".getBytes(StandardCharsets.UTF_8);
        final Map<NodeId, InetSocketAddress> parsed = ConfigSupport.parseMembersProperties(bytes, "members");
        assertEquals(1, parsed.size());
        assertEquals(new InetSocketAddress("127.0.0.1", 7001), parsed.get(NodeId.of("1")));
    }

    @Test
    void emptyFileIsRejected() {
        final byte[] bytes = "cluster_id = demo\n".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class,
                () -> ConfigSupport.parseMembersProperties(bytes, "nodes"));
    }

    @Test
    void malformedAddressIsRejected() {
        final byte[] bytes = "node.1 = not-a-host-port\n".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class,
                () -> ConfigSupport.parseMembersProperties(bytes, "nodes"));
    }

    @Test
    void envNameDerivation() {
        assertEquals("DISCAS_HTTP_BIND", ConfigSupport.envName("http-bind"));
        assertEquals("DISCAS_NODE_ID", ConfigSupport.envName("node-id"));
    }

    @Test
    void parseAddressRejectsZeroPortUnlessAllowed() {
        assertThrows(IllegalArgumentException.class,
                () -> ConfigSupport.parseAddress("127.0.0.1:0", "bind"));
        final InetSocketAddress ephemeral = ConfigSupport.parseAddress("127.0.0.1:0", "bind", true);
        assertEquals(0, ephemeral.getPort());
    }

    @Test
    void missingFileIsRejected() {
        final Path missing = dir.resolve("does-not-exist.conf");
        assertThrows(IllegalArgumentException.class,
                () -> ConfigSupport.parseMembersFile(missing, "nodes"));
    }
}
