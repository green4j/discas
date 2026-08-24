/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.runbook;

import io.github.green4j.discas.admin.runbook.ClusterPlan.ClientAuth;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The decisions a plan makes on its own -- the ones the runbook then merely repeats.
 *
 * <p>Nothing here reads a prompt or asserts a line of {@code RUN.md}: the dialogue is console
 * interaction and the runbook is prose, and a test over either fails when somebody improves a
 * sentence. What is worth pinning is what the plan <em>decides</em>.
 */
@DisplayName("ClusterPlan -- what a plan settles before anything is written")
class ClusterPlanTest {

    @ParameterizedTest(name = "{0}")
    @CsvSource({
        // A client certificate cannot be presented over a plaintext connection, so mtls settles
        // this by itself -- not a second choice an operator could get wrong, and not a branch
        // every renderer repeats.
        "MTLS,     true",
        "ALLOWALL, false",
    })
    @DisplayName("Only mtls settles the client port's TLS by itself")
    void mtlsImpliesClientTls(final ClientAuth auth, final boolean expected) {
        assertEquals(expected, base().clientAuth(auth).clientTls(false).build().clientTls());
    }

    @Test
    @DisplayName("Host paths are joined, not concatenated by whoever renders them")
    void pathsAreJoinedOnce() {
        final ClusterPlan plan = base()
                .dataDirectory("/srv/discas/")
                .configDirectory("/etc/discas//")
                .build();

        assertEquals("/srv/discas/1", plan.dataDirectoryOf(NodeId.of("1")));
        assertEquals("/etc/discas/members.conf", plan.configFile("members.conf"));
    }

    @Test
    @DisplayName("An auth mode is spelled the node's way or not accepted")
    void authModesAreTheNodesOwn() {
        assertEquals(ClientAuth.TOKEN, ClientAuth.of("token"));
        assertEquals("mtls", ClientAuth.MTLS.cliName());
        assertThrows(IllegalArgumentException.class, () -> ClientAuth.of("password"));
    }

    @Test
    @DisplayName("A plan without a cluster or without members is not a plan")
    void requiredFieldsAreRequired() {
        final Map<NodeId, InetSocketAddress> members = Collections.singletonMap(
                NodeId.of("1"), new InetSocketAddress("10.0.0.1", 7001));

        assertThrows(IllegalArgumentException.class,
                () -> ClusterPlan.builder().members(members).build());
        assertThrows(IllegalArgumentException.class,
                () -> ClusterPlan.builder().clusterId(ClusterId.of("c")).build());
        assertThrows(IllegalArgumentException.class, () -> base().clientPort(0));
    }

    private static ClusterPlan.Builder base() {
        return ClusterPlan.builder()
                .clusterId(ClusterId.of("new-cluster"))
                .members(Collections.singletonMap(
                        NodeId.of("1"), new InetSocketAddress("10.0.0.1", 7001)));
    }
}
