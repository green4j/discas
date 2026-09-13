/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientDescription;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.node.acl.ClientOp;
import io.github.green4j.discas.node.acl.InMemoryClientAcl;
import io.github.green4j.discas.node.audit.AuditConfigSnapshot;
import io.github.green4j.discas.node.audit.AuditHashAlgorithm;
import io.github.green4j.discas.node.audit.AuditOverflow;
import io.github.green4j.discas.node.audit.LoggingAuditLog;
import io.github.green4j.discas.node.audit.NodeAudit;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The trail a node leaves for a client that did some work: a request and a result for every
 * operation, the client id and its description on each, and no value printed whole outside the
 * prefixes that allow it.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Audit trail -- what a node records about its clients")
class AuditTrailTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClientId WEB = ClientId.of("web-1");
    private static final int BUFFER_BYTES = 1 << 16;

    private final ByteArrayOutputStream captured = new ByteArrayOutputStream();

    private EventLoop nodeLoop;
    private DisCasNode node;
    private DisCasClient client;

    private static AuditConfigSnapshot settings() {
        return new AuditConfigSnapshot(AuditConfigSnapshot.SINK_LOG, BUFFER_BYTES,
                AuditOverflow.DROP, 16, 16, 0.25, AuditHashAlgorithm.SHA_256, null, 0,
                64 * 1024, 4096, List.of("lock/"), List.of("lock/"));
    }

    @BeforeEach
    void setup() throws Exception {
        final List<NodeId> members = List.of(NODE);
        nodeLoop = new EventLoop("node-1");
        final PrintStream stream = new PrintStream(captured, true);
        node = new DisCasNode(
                NodeConfig.builder()
                        .nodeId(NODE)
                        .clusterId(ClusterId.of("audit"))
                        .clusterSize(members.size())
                        .auditBufferBytes(BUFFER_BYTES)
                        .build(),
                new InMemoryWal(),
                nodeLoop,
                new InProcessPeerTransport(NODE, members.size(), nodeLoop,
                        InMemoryMembers.ofNodes(members)),
                NodeObserver.NONE,
                NodeAudit.of(new LoggingAuditLog(new Log("1", stream, stream)), settings()));
        node.registerClientMessages(ingress ->
                InProcessClientRegistry.register(NODE, nodeLoop, ingress, node.clusterSize()));
        node.registerClientAcl(InMemoryClientAcl.builder()
                .grant(WEB, "app/", ClientOp.GET, ClientOp.PUT, ClientOp.CAS, ClientOp.DELETE,
                        ClientOp.SCAN)
                .grant(WEB, "lock/", ClientOp.GET, ClientOp.PUT)
                .build());
        node.start();

        client = DisCasClientFactory.createInProcess(WEB,
                ClientDescription.of("Jenkins euc1-blue"), nodeLoop, members,
                io.github.green4j.discas.client.ClientObserver.NONE,
                io.github.green4j.discas.client.DisCasClientConfig.defaults());
        TestAwait.awaitReady(client, "app/__ready__");
    }

    @AfterEach
    void tearDown() {
        close(client);
        close(node);
        InProcessClientRegistry.unregister(NODE);
    }

    private static void close(final AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (final Exception ignored) {
        }
    }

    private List<String> auditLines() throws Exception {
        // The drain is a thread of its own, so the last records are still in flight when the
        // operation that produced them has returned.
        for (int i = 0; i < 100 && !captured.toString(StandardCharsets.UTF_8).contains("RESULT"); i++) {
            Thread.sleep(10L);
        }
        Thread.sleep(50L);
        final List<String> lines = new ArrayList<>();
        for (final String line : captured.toString(StandardCharsets.UTF_8).split("\n")) {
            if (line.contains("AUDIT ")) {
                lines.add(line.substring(line.indexOf("AUDIT ")));
            }
        }
        return lines;
    }

    @Test
    @DisplayName("Every operation leaves a request and a result naming the client and its description")
    void everyOperationIsRecordedWithBothHalves() throws Exception {
        client.put(TestBytes.utf8("app/user/1"), TestBytes.utf8("alice")).get(8, TimeUnit.SECONDS);
        client.get(TestBytes.utf8("app/user/1")).get(8, TimeUnit.SECONDS);
        client.scan(TestBytes.utf8("app/")).get(8, TimeUnit.SECONDS);
        client.delete(TestBytes.utf8("app/user/1")).get(8, TimeUnit.SECONDS);

        final List<String> lines = auditLines();
        for (final String op : List.of("op=PUT", "op=GET", "op=SCAN", "op=DELETE")) {
            assertTrue(lines.stream().anyMatch(l -> l.startsWith("AUDIT REQUEST") && l.contains(op)),
                    op + " request missing from " + lines);
            assertTrue(lines.stream().anyMatch(l -> l.startsWith("AUDIT RESULT") && l.contains(op)),
                    op + " result missing from " + lines);
        }
        assertTrue(lines.stream().allMatch(l -> l.contains("client=web-1 (Jenkins euc1-blue)")),
                lines.toString());
    }

    @Test
    @DisplayName("A refused operation is in the trail with the code the client was given")
    void deniedOperationIsRecorded() throws Exception {
        try {
            client.put(TestBytes.utf8("other/secret"), TestBytes.utf8("x")).get(8, TimeUnit.SECONDS);
        } catch (final ExecutionException expected) {
            // The refusal is the point; what it leaves in the trail is what this asserts.
        }

        final List<String> lines = auditLines();
        assertTrue(lines.stream().anyMatch(
                l -> l.startsWith("AUDIT RESULT") && l.contains("failed")
                        && l.contains("error=ACCESS_DENIED")), lines.toString());
    }

    @Test
    @DisplayName("A value is shown whole only under a prefix that allows it")
    void valuesAreTruncatedOutsideTheAllowedPrefixes() throws Exception {
        final String secret = "0123456789abcdefghijklmnopqrstuvwxyz";
        client.put(TestBytes.utf8("app/user/2"), TestBytes.utf8(secret)).get(8, TimeUnit.SECONDS);
        client.put(TestBytes.utf8("lock/eod"), TestBytes.utf8("owner=euc1")).get(8, TimeUnit.SECONDS);

        final List<String> lines = auditLines();
        final String appLine = lines.stream()
                .filter(l -> l.startsWith("AUDIT REQUEST") && l.contains("op=PUT")
                        && l.contains("value=" + secret.length() + ":"))
                .findFirst().orElseThrow();
        assertFalse(appLine.contains(secret), appLine);
        assertTrue(appLine.contains(".."), appLine);
        // The digest is what makes the truncated line checkable against a candidate value.
        assertTrue(appLine.contains("valueHash=sha-256:"), appLine);

        final String lockLine = lines.stream()
                .filter(l -> l.startsWith("AUDIT REQUEST") && l.contains("key=8:\"lock/eod\""))
                .findFirst().orElseThrow();
        assertTrue(lockLine.contains("value=10:\"owner=euc1\""), lockLine);
    }

    @Test
    void sessionsAreNotRecordedOnTheInProcessPath() throws Exception {
        client.get(TestBytes.utf8("app/user/1")).get(8, TimeUnit.SECONDS);

        // There is no CLIENT_HELLO without a connection, so there is no session to open: the
        // identity arrives with every request instead, and the trail shows exactly that.
        assertEquals(0, auditLines().stream().filter(l -> l.contains("SESSION_")).count());
    }
}
