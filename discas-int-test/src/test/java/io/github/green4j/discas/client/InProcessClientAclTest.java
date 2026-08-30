/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.acl.ClientOp;
import io.github.green4j.discas.node.acl.InMemoryClientAcl;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What a bound client ACL means on the in-process path, where there is no connection, no
 * CLIENT_HELLO and therefore no credential: the transport hands the node the client id its
 * own {@code DisCasClient} was constructed with, as the trusted one.
 * <p>
 * The question this settles is whether authorization still runs there. It is the precondition
 * for a colocated client that talks in-process to its own node and over TCP to the rest: if
 * authorization were skipped in-process, that client would enforce a different policy depending
 * on which coordinator a key happens to hash to.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("In-process client ACL -- authorization applies, authentication does not exist")
class InProcessClientAclTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClientId WEB = ClientId.of("web-1");
    private static final ClientId ROGUE = ClientId.of("rogue-1");

    private EventLoop nodeLoop;
    private DisCasNode node;
    private DisCasClient web;
    private DisCasClient rogue;
    private DisCasClient impostor;

    @BeforeEach
    void setup() throws Exception {
        final List<NodeId> members = List.of(NODE);
        nodeLoop = new EventLoop("node-1");
        node = new DisCasNode(
                NodeConfig.builder()
                        .nodeId(NODE)
                        .clusterId(ClusterId.of("acl-in-process"))
                        .clusterSize(members.size())
                        .build(),
                new InMemoryWal(),
                nodeLoop,
                new InProcessPeerTransport(NODE, members.size(), nodeLoop,
                        InMemoryMembers.ofNodes(members)));
        node.registerClientMessages(ingress ->
                InProcessClientRegistry.register(NODE, nodeLoop, ingress, node.clusterSize()));

        // web-1 may read and write under app/ and nowhere else. rogue-1 is named by no grant at
        // all, which under default-deny is the more interesting of the two.
        node.registerClientAcl(InMemoryClientAcl.builder()
                .grant(WEB, "app/", ClientOp.GET, ClientOp.PUT, ClientOp.CAS)
                .build());
        node.start();

        web = DisCasClientFactory.create(WEB, new InProcessClientBootstrap(members));
        rogue = DisCasClientFactory.create(ROGUE, new InProcessClientBootstrap(members));
        // Same identity as `web`, built by a caller that presented nothing to claim it.
        impostor = DisCasClientFactory.create(WEB, new InProcessClientBootstrap(members));

        TestAwait.awaitReady(web, "app/__ready__");
    }

    @AfterEach
    void tearDown() {
        closeQuietly(web);
        closeQuietly(rogue);
        closeQuietly(impostor);
        closeQuietly(node);
        InProcessClientRegistry.unregister(NODE);
    }

    private static void closeQuietly(final AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (final Exception ignored) {
        }
    }

    @Test
    @DisplayName("A granted prefix is writable in-process")
    void grantedPrefixIsWritable() throws Exception {
        web.put(TestBytes.utf8("app/user/1"), TestBytes.utf8("alice")).get(8, TimeUnit.SECONDS);
        assertEquals("alice", TestBytes.string(
                web.get(TestBytes.utf8("app/user/1")).get(8, TimeUnit.SECONDS).value()));
    }

    @Test
    @DisplayName("An out-of-prefix write is denied on the in-process path too")
    void outOfPrefixWriteIsDenied() {
        assertEquals(ClientErrorCode.ACCESS_DENIED, codeOf(assertThrows(ExecutionException.class,
                () -> web.put(TestBytes.utf8("other/secret"), TestBytes.utf8("x"))
                        .get(8, TimeUnit.SECONDS))));
    }

    /**
     * The load-bearing case. A client the ACL never names gets nothing, which is what makes
     * authorization -- as opposed to authentication -- real on this path.
     */
    @Test
    @DisplayName("A client no grant names is denied everything")
    void unlistedClientIsDeniedEverything() {
        assertEquals(ClientErrorCode.ACCESS_DENIED, codeOf(assertThrows(ExecutionException.class,
                () -> rogue.put(TestBytes.utf8("app/user/2"), TestBytes.utf8("mallory"))
                        .get(8, TimeUnit.SECONDS))));
        assertEquals(ClientErrorCode.ACCESS_DENIED, codeOf(assertThrows(ExecutionException.class,
                () -> rogue.get(TestBytes.utf8("app/user/2")).get(8, TimeUnit.SECONDS))));
    }

    /**
     * The other half of the same fact: the id is asserted, never proved. Any code in this JVM may
     * construct a client under any identity and receive that identity's grants. In-process that is
     * the honest reading -- the caller is already inside the trust boundary -- and it is the
     * sentence a colocated transport has to carry into its documentation.
     */
    @Test
    @DisplayName("Any code in the JVM may claim any identity")
    void identityIsAssertedNotProved() throws Exception {
        impostor.put(TestBytes.utf8("app/user/3"), TestBytes.utf8("claimed"))
                .get(8, TimeUnit.SECONDS);
        assertEquals("claimed", TestBytes.string(
                web.get(TestBytes.utf8("app/user/3")).get(8, TimeUnit.SECONDS).value()));
    }

    private static ClientErrorCode codeOf(final ExecutionException e) {
        return assertInstanceOf(DisCasOperationException.class, e.getCause()).code();
    }
}
