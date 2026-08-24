/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.node.HashedBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ClientAuthorizer -- permissive default, default-deny prefix grants")
class ClientAuthorizerTest {

    private static final ClientId WEB = ClientId.of("web-1");
    private static final ClientId STRANGER = ClientId.of("stranger");

    private EventLoop loop;

    @AfterEach
    void tearDown() {
        if (loop != null) {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(2));
        }
    }

    private static HashedBytes key(final String s) {
        return new HashedBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    private ClientAuthorizer bound(final ClientAcl acl) throws Exception {
        loop = new EventLoop("authz-test");
        loop.start();
        final ClientAuthorizer authorizer = new ClientAuthorizer();
        // bind() marshals the (replayed) snapshot onto the loop; enqueue a follow-up task and
        // wait for it so the volatile snapshot is guaranteed applied (FIFO) before we assert.
        authorizer.bind(acl, loop);
        final CountDownLatch applied = new CountDownLatch(1);
        loop.execute(applied::countDown);
        applied.await(2, TimeUnit.SECONDS);
        return authorizer;
    }

    @Test
    @DisplayName("Unbound authorizer is permissive -- allows every op on every key")
    void permissiveWhenUnbound() {
        final ClientAuthorizer authorizer = new ClientAuthorizer();
        assertTrue(authorizer.allow(WEB, ClientOp.PUT, key("anything")));
        assertTrue(authorizer.allow(null, ClientOp.GET, key("also/anything")));
    }

    @Test
    @DisplayName("Bound: a grant permits its ops on its prefix and denies others (default-deny)")
    void grantsAndDefaultDeny() throws Exception {
        final ClientAcl acl = InMemoryClientAcl.builder()
                .grant(WEB, "app/", ClientOp.GET, ClientOp.PUT)
                .build();
        final ClientAuthorizer authorizer = bound(acl);

        assertTrue(authorizer.allow(WEB, ClientOp.GET, key("app/user/1")));
        assertTrue(authorizer.allow(WEB, ClientOp.PUT, key("app/user/1")));
        assertFalse(authorizer.allow(WEB, ClientOp.DELETE, key("app/user/1")),
                "Op not in the grant is denied");
        assertFalse(authorizer.allow(WEB, ClientOp.GET, key("other/x")),
                "Key outside the prefix is denied");
    }

    @Test
    @DisplayName("Bound: a client with no policy is denied everything (default-deny)")
    void unknownClientDenied() throws Exception {
        final ClientAcl acl = InMemoryClientAcl.builder()
                .grant(WEB, "app/", ClientOp.GET)
                .build();
        final ClientAuthorizer authorizer = bound(acl);

        assertFalse(authorizer.allow(STRANGER, ClientOp.GET, key("app/x")));
        assertFalse(authorizer.allow(null, ClientOp.GET, key("app/x")));
    }

    @Test
    @DisplayName("Bound: an empty-prefix grant covers every key (no reserved namespace)")
    void emptyPrefixGrantCoversAllKeys() throws Exception {
        final ClientAcl acl = InMemoryClientAcl.builder()
                .grant(WEB, "", ClientOp.GET, ClientOp.PUT) // empty prefix = all keys
                .build();
        final ClientAuthorizer authorizer = bound(acl);

        assertTrue(authorizer.allow(WEB, ClientOp.GET, key("anything")));
        assertTrue(authorizer.allow(WEB, ClientOp.GET, key("__system/whatever")),
                "No key namespace is special-cased -- an empty-prefix grant covers it too");
    }
}
