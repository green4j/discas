/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.node.HashedBytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("FileClientAcl -- parse grants, reload")
class FileClientAclTest {

    private static final ClientId WEB = ClientId.of("web-1");

    private static HashedBytes key(final String s) {
        return new HashedBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Parses acl.<id> = prefix:ops grants with op letters")
    void parsesGrants(@TempDir final Path dir) throws Exception {
        final Path file = dir.resolve("acl.conf");
        Files.writeString(file, "acl.web-1 = app/:GP ; report/:GS\n");

        try (FileClientAcl acl = new FileClientAcl(file)) {
            final ClientPolicy policy = acl.snapshot().policy(WEB);
            assertTrue(policy.allows(ClientOp.GET, key("app/x")));
            assertTrue(policy.allows(ClientOp.PUT, key("app/x")));
            assertFalse(policy.allows(ClientOp.DELETE, key("app/x")));
            assertTrue(policy.allows(ClientOp.SCAN, key("report/y")));
            assertFalse(policy.allows(ClientOp.PUT, key("report/y")));
            assertFalse(policy.allows(ClientOp.GET, key("other/z")));
        }
    }
}
