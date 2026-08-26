/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.node.HashedBytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("FileClientAcl -- parse grants, reload")
class FileClientAclTest {

    private static final ClientId WEB = ClientId.of("web-1");

    private static HashedBytes key(final String s) {
        return new HashedBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Reordering grants and lines is not a change, and is reported as applying nothing")
    void cosmeticEditsAreNotChanges(@TempDir final Path dir) throws Exception {
        // Evaluation is a plain OR over the grants, so their order is unobservable in any decision
        // the node makes -- and what the file cannot express, a reload must not report as a change.
        // Otherwise every tidy-up of the file republishes the table and reads, in the log and the
        // metrics, as a policy change that nobody made.
        final Path file = dir.resolve("acl.conf");
        Files.writeString(file, "acl.web-1 = app/:GP ; report/:GS\nacl.reporter = report/:GS\n");

        final List<String> events = new ArrayList<>();
        try (FileClientAcl acl = new FileClientAcl(file, recording(events))) {
            events.clear();
            Files.writeString(file,
                    "acl.reporter  =  report/:GS \n"           // lines swapped, spaces added
                            + "acl.web-1 = report/:SG ; app/:PG\n");  // grants and ops swapped
            acl.reloadNow();

            assertEquals(List.of("unchanged"), events,
                    "Same policy written differently is not a new revision");
        }
    }

    /**
     * Records which event fired, and nothing of what it said: the wording is for an operator, and a
     * test that matched it would break on every improvement while checking nothing.
     */
    private static ReloadObserver recording(final List<String> events) {
        return new ReloadObserver() {
            @Override
            public void reloaded(final String source, final String detail) {
                events.add("reloaded");
            }

            @Override
            public void reloadUnchanged(final String source, final String detail) {
                events.add("unchanged");
            }
        };
    }

    @Test
    @DisplayName("A file whose keys are all mis-prefixed is refused, not read as an empty policy")
    void misPrefixedKeysAreRefused(@TempDir final Path dir) throws Exception {
        // "alc." rather than "acl." parses perfectly well as java properties and yields no entries
        // at all -- which, taken as a policy, denies every client every operation. A typo must not
        // be able to do that silently, so the file is refused the way a members file with no node.*
        // in it is. The initial load has no last good value to keep, so it fails fast.
        final Path file = dir.resolve("acl.conf");
        Files.writeString(file, "alc.web-1 = app/:GP\n");

        assertThrows(RuntimeException.class, () -> new FileClientAcl(file).close(),
                "A file with properties but no acl.* entry is not a policy");
    }

    @Test
    @DisplayName("An edit that mis-prefixes every key keeps the revision in force")
    void misPrefixedEditKeepsTheLastGoodRevision(@TempDir final Path dir) throws Exception {
        final Path file = dir.resolve("acl.conf");
        Files.writeString(file, "acl.web-1 = app/:GP\n");

        try (FileClientAcl acl = new FileClientAcl(file)) {
            Files.writeString(file, "alc.web-1 = app/:GP\n");
            acl.reloadNow();

            assertTrue(acl.snapshot().policy(WEB).allows(ClientOp.GET, key("app/x")),
                    "The last good revision stands, as for any other edit that does not parse");
        }
    }

    @Test
    @DisplayName("A file with nothing in it is a policy: nobody may do anything")
    void anEmptyFileDeniesEveryone(@TempDir final Path dir) throws Exception {
        // The other side of the rule above, and the reason it is scoped to files that do have
        // properties: an operator who empties the file means it, and gets default-deny.
        final Path file = dir.resolve("acl.conf");
        Files.writeString(file, "# every grant withdrawn\n");

        try (FileClientAcl acl = new FileClientAcl(file)) {
            assertNull(acl.snapshot().policy(WEB), "No policy for anyone, and no complaint");
        }
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
