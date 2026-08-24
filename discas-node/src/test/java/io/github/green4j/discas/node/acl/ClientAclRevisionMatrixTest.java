/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.node.Await;
import io.github.green4j.discas.node.HashedBytes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Editing policy while the cluster is serving: every kind of edit, against a node that has applied
 * it and a node that has not.
 *
 * <p>The rule the whole table follows is that <b>a decision is a function of one revision</b> -- the
 * one in force at that node when the request arrives. A snapshot is immutable and installed whole,
 * so an edit is either invisible to a request or wholly visible to it.
 *
 * <p>That immutability is why the table below tests <em>which</em> revision decides rather than
 * whether one decision can mix two: with a snapshot that cannot change under a reader, no
 * interleaving produces a mixture, and a test written for one would be demonstrating the absence of
 * a code path rather than the presence of a guarantee.
 *
 * <p>Two columns rather than one, because the second is the honest part: a client failing over
 * between coordinators mid-rollout meets two revisions, and the pair of answers <em>is</em> the
 * observable behaviour. Revocation is bounded by how fast the file reaches every node, not by zero,
 * and a test that asserted one global answer would be asserting a guarantee this system does not
 * make.
 *
 * <p>The row that matters most is the unparseable one. A broken edit must leave the revision in
 * force untouched -- failing open would hand out access nobody granted, and failing closed would
 * take a serving cluster off the air over a typo.
 */
@DisplayName("Client ACL -- an edit, and the two revisions a failover can meet")
class ClientAclRevisionMatrixTest {

    private static final ClientId WEB = ClientId.of("web-1");
    /** Granted from the start, so every row has a control that must not move. */
    private static final String GRANTED = "app/x";
    /** Granted only by the edit, or revoked by it, depending on the row. */
    private static final String SUBJECT = "report/y";
    /** In no policy any row writes, so it is denied the moment a revision is in force. */
    private static final ClientId NOBODY = ClientId.of("nobody");

    @ParameterizedTest(name = "{0}: before={1} after={2}, untouched grant after={3}")
    @CsvSource({
        // edit, the subject key at the node that has NOT reloaded, at the one that HAS, and the
        // grant the edit did not mention -- which moves in exactly one row, and says so.
        "UNCHANGED,      DENIED,  DENIED,  ALLOWED",
        "GRANT_ADDED,    DENIED,  ALLOWED, ALLOWED",
        "GRANT_REVOKED,  ALLOWED, DENIED,  ALLOWED",
        // An entry with no grants at all is a valid policy, not a broken file: it says this client
        // may do nothing. Worth its own row because it is the shape an operator writes to cut a
        // client off, and it must be applied rather than mistaken for damage.
        // ... and here it moves: an empty entry is not "revoke this prefix", it is "this client has
        // no policy". Everything it held goes, which is the point of writing one.
        "CLIENT_EMPTIED, ALLOWED, DENIED,  DENIED",
        // A file that will not parse leaves the revision in force exactly as it was, so both nodes
        // answer what they answered before -- the point being that they answer the *same* thing, and
        // that it is neither "everything" nor "nothing".
        "UNPARSEABLE,    ALLOWED, ALLOWED, ALLOWED",
    })
    void everyEditAgainstBothRevisions(final String edit, final String before, final String after,
                                       final String untouchedAfter, @TempDir final Path dir)
            throws Exception {
        final Path file = dir.resolve("acl.conf");
        Files.writeString(file, startingPolicy(edit));

        // Two nodes reading the same file, each with its own loader -- which is what a cluster is.
        // Only one of them is told to reload, and that is the whole of "mid-rollout": the file is
        // one thing, the revisions in force are two.
        try (FileClientAcl staleAcl = new FileClientAcl(file);
                FileClientAcl freshAcl = new FileClientAcl(file)) {
            final ClientAuthorizer stale = boundAuthorizer(staleAcl);
            final ClientAuthorizer fresh = boundAuthorizer(freshAcl);

            assertEquals(before.equals("ALLOWED"), stale.allow(WEB, ClientOp.PUT, key(SUBJECT)),
                    "Before the edit, " + edit);

            rewrite(file, editedPolicy(edit));
            freshAcl.reloadNow();
            awaitApplied(freshAcl, fresh);

            assertEquals(after.equals("ALLOWED"), fresh.allow(WEB, ClientOp.PUT, key(SUBJECT)),
                    "After the edit, " + edit);
            assertEquals(before.equals("ALLOWED"), stale.allow(WEB, ClientOp.PUT, key(SUBJECT)),
                    "The node that has not applied the edit must still answer what it did: a"
                            + " revision is not partially visible");

            // The grant the edit did not name. At the stale node it cannot have moved at all; at the
            // fresh one it moves only where the edit really did reach it, which the table states
            // rather than assumes.
            assertTrue(stale.allow(WEB, ClientOp.PUT, key(GRANTED)),
                    "A node that has not reloaded cannot have lost a grant");
            assertEquals(untouchedAfter.equals("ALLOWED"), fresh.allow(WEB, ClientOp.PUT, key(GRANTED)),
                    "The grant the edit did not mention, after " + edit);
        }
    }

    @Test
    @DisplayName("An unparseable edit keeps the revision in force -- it does not fail open or shut")
    void unparseableEditIsNeitherOpenNorShut(@TempDir final Path dir) throws Exception {
        // Stated separately from the row above because the row can only show that the answers did
        // not move; this shows what they did not move *to*. Failing open would grant what nobody
        // wrote, failing shut would take a serving cluster down over a typo, and a test that only
        // compared before with after would pass in the first case.
        final Path file = dir.resolve("acl.conf");
        Files.writeString(file, "acl.web-1 = app/:GP ; report/:GP\n");

        try (FileClientAcl acl = new FileClientAcl(file)) {
            final ClientAuthorizer authorizer = boundAuthorizer(acl);
            rewrite(file, "acl.web-1 = app/\n");
            acl.reloadNow();

            assertTrue(authorizer.allow(WEB, ClientOp.PUT, key(SUBJECT)),
                    "The last good revision still grants what it granted");
            assertFalse(authorizer.allow(WEB, ClientOp.PUT, key("other/z")),
                    "And still denies what it denied -- a broken file is not an open door");
            assertFalse(authorizer.allow(ClientId.of("nobody"), ClientOp.GET, key(GRANTED)),
                    "Nor does it make the node permissive for a client with no policy at all");
        }
    }

    /** The policy in force before the edit. */
    private static String startingPolicy(final String edit) {
        switch (edit) {
            case "GRANT_REVOKED":
            case "CLIENT_EMPTIED":
            case "UNPARSEABLE":
                return "acl.web-1 = app/:GP ; report/:GP\n";
            default:
                return "acl.web-1 = app/:GP\n";
        }
    }

    /** What the operator wrote. */
    private static String editedPolicy(final String edit) {
        switch (edit) {
            case "UNCHANGED":
                return "acl.web-1 = app/:GP\n";
            case "GRANT_ADDED":
                return "acl.web-1 = app/:GP ; report/:GP\n";
            case "GRANT_REVOKED":
                return "acl.web-1 = app/:GP\n";
            case "CLIENT_EMPTIED":
                // Parses, and means it: an entry with no grants leaves the client with no policy.
                return "acl.web-1 =\n";
            case "UNPARSEABLE":
                // A grant with no ops after it. The parser refuses the file rather than guessing.
                return "acl.web-1 = app/\n";
            default:
                throw new IllegalArgumentException(edit);
        }
    }

    /**
     * A node holding its own copy of the current revision. Two of them stand in for two coordinators
     * a client can reach: one is told about the reload, the other is not.
     */
    private static ClientAuthorizer boundAuthorizer(final FileClientAcl acl) throws Exception {
        final EventLoop loop = new EventLoop("acl-" + System.nanoTime());
        loop.start();
        final ClientAuthorizer authorizer = new ClientAuthorizer();
        authorizer.bind(acl, loop);
        // Wait on a client no policy grants, never on one that is granted. An authorizer that has
        // not received a revision yet holds no snapshot, and no snapshot means "unconfigured", which
        // allows *everything* -- so asking whether a granted client is allowed answers yes before
        // binding has taken effect, and the wait passes on its first poll without waiting for
        // anything. Denial is the only answer that cannot be produced by the unconfigured state.
        Await.until("the authorizer to receive its first revision", () -> {
            if (authorizer.allow(NOBODY, ClientOp.GET, key(GRANTED))) {
                throw new IllegalStateException(
                        "Still unconfigured: no revision has reached the authorizer");
            }
        });
        return authorizer;
    }

    /** The reload is marshalled onto a loop, so the fresh node has it a moment after reloadNow(). */
    private static void awaitApplied(final FileClientAcl acl, final ClientAuthorizer fresh)
            throws Exception {
        final ClientPolicy expected = acl.snapshot().policy(WEB);
        Await.until("the reloaded revision to reach the node", () -> {
            final boolean nodeAllows = fresh.allow(WEB, ClientOp.PUT, key(SUBJECT));
            final boolean fileAllows = expected != null && expected.allows(ClientOp.PUT, key(SUBJECT));
            if (nodeAllows != fileAllows) {
                throw new IllegalStateException("node=" + nodeAllows + " file=" + fileAllows);
            }
        });
    }

    /**
     * Write and make the change visible to the loader, which skips a file whose size and
     * last-modified are what they were. Setting the timestamp forward is deterministic where
     * sleeping for the filesystem's granularity is only likely.
     */
    private static void rewrite(final Path file, final String contents) throws Exception {
        final FileTime before = Files.getLastModifiedTime(file);
        Files.writeString(file, contents);
        Files.setLastModifiedTime(file,
                FileTime.fromMillis(before.toMillis() + Duration.ofSeconds(1).toMillis()));
    }

    private static HashedBytes key(final String text) {
        return new HashedBytes(text.getBytes(StandardCharsets.UTF_8));
    }
}
