/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("FileMembers -- parse, reload gates, safe publication")
class FileMembersTest {

    private static Path write(final Path dir, final String content) throws Exception {
        final Path file = dir.resolve("members.conf");
        Files.writeString(file, content);
        return file;
    }

    @Test
    @DisplayName("Parses node.<id>=host:port members (no cluster_id in the file)")
    void parsesMembers(@TempDir final Path dir) throws Exception {
        final Path file = write(dir, "node.1=host-1:9001\nnode.2=host-2:9002\n");
        try (FileMembers members = new FileMembers(file)) {
            final MembersSnapshot<TcpMemberInfo> snap = members.snapshot();
            assertEquals(Set.of(NodeId.of("1"), NodeId.of("2")), snap.ids());
            assertEquals("host-2", snap.get(NodeId.of("2")).host());
            assertEquals(9002, snap.get(NodeId.of("2")).port());
        }
    }

    @Test
    @DisplayName("Subscribe replays the initial, then a changed file notifies")
    void changedFileSwapsAndNotifies(@TempDir final Path dir) throws Exception {
        final Path file = write(dir, "node.1=host-1:9001\nnode.2=host-2:9002\n");
        try (FileMembers members = new FileMembers(file)) {
            final AtomicReference<MembersSnapshot> notified = new AtomicReference<>();
            final AtomicInteger count = new AtomicInteger();
            members.addListener(snap -> {
                notified.set(snap);
                count.incrementAndGet();
            });
            // replay-on-subscribe delivered the current snapshot as update #0
            assertEquals(1, count.get(), "Subscribe replays the current snapshot");
            assertEquals(Set.of(NodeId.of("1"), NodeId.of("2")), notified.get().ids());

            Files.writeString(file, "node.2=host-2:9002\nnode.3=host-3:9003\n");
            members.reloadNow();

            assertEquals(2, count.get(), "One further notify for the change");
            assertEquals(Set.of(NodeId.of("2"), NodeId.of("3")), notified.get().ids());
            assertSame(notified.get(), members.snapshot(), "Published snapshot is current");
        }
    }

    @Test
    @DisplayName("Content gate: rewriting with identical content does not swap or notify")
    void identicalContentIsSkipped(@TempDir final Path dir) throws Exception {
        final String content = "node.1=host-1:9001\nnode.2=host-2:9002\n";
        final Path file = write(dir, content);
        try (FileMembers members = new FileMembers(file)) {
            final MembersSnapshot before = members.snapshot();
            final AtomicInteger count = new AtomicInteger();
            members.addListener(snap -> count.incrementAndGet());
            assertEquals(1, count.get(), "Subscribe replays the current snapshot");

            Files.writeString(file, content);
            members.reloadNow();

            assertEquals(1, count.get(), "No further notify for identical content");
            assertSame(before, members.snapshot(), "Snapshot reference unchanged");
        }
    }

    @Test
    @DisplayName("Re-addressing a member is reflected in the new snapshot")
    void reAddressReflected(@TempDir final Path dir) throws Exception {
        final Path file = write(dir, "node.1=host-1:9001\nnode.2=host-2:9002\n");
        try (FileMembers members = new FileMembers(file)) {
            Files.writeString(file, "node.1=host-1:9001\nnode.2=moved-host:9500\n");
            members.reloadNow();

            final TcpMemberInfo m2 = members.snapshot().get(NodeId.of("2"));
            assertEquals("moved-host", m2.host());
            assertEquals(9500, m2.port());
        }
    }

    @Test
    @DisplayName("A malformed reload is ignored and the last good snapshot is kept")
    void malformedReloadIgnored(@TempDir final Path dir) throws Exception {
        final Path file = write(dir, "node.1=host-1:9001\n");
        try (FileMembers members = new FileMembers(file)) {
            final AtomicReference<MembersSnapshot> notified = new AtomicReference<>();
            members.addListener(notified::set);
            assertEquals(Set.of(NodeId.of("1")), notified.get().ids(), "Subscribe replays current");

            Files.writeString(file, "this is not valid\n");
            members.reloadNow();

            assertEquals(Set.of(NodeId.of("1")), notified.get().ids(),
                    "Malformed reload does not notify (still the initial)");
            assertEquals(Set.of(NodeId.of("1")), members.snapshot().ids());
        }
    }

    @Test
    @DisplayName("A listener that subscribes after a change replays the latest snapshot")
    void lateSubscriberGetsLatest(@TempDir final Path dir) throws Exception {
        final Path file = write(dir, "node.1=host-1:9001\nnode.2=host-2:9002\n");
        try (FileMembers members = new FileMembers(file)) {
            Files.writeString(file, "node.1=host-1:9001\nnode.3=host-3:9003\n");
            members.reloadNow(); // change happens BEFORE anyone subscribes

            final AtomicReference<MembersSnapshot> first = new AtomicReference<>();
            members.addListener(first::set);
            assertEquals(Set.of(NodeId.of("1"), NodeId.of("3")), first.get().ids(),
                    "Replay-on-subscribe delivers the latest snapshot, not the initial");
        }
    }

    @Test
    @DisplayName("A held snapshot reference is immutable across a reload swap")
    void heldSnapshotIsStable(@TempDir final Path dir) throws Exception {
        final Path file = write(dir, "node.1=host-1:9001\n");
        try (FileMembers members = new FileMembers(file)) {
            final MembersSnapshot held = members.snapshot();

            Files.writeString(file, "node.1=host-1:9001\nnode.2=host-2:9002\n");
            members.reloadNow();

            assertEquals(Set.of(NodeId.of("1")), held.ids(), "Old snapshot unaffected by swap");
            assertTrue(members.snapshot().contains(NodeId.of("2")), "Current snapshot updated");
        }
    }
}
