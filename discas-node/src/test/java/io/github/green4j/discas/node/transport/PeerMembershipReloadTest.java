/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.Await;
import io.github.green4j.discas.node.PeerMessage;
import io.github.green4j.discas.node.TestPorts;
import io.github.green4j.discas.node.membership.FileMembers;
import io.github.green4j.discas.common.transport.security.PlaintextPeerSecurity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Peer transport reacts to FileMembers reload")
class PeerMembershipReloadTest {

    private static final ClusterId CLUSTER = ClusterId.of("member-cluster");

    @Test
    @DisplayName("A constant-N replace drops the removed peer's connection")
    void constantNReplaceDropsRemovedPeer(@TempDir final Path dir) throws Exception {
        final NodeId n1 = NodeId.of("1");
        final NodeId n2 = NodeId.of("2");
        final NodeId n3 = NodeId.of("3");
        final List<Integer> ports = TestPorts.allocate(3);
        final int p1 = ports.get(0);
        final int p2 = ports.get(1);
        final int p3 = ports.get(2);
        final String both = "node.1=127.0.0.1:" + p1 + "\nnode.2=127.0.0.1:" + p2 + "\n";
        final Path file1 = Files.writeString(dir.resolve("node1.conf"), both);
        final Path file2 = Files.writeString(dir.resolve("node2.conf"), both);

        final TcpTransportConfig config = TcpTransportConfig.defaults();
        final EventLoop loop1 = new EventLoop("mem-1");
        final EventLoop loop2 = new EventLoop("mem-2");
        final FileMembers store1 = new FileMembers(file1);
        final FileMembers store2 = new FileMembers(file2);
        final TcpPeerTransport t1 = new TcpPeerTransport(n1, CLUSTER, 2, loop1,
                new InetSocketAddress("127.0.0.1", p1), store1,
                PlaintextPeerSecurity.PROVIDER, config);
        final TcpPeerTransport t2 = new TcpPeerTransport(n2, CLUSTER, 2, loop2,
                new InetSocketAddress("127.0.0.1", p2), store2,
                PlaintextPeerSecurity.PROVIDER, config);

        // A transport that cannot state a promise ceiling neither dials nor accepts -- the rollback
        // guard doing its job. A node binds this from its store once replay
        // has established it; a bare transport has to be told, exactly as one.
        t1.bindPromiseCeiling(() -> 0L);
        t2.bindPromiseCeiling(() -> 0L);
        final CountDownLatch received = new CountDownLatch(1);
        t1.register(msg -> { });
        t2.register(msg -> received.countDown());
        loop1.start();
        loop2.start();
        try {
            // Establish the link: node 1 (lower id) dials node 2.
            loop1.execute(() -> t1.send(n2, prepare(n1)));
            assertTrue(received.await(10, TimeUnit.SECONDS), "Node 2 receives over the link");
            assertTrue(t1.peers().contains(n2), "Node 2 is a peer before the replace");

            // Constant-N replace: swap node 2 for node 3 in node 1's file (N stays 2).
            Files.writeString(file1, "node.1=127.0.0.1:" + p1 + "\nnode.3=127.0.0.1:" + p3 + "\n");
            store1.reloadNow();

            // node 1 applies the replace on the loop, and one reconciliation is two passes inside
            // it: removals first, then additions. This assertion runs off the loop, so it has to
            // wait for the state the reload ends in -- waiting for the removal alone and then
            // asserting the addition lands between the two passes about once in a hundred loaded
            // runs, which is how it was found: green a hundred times alone, red under a full verify.
            Await.until("node 1 to apply the replace", Duration.ofSeconds(5), () -> {
                final List<NodeId> current = t1.peers();
                if (current.contains(n2) || !current.contains(n3)) {
                    throw new IllegalStateException("Reconciliation not applied yet: " + current);
                }
            });
            assertFalse(t1.peers().contains(n2), "Node 2 dropped from node 1 after the replace");
            assertTrue(t1.peers().contains(n3), "Node 3 added by the replace");
        } finally {
            store1.close();
            store2.close();
            t1.close();
            t2.close();
            loop1.shutdown();
            loop2.shutdown();
            loop1.awaitTermination(Duration.ofSeconds(2));
            loop2.awaitTermination(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("Re-addressing a peer: forceReconnect=false keeps the live link, true breaks it")
    void reAddressRespectsForceReconnect(@TempDir final Path dir) throws Exception {
        assertTrue(linkSurvivesReAddress(dir, "keep", false),
                "forceReconnect=false keeps the healthy connection to the old address");
        assertFalse(linkSurvivesReAddress(dir, "force", true),
                "forceReconnect=true drops the connection and redials the (dead) new address");
    }

    /**
     * Establish n1->n2, then re-address n2 in n1's file to a dead port and reload.
     * Returns whether a subsequent send from n1 still reaches n2.
     */
    private static boolean linkSurvivesReAddress(final Path dir, final String tag,
                                                 final boolean forceReconnect) throws Exception {
        final NodeId n1 = NodeId.of("1");
        final NodeId n2 = NodeId.of("2");
        final List<Integer> ports = TestPorts.allocate(3);
        final int p1 = ports.get(0);
        final int p2 = ports.get(1);
        final int deadPort = ports.get(2); // not listened on
        final Path file1 = Files.writeString(dir.resolve("n1-" + tag + ".conf"),
                "node.1=127.0.0.1:" + p1 + "\nnode.2=127.0.0.1:" + p2 + "\n");
        final Path file2 = Files.writeString(dir.resolve("n2-" + tag + ".conf"),
                "node.1=127.0.0.1:" + p1 + "\nnode.2=127.0.0.1:" + p2 + "\n");

        final TcpTransportConfig cfg1 = TcpTransportConfig.builder().forceReconnect(forceReconnect).build();
        final TcpTransportConfig cfg2 = TcpTransportConfig.defaults();
        final EventLoop loop1 = new EventLoop("readdr-1-" + tag);
        final EventLoop loop2 = new EventLoop("readdr-2-" + tag);
        final FileMembers store1 = new FileMembers(file1);
        final FileMembers store2 = new FileMembers(file2);
        final TcpPeerTransport t1 = new TcpPeerTransport(n1, CLUSTER, 2, loop1,
                new InetSocketAddress("127.0.0.1", p1), store1,
                PlaintextPeerSecurity.PROVIDER, cfg1);
        final TcpPeerTransport t2 = new TcpPeerTransport(n2, CLUSTER, 2, loop2,
                new InetSocketAddress("127.0.0.1", p2), store2,
                PlaintextPeerSecurity.PROVIDER, cfg2);

        t1.bindPromiseCeiling(() -> 0L);
        t2.bindPromiseCeiling(() -> 0L);

        final CountDownLatch first = new CountDownLatch(1);
        final AtomicReference<CountDownLatch> probe = new AtomicReference<>(first);
        t1.register(msg -> { });
        t2.register(msg -> probe.get().countDown());
        loop1.start();
        loop2.start();
        try {
            loop1.execute(() -> t1.send(n2, prepare(n1)));
            assertTrue(first.await(10, TimeUnit.SECONDS), "Initial link established");

            // Re-address n2 to a dead port in n1's file and reload.
            Files.writeString(file1, "node.1=127.0.0.1:" + p1 + "\nnode.2=127.0.0.1:" + deadPort + "\n");
            store1.reloadNow();
            // Let the change reconcile on loop1 before the next send.
            final CountDownLatch reconciled = new CountDownLatch(1);
            loop1.execute(reconciled::countDown);
            reconciled.await(2, TimeUnit.SECONDS);

            // Probe with a fresh send and observe whether it still reaches n2.
            final CountDownLatch afterReAddress = new CountDownLatch(1);
            probe.set(afterReAddress);
            loop1.execute(() -> t1.send(n2, prepare(n1)));
            return afterReAddress.await(3, TimeUnit.SECONDS);
        } finally {
            store1.close();
            store2.close();
            t1.close();
            t2.close();
            loop1.shutdown();
            loop2.shutdown();
            loop1.awaitTermination(Duration.ofSeconds(2));
            loop2.awaitTermination(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("Nodes started with different N do not interconnect (CLUSTER_SIZE_MISMATCH guard)")
    void differentNNodesDoNotInterconnect(@TempDir final Path dir) throws Exception {
        final NodeId n1 = NodeId.of("1");
        final NodeId n2 = NodeId.of("2");
        final List<Integer> ports = TestPorts.allocate(3);
        final int p1 = ports.get(0);
        final int p2 = ports.get(1);
        final int p3 = ports.get(2);
        // node 1 sees N=2; node 2 sees N=3 -- a mixed-N cluster.
        final Path file1 = Files.writeString(dir.resolve("n1.conf"),
                "node.1=127.0.0.1:" + p1 + "\nnode.2=127.0.0.1:" + p2 + "\n");
        final Path file2 = Files.writeString(dir.resolve("n2.conf"),
                "node.1=127.0.0.1:" + p1 + "\nnode.2=127.0.0.1:" + p2 + "\nnode.3=127.0.0.1:" + p3 + "\n");

        final TcpTransportConfig config = TcpTransportConfig.defaults();
        final EventLoop loop1 = new EventLoop("mixed-1");
        final EventLoop loop2 = new EventLoop("mixed-2");
        final FileMembers m1 = new FileMembers(file1);
        final FileMembers m2 = new FileMembers(file2);
        final TcpPeerTransport t1 = new TcpPeerTransport(n1, CLUSTER, 2, loop1,
                new InetSocketAddress("127.0.0.1", p1), m1, PlaintextPeerSecurity.PROVIDER, config);
        final TcpPeerTransport t2 = new TcpPeerTransport(n2, CLUSTER, 3, loop2,
                new InetSocketAddress("127.0.0.1", p2), m2, PlaintextPeerSecurity.PROVIDER, config);

        // A transport that cannot state a promise ceiling neither dials nor accepts -- the rollback
        // guard doing its job. A node binds this from its store once replay
        // has established it; a bare transport has to be told, exactly as one.
        t1.bindPromiseCeiling(() -> 0L);
        t2.bindPromiseCeiling(() -> 0L);
        final CountDownLatch received = new CountDownLatch(1);
        t1.register(msg -> { });
        t2.register(msg -> received.countDown());
        loop1.start();
        loop2.start();
        try {
            // node 1 (lower id) repeatedly dials node 2; the guard must block the link.
            for (int i = 0; i < 5; i++) {
                loop1.execute(() -> t1.send(n2, prepare(n1)));
                Thread.sleep(50L);
            }
            assertFalse(received.await(1, TimeUnit.SECONDS),
                    "Mixed-N nodes must not form a link (CLUSTER_SIZE_MISMATCH)");
        } finally {
            m1.close();
            m2.close();
            t1.close();
            t2.close();
            loop1.shutdown();
            loop2.shutdown();
            loop1.awaitTermination(Duration.ofSeconds(2));
            loop2.awaitTermination(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("A member change made before loop.start() is applied on the loop (replay)")
    void preStartChangeApplied(@TempDir final Path dir) throws Exception {
        final NodeId n1 = NodeId.of("1");
        final NodeId n2 = NodeId.of("2");
        final NodeId n3 = NodeId.of("3");
        final List<Integer> ports = TestPorts.allocate(3);
        final int p1 = ports.get(0);
        final int p2 = ports.get(1);
        final int p3 = ports.get(2);
        final Path file = Files.writeString(dir.resolve("m.conf"),
                "node.1=127.0.0.1:" + p1 + "\nnode.2=127.0.0.1:" + p2 + "\n"); // N=2

        final TcpTransportConfig config = TcpTransportConfig.defaults();
        final EventLoop loop = new EventLoop("prestart");
        final FileMembers members = new FileMembers(file);
        final TcpPeerTransport t = new TcpPeerTransport(n1, CLUSTER, 2, loop,
                new InetSocketAddress("127.0.0.1", p1), members,
                PlaintextPeerSecurity.PROVIDER, config);
        t.register(msg -> { });
        try {
            // Constant-N replace 2->3, applied BEFORE the loop starts.
            Thread.sleep(10L);
            Files.writeString(file, "node.1=127.0.0.1:" + p1 + "\nnode.3=127.0.0.1:" + p3 + "\n");
            members.reloadNow();

            loop.start();
            drain(loop);
            assertTrue(t.peers().contains(n3), "Pre-start change applied on the loop");
            assertFalse(t.peers().contains(n2), "Old peer dropped");
        } finally {
            members.close();
            t.close();
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(2));
        }
    }

    /** Block until the loop has drained everything queued so far. */
    private static void drain(final EventLoop loop) throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        loop.execute(done::countDown);
        done.await(2, TimeUnit.SECONDS);
    }

    private static PeerMessage prepare(final NodeId from) {
        return new PeerMessage.PrepareReq(from, 1L, new HashedBytes(ByteBuffer.wrap(new byte[]{7})),
                new Ballot(1, from));
    }

}
