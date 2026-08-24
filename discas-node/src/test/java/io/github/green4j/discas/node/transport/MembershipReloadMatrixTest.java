/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.PlaintextPeerSecurity;
import io.github.green4j.discas.node.Await;
import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.TestPorts;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.node.membership.FileMembers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Every kind of edit an operator can make to a live member file, and which of them a running node
 * takes.
 *
 * <p>One rule decides all of them: <b>{@code N} is frozen at startup</b>, because the proposer took
 * its quorum from it and a list that changed the count would resize that quorum under rounds already
 * in flight. So a reload is applied when it still describes exactly {@code N} members including this
 * node, and refused <em>whole</em> otherwise -- never in part, which would leave a node running a
 * membership nobody wrote.
 *
 * <p>{@code PeerMembershipReloadTest} covers what applying one <em>does</em>: connections dropped,
 * re-addressed peers redialled, {@code forceReconnect} honoured. This covers which ones are applied
 * at all, as a table rather than a handful of cases, because the interesting rows are the ones
 * nobody writes a case for -- a file that still has the right number of members after dropping the
 * node reading it.
 */
@DisplayName("Membership reload -- which edits a running node accepts")
class MembershipReloadMatrixTest {

    private static final ClusterId CLUSTER = ClusterId.of("reload-matrix");
    private static final NodeId SELF = NodeId.of("1");
    private static final NodeId PEER = NodeId.of("2");
    private static final NodeId THIRD = NodeId.of("3");

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
        // Edits that keep the shape: taken.
        "UNCHANGED,        APPLIED",
        "PEER_READDRESSED, APPLIED",
        "PEER_REPLACED,    APPLIED",
        // Edits that change how many members there are: refused by the guard, either direction.
        "GROWN,            REFUSED_BY_GUARD",
        "SHRUNK,           REFUSED_BY_GUARD",
        // The right count, reached by deleting the line of the node doing the reading. An operator
        // replacing a member writes this by editing one line too many, and a node that took it would
        // hold N members while no longer being one of them.
        "SELF_REMOVED,     REFUSED_BY_GUARD",
        // A file with no members at all never reaches the guard: it is not a membership, and the
        // loader refuses to parse it. Worth its own outcome because the operator sees a different
        // line and the membership is left alone by a different mechanism.
        "EMPTIED,          REFUSED_BY_FILE",
    })
    void everyEdit(final String edit, final String verdict, @TempDir final Path dir)
            throws Exception {
        final List<Integer> ports = TestPorts.allocate(3);
        final Path file = Files.writeString(dir.resolve("members.conf"),
                line(SELF, ports.get(0)) + line(PEER, ports.get(1)));

        final EventLoop loop = new EventLoop("reload-matrix");
        final AtomicReference<String> refusal = new AtomicReference<>();
        final AtomicReference<Throwable> unparseable = new AtomicReference<>();
        final FileMembers members = new FileMembers(file, new ReloadObserver() {
            @Override
            public void reloadFailed(final String source, final Throwable error) {
                unparseable.set(error);
            }
        });
        final TcpPeerTransport transport = new TcpPeerTransport(
                SELF, CLUSTER, 2, loop, new InetSocketAddress("127.0.0.1", ports.get(0)),
                members, PlaintextPeerSecurity.PROVIDER, TcpTransportConfig.defaults(),
                observer(refusal));
        loop.start();
        try {
            rewrite(file, edited(edit, ports));
            members.reloadNow();

            if ("APPLIED".equals(verdict)) {
                final List<NodeId> expected =
                        "PEER_REPLACED".equals(edit) ? List.of(THIRD) : List.of(PEER);
                Await.until("the reload to be applied", () -> {
                    if (!transport.peers().equals(expected)) {
                        throw new IllegalStateException("peers=" + transport.peers());
                    }
                });
                assertNull(refusal.get(), "An applied reload must not also be reported refused");
            } else {
                final AtomicReference<?> reporter =
                        "REFUSED_BY_FILE".equals(verdict) ? unparseable : refusal;
                Await.until("the reload to be refused", () -> {
                    if (reporter.get() == null) {
                        throw new IllegalStateException("No refusal yet");
                    }
                });
                assertNotNull(reporter.get());
                assertEquals(List.of(PEER), transport.peers(),
                        "A refused reload must leave the membership exactly as it was: taking it in"
                                + " part would leave a list nobody wrote");
            }
        } finally {
            transport.close();
            loop.shutdown();
        }
    }

    /** The file contents each edit produces. */
    private static String edited(final String edit, final List<Integer> ports) {
        switch (edit) {
            case "UNCHANGED":
                return line(SELF, ports.get(0)) + line(PEER, ports.get(1));
            case "PEER_READDRESSED":
                return line(SELF, ports.get(0)) + line(PEER, ports.get(2));
            case "PEER_REPLACED":
                return line(SELF, ports.get(0)) + line(THIRD, ports.get(2));
            case "GROWN":
                return line(SELF, ports.get(0)) + line(PEER, ports.get(1)) + line(THIRD, ports.get(2));
            case "SHRUNK":
                return line(SELF, ports.get(0));
            case "EMPTIED":
                return "";
            case "SELF_REMOVED":
                return line(PEER, ports.get(1)) + line(THIRD, ports.get(2));
            default:
                throw new IllegalArgumentException(edit);
        }
    }

    /**
     * Write the file and make sure it looks changed.
     * <p>
     * The loader skips a file whose signature -- size and last-modified -- is what it was, so an
     * edit that keeps the size and lands inside the same clock tick is invisible to it. Setting the
     * timestamp forward says so deterministically, where sleeping for the filesystem's granularity
     * would only make it likely.
     */
    private static void rewrite(final Path file, final String contents) throws Exception {
        final FileTime before = Files.getLastModifiedTime(file);
        Files.writeString(file, contents);
        Files.setLastModifiedTime(file,
                FileTime.fromMillis(before.toMillis() + Duration.ofSeconds(1).toMillis()));
    }

    private static String line(final NodeId id, final int port) {
        return "node." + id.value() + "=127.0.0.1:" + port + "\n";
    }

    private static NodeObserver observer(final AtomicReference<String> refusal) {
        return new NodeObserver() {
            @Override
            public void membersReloadRejected(final String reason) {
                refusal.set(reason);
            }
        };
    }
}
