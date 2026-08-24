/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.lock.Lock;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockAcquireStatus;
import io.github.green4j.discas.client.lock.LockInfoResult;
import io.github.green4j.discas.client.lock.LockInfoStatus;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.client.transport.TcpClientTransport;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two clients whose wall clocks disagree by three quarters of an hour, holding and judging the same
 * lock lease.
 *
 * <h2>Why this is a mutual-exclusion test, not a formatting one</h2>
 * A lock here is a client-side convention over CAS: the node never sees a lease, so
 * {@code leaseUntilEpochMs} is written by one client and judged by another, and nothing but the
 * clocks makes those two agree. A holder whose clock lags writes a deadline that everyone else
 * reads as long past, so the next client to look considers the lock free and takes it -- two
 * holders, from clocks alone. A holder whose clock runs ahead writes a deadline far in the future,
 * so a lock abandoned by a dead process stays locked long past its TTL.
 * <p>
 * Both are asserted below, and both are what {@link ClusterClock} exists to remove: each client
 * corrects against the coordinator's clock, so what goes into the record is the cluster's time
 * rather than anyone's local idea of it.
 *
 * <h2>Where the skew comes from</h2>
 * The machine's clock cannot be moved by a test, so the skewed client is built through the
 * package-private seam that takes a {@link TimeSource} -- lying wall clock, honest monotonic one,
 * which is exactly the shape of the fault: a wrong clock still measures elapsed time correctly.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
// One node for the file: each test locks a job/* key of its own.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Lock leases -- clients with skewed clocks still agree")
class LockClockSkewIntegrationTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final Duration SKEW = Duration.ofMinutes(45);
    private static final Duration LEASE = Duration.ofSeconds(30);
    /** Slack for everything the test itself spends between two readings of a clock. */
    private static final long SLACK_MS = 5_000L;

    // Static so it exists before the class-level @BeforeAll below.
    @TempDir
    static Path baseDir;

    private DisCasNode node;
    private InetSocketAddress clientAddress;
    private final List<DisCasClient> clients = new ArrayList<>();

    /** The real clocks, with the wall one displaced. Monotonic time is never wrong, and is not. */
    private static final class SkewedClocks implements TimeSource {
        private final long skewMillis;

        private SkewedClocks(final long skewMillis) {
            this.skewMillis = skewMillis;
        }

        @Override
        public long wallMillis() {
            return System.currentTimeMillis() + skewMillis;
        }

        @Override
        public long monotonicNanos() {
            return System.nanoTime();
        }
    }

    @BeforeAll
    void setUp() throws Exception {
        final ListenSocket peerSocket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        final Path walBase = baseDir.resolve("node-1");
        Files.createDirectories(walBase);
        final FileWal wal = new FileWal(StorageConfig.builder().baseDirectory(walBase).build());
        wal.initialize();

        node = DisCasNodeFactory.create(
                new NodeConfig(NODE, ClusterId.of("lock-clock-cluster"), 1),
                new TcpPeerBootstrap(peerSocket,
                        InMemoryMembers.ofTcp(Map.of(NODE, peerSocket.address())),
                        TcpTransportConfig.defaults()),
                wal);
        final TcpClientServerTransport clientServer =
                DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                        new InetSocketAddress("127.0.0.1", 0), ClientTransportConfig.defaults()));
        clientAddress = new InetSocketAddress("127.0.0.1", clientServer.boundPort());
        node.start();
    }

    @AfterAll
    void tearDown() {
        for (final DisCasClient client : clients) {
            try {
                client.close();
            } catch (final Exception ignored) {
                // teardown
            }
        }
        clients.clear();
        if (node != null) {
            node.close();
        }
    }

    @Test
    @DisplayName("A holder whose clock lags does not hand its lock to the next client that looks")
    void laggingHolderKeepsItsLock() throws Exception {
        final DisCasClient lagging = client("lagging", -SKEW.toMillis());
        final DisCasClient honest = client("honest", 0L);

        final LockAcquireResult acquired =
                lagging.tryLock(TestBytes.utf8("job/lagging"), LEASE).get(10, TimeUnit.SECONDS);
        assertEquals(LockAcquireStatus.ACQUIRED, acquired.status());

        // Uncorrected, this holder would have written a deadline 45 minutes in the past, and this
        // read would report it expired -- after which nothing stops a second holder.
        final LockInfoResult seen =
                honest.getLockInfo(TestBytes.utf8("job/lagging")).get(10, TimeUnit.SECONDS);
        assertEquals(LockInfoStatus.LOCKED, seen.status(),
                "A lock held under a lagging clock must not read as expired to anyone else");

        final LockAcquireResult stolen =
                honest.tryLock(TestBytes.utf8("job/lagging"), LEASE).get(10, TimeUnit.SECONDS);
        assertEquals(LockAcquireStatus.HELD_BY_OTHER, stolen.status(),
                "Two holders at once is the failure this correction exists to prevent");
    }

    @Test
    @DisplayName("A holder whose clock runs ahead does not lock the key up for three quarters of an hour")
    void racingHolderDoesNotOverstayItsLease() throws Exception {
        final DisCasClient racing = client("racing", SKEW.toMillis());
        final DisCasClient honest = client("honest", 0L);

        assertEquals(LockAcquireStatus.ACQUIRED,
                racing.tryLock(TestBytes.utf8("job/racing"), LEASE).get(10, TimeUnit.SECONDS)
                        .status());

        final LockInfoResult seen =
                honest.getLockInfo(TestBytes.utf8("job/racing")).get(10, TimeUnit.SECONDS);
        assertEquals(LockInfoStatus.LOCKED, seen.status());

        final long overshootMs =
                seen.info().leaseUntilEpochMs() - (System.currentTimeMillis() + LEASE.toMillis());
        assertTrue(overshootMs <= SLACK_MS,
                "The stored deadline is " + overshootMs + "ms beyond a full lease from now:"
                        + " uncorrected, a crashed holder would hold this key for " + SKEW
                        + " past its TTL");
    }

    @Test
    @DisplayName("A holder's own view of its lease survives its clock being wrong")
    void remainingLeaseIsMeasuredMonotonically() throws Exception {
        final DisCasClient lagging = client("lagging-holder", -SKEW.toMillis());

        final LockAcquireResult acquired =
                lagging.tryLock(TestBytes.utf8("job/remaining"), LEASE).get(10, TimeUnit.SECONDS);
        assertEquals(LockAcquireStatus.ACQUIRED, acquired.status());

        // The holder's own clock is 45 minutes behind the cluster's, so the deadline it stored --
        // in cluster time -- is 45 minutes ahead of anything its own wall clock will read for the
        // next three quarters of an hour. Judged on the wall clock, this lease would look enormous;
        // judged on the monotonic one, it is the lease that was asked for.
        final Lock lock = acquired.lock();
        final Duration remaining = lock.remainingLease();
        assertTrue(remaining.compareTo(LEASE) <= 0,
                "A holder must never believe it has more lease than it asked for, got " + remaining);
        assertTrue(remaining.compareTo(LEASE.minusSeconds(10)) > 0,
                "Nor materially less, got " + remaining);
    }

    private DisCasClient client(final String name, final long skewMillis) throws Exception {
        final ClientId clientId = ClientId.of(name);
        final EventLoop loop = new EventLoop("clock-skew-" + name);
        final TcpClientBootstrap bootstrap = new TcpClientBootstrap(
                Map.of(NODE, clientAddress), ClientTransportConfig.defaults());
        final TcpClientTransport transport = new TcpClientTransport(
                loop, bootstrap.nodeAddresses, bootstrap.clientTransportConfig, clientId,
                null, bootstrap.securityProvider, ClientObserver.NONE);
        final DisCasClient client = new DisCasClient(clientId, transport, loop, true,
                ClientObserver.NONE, DisCasClientConfig.defaults(), new SkewedClocks(skewMillis));
        clients.add(client);
        // Also the handshake that measures the offset: nothing is corrected until a coordinator has
        // answered once, which is the same order an application sees.
        TestAwait.awaitReady(client);
        return client;
    }
}
