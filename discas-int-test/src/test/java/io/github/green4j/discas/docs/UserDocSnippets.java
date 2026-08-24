/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.docs;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.ScanPage;
import io.github.green4j.discas.client.ScanResult;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.DisCasOperationException;
import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.LoggingClientObserver;
import io.github.green4j.discas.client.MetricsClientObserver;
import io.github.green4j.discas.client.ScanCoverage;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.client.WatchResult;
import io.github.green4j.discas.client.lock.Lock;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockInfoStatus;
import io.github.green4j.discas.client.lock.LockWriteResult;
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.io.Closeables;
import io.github.green4j.discas.common.io.LoggingReloadObserver;
import io.github.green4j.discas.common.io.MetricsReloadObserver;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.observability.ObservabilityConfig;
import io.github.green4j.discas.common.observability.ObservabilityServer;
import io.github.green4j.discas.common.operator.OperatorAttention;
import io.github.green4j.discas.common.operator.OperatorState;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.CertRotationManager;
import io.github.green4j.discas.common.transport.tls.RenewalPolicy;
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.TlsClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsContexts;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;
import io.github.green4j.discas.common.client.auth.ClientAuthenticator;
import io.github.green4j.discas.common.client.auth.FileClientTokenStore;
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.io.Reloadable;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.LoggingNodeObserver;
import io.github.green4j.discas.node.MetricsNodeObserver;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.PeerStateObserver;
import io.github.green4j.discas.node.acl.FileClientAcl;
import io.github.green4j.discas.node.membership.FileMembers;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.observability.NodeEndpoints;
import io.github.green4j.discas.node.transport.InProcessPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import io.github.green4j.discas.node.wal.Wal;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/**
 * Every code fragment in {@code docs/user/} , compiled.
 *
 * <p>Not a test -- there is no {@code @Test} here and nothing runs. It exists so that
 * {@code compileTestJava} fails when the user documentation drifts from the API it documents, which
 * is the one kind of rot a reader cannot detect and the writer will not notice.
 *
 * <p>Keep it in step by hand: change a snippet in the docs, change it here. The compiler proves the
 * API exists and the shapes line up; it cannot prove the prose around a snippet is true, so a
 * change here is a prompt to re-read the paragraph it belongs to.
 */
@SuppressWarnings("unused")
public final class UserDocSnippets {

    private UserDocSnippets() {
    }

    // ---- 01: helpers the other snippets use ---------------------------------------------------

    static ByteBuffer utf8(final String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    static String str(final ByteBuffer b) {
        return b == null ? null : StandardCharsets.UTF_8.decode(b).toString();
    }

    // ---- 01: getting started -------------------------------------------------------------------

    static DisCasClient gettingStarted() {
        final Map<NodeId, InetSocketAddress> nodes = Map.of(
                NodeId.of("1"), new InetSocketAddress("10.0.0.1", 8001),
                NodeId.of("2"), new InetSocketAddress("10.0.0.2", 8001),
                NodeId.of("3"), new InetSocketAddress("10.0.0.3", 8001));

        final DisCasClient client = DisCasClientFactory.create(
                ClientId.of("orders-service"),
                new TcpClientBootstrap(nodes, ClientTransportConfig.defaults()));

        client.put("service/leader", utf8("node-7")).join();
        final String leader = str(client.get("service/leader").join().value());

        client.get("service/leader")
                .thenApplyAsync(read -> str(read.value()), Runnable::run)
                .thenAccept(v -> { });

        client.close();
        return client;
    }

    // ---- 02: keys and values ---------------------------------------------------------------------

    static void reading(final DisCasClient client) {
        final GetResult current = client.get("config/timeout").join();
        current.exists();
        current.value();
        current.version();

        client.get("config/timeout", ReadConsistency.SERIALIZABLE).join();
    }

    static void writing(final DisCasClient client, final GetResult current) {
        final Version at = client.put("config/timeout", utf8("30s")).join();

        final CasResult r = client.cas("config/timeout", current.version(), "45s").join();
        r.swapped();
        r.version();
        r.value();

        final GetResult updated = client.update("config/timeout", now -> bump(now)).join();

        final CasResult created = client.putIfAbsent("service/owner", "me").join();
        if (!created.swapped()) {
            created.value();
        }
    }

    static GetResult increment(final DisCasClient client, final String key) {
        return client.update(key, current ->
                Long.toString(current == null ? 1L : Long.parseLong(current) + 1L)).join();
    }

    static void updateFailure(final DisCasClient client, final UnaryOperator<String> inc) {
        try {
            client.update("counter", inc).join();
        } catch (final CompletionException e) {
            // UpdateContendedException: the key is hot. Anything else: unknown outcome.
        }

        client.update("counter", inc, Duration.ofSeconds(5)).join();
    }

    static void deleting(final DisCasClient client, final GetResult current) {
        final Version at = client.delete("config/timeout").join();
        client.delete("config/timeout", current.version()).join();
    }

    private static String bump(final String current) {
        return current;
    }

    static void handlingFailure(final DisCasClient client, final ByteBuffer key,
                                final ByteBuffer value) {
        client.put(key, value).handle((ok, err) -> {
            if (err == null) {
                return "applied";
            }
            final Throwable cause = err instanceof CompletionException ? err.getCause() : err;
            if (cause instanceof DisCasOperationException) {
                final ClientErrorCode code = ((DisCasOperationException) cause).code();
            }
            return "failed";
        });
    }

    static void authorMarker(final DisCasClient client, final String key, final String myId,
                             final String payload) {
        final String marker = "writer-" + myId + ":" + payload;
        client.put(key, utf8(marker)).handle((ok, err) -> {
            if (err == null) {
                return true;
            }
            return marker.equals(str(client.get(key).join().value()));
        });
    }

    // ---- 03: scan and watch ---------------------------------------------------------------------

    static void scanning(final DisCasClient client) {
        final ScanPage keys = client.scan("service/").join();
        for (final ScanResult r : keys.results()) {
            final String key = str(r.key());
            r.version();
        }

        client.scan("service/", ScanCoverage.QUORUM).join();
        client.scan("service/", ScanCoverage.ANY_AVAILABLE).join();
    }

    static void paging(final DisCasClient client) {
        final ByteBuffer prefix = utf8("orders/");
        ByteBuffer cursor = null;
        do {
            final ScanPage page = client.scan(prefix, cursor, 500).join();
            for (final ScanResult r : page.results()) {
                r.key();
            }
            page.quorumReached();
            page.complete();
            cursor = page.nextCursor();
        } while (cursor != null);
    }

    static void watching(final DisCasClient client, final boolean running) {
        Version cursor = client.get("config/timeout").join().version();

        while (running) {
            final WatchResult w =
                    client.watch("config/timeout", cursor, Duration.ofSeconds(30)).join();
            if (w.changed()) {
                w.value();
                cursor = w.version();
            }
        }

        client.watch(utf8("config/timeout"), cursor, Duration.ofSeconds(30),
                ReadConsistency.SERIALIZABLE);
    }

    // ---- 04: locks -------------------------------------------------------------------------------

    static void takingALock(final DisCasClient client) {
        final LockAcquireResult r = client.tryLock("jobs/nightly", Duration.ofSeconds(30)).join();

        final LockAcquireResult w = client.lock("jobs/nightly",
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)).join();

        if (r.acquired()) {
            final Lock lock = r.lock();
            try {
                lock.fencingToken();
            } finally {
                lock.release().join();
            }
        }

        client.tryLock("jobs/nightly", Duration.ofSeconds(30), "worker-3");
    }

    static void holdingALock(final Lock lock) {
        final Duration left = lock.remainingLease();

        final LockWriteResult renewed = lock.renew(Duration.ofSeconds(30)).join();
        if (!renewed.applied()) {
            renewed.status();
            return;
        }

        lock.validate().join();
        lock.info().join();
        lock.lockInfo();
    }

    static void lockRecovery(final DisCasClient client, final String myOwnerId) {
        client.getLockInfo("jobs/nightly").thenAccept(info -> {
            if (info.status() == LockInfoStatus.LOCKED
                    && myOwnerId.equals(info.info().ownerId())) {
                // it is mine after all
            }
        });
    }

    // ---- 05: client setup --------------------------------------------------------------------------

    static void transports(final Map<NodeId, InetSocketAddress> nodes) {
        DisCasClientFactory.create(ClientId.of("orders-service"), new TcpClientBootstrap(nodes,
                ClientTransportConfig.defaults()));

        DisCasClientFactory.create(
                ClientId.of("embedded"),
                new InProcessClientBootstrap(
                        List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"))));
    }

    static void timeouts(final ClientId clientId, final TcpClientBootstrap bootstrap) {
        final DisCasClientConfig config = DisCasClientConfig.builder()
                .perAttemptTimeout(Duration.ofSeconds(2))
                .requestDeadline(Duration.ofSeconds(10))
                .scanTimeout(Duration.ofSeconds(5))
                .build();

        DisCasClientFactory.create(clientId, bootstrap, config);
    }

    static void authentication(final Map<NodeId, InetSocketAddress> nodes,
                               final Path keyStorePath, final char[] keyStorePassword,
                               final Path trustStorePath, final char[] trustStorePassword)
            throws Exception {
        new TcpClientBootstrap(nodes, ClientTransportConfig.defaults(), "the-token");

        final KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keyStorePath)) {
            keys.load(in, keyStorePassword);
        }
        final KeyStore trust = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(trustStorePath)) {
            trust.load(in, trustStorePassword);
        }

        final TlsConfig tls = TlsConfig.of(TlsContexts.build(keys, keyStorePassword, trust));

        new TcpClientBootstrap(nodes, ClientTransportConfig.defaults(), null,
                new TlsClientSecurityProvider(tls));

        TlsClientSecurityProvider.serverAuthOnly(tls);
    }

    static void clientObservability(final Map<NodeId, InetSocketAddress> nodes,
                                    final MetricRegistry metricRegistry, final Log log,
                                    final OperatorAttention operatorAttention,
                                    final String token,
                                    final ClientSecurityProvider security) {
        final ClientObserver observer =
                new MetricsClientObserver(metricRegistry,
                        new LoggingClientObserver(log, operatorAttention, ClientObserver.NONE));

        new TcpClientBootstrap(nodes, ClientTransportConfig.defaults(), token, security, observer);
    }

    static void colocatedClient(final DisCasNode node) {
        final DisCasClient client = DisCasClientFactory.createColocated(
                ClientId.of("embedded"), node.loop(), List.of(NodeId.of("1"), NodeId.of("2")));
    }

    // ---- 06: embedding a node ------------------------------------------------------------------------

    static void inProcessCluster(final Wal wal) throws Exception {
        final List<NodeId> nodeIds = List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));
        final ClusterId clusterId = ClusterId.of("embedded");

        final List<DisCasNode> nodes = new ArrayList<>();
        for (final NodeId id : nodeIds) {
            nodes.add(DisCasNodeFactory.create(
                    new NodeConfig(id, clusterId, nodeIds.size()),
                    new InProcessPeerBootstrap(InMemoryMembers.ofNodes(nodeIds)),
                    wal));
        }

        final DisCasClient client = DisCasClientFactory.create(
                ClientId.of("embedded-client"),
                new InProcessClientBootstrap(nodeIds));

        for (final DisCasNode node : nodes) {
            node.start();
        }

        client.get("__ready_probe__").get(30, TimeUnit.SECONDS);
    }

    static DisCasNode tcpNode(final Wal wal) {
        final ListenSocket peerSocket = ListenSocket.bind(new InetSocketAddress("0.0.0.0", 7001));

        final Map<NodeId, InetSocketAddress> peers = Map.of(
                NodeId.of("1"), new InetSocketAddress("10.0.0.1", 7001),
                NodeId.of("2"), new InetSocketAddress("10.0.0.2", 7001),
                NodeId.of("3"), new InetSocketAddress("10.0.0.3", 7001));

        final DisCasNode node = DisCasNodeFactory.create(
                new NodeConfig(NodeId.of("1"), ClusterId.of("prod"), peers.size()),
                new TcpPeerBootstrap(peerSocket, InMemoryMembers.ofTcp(peers),
                        TcpTransportConfig.defaults()),
                wal);

        final TcpClientServerTransport clientServer = DisCasNodeFactory.createClientServer(node,
                new TcpClientServerBootstrap(
                        new InetSocketAddress("0.0.0.0", 8001),
                        ClientTransportConfig.defaults()));

        node.start();
        node.healthSource().state();
        node.addLifecycleCloseable(clientServer);
        return node;
    }

    static FileWal durability() throws IOException {
        final FileWal wal = new FileWal(StorageConfig.builder()
                .baseDirectory(Path.of("/var/lib/discas/node-1"))
                .walMaxFileBytes(64 * 1024 * 1024)
                .snapshotRetentionCount(2)
                .build());
        wal.initialize();
        return wal;
    }

    // ---- 07: a custom starter -------------------------------------------------------------------------

    static PeerStateObserver observerChain(final NodeConfig config, final NodeId nodeId,
                                           final int clusterSize,
                                           final TcpPeerBootstrap peerBootstrap, final Wal wal) {
        final Log log = new Log(nodeId.value());
        final MetricRegistry metrics = new MetricRegistry();
        final OperatorAttention attention = new OperatorAttention(log);

        final PeerStateObserver peerState = new PeerStateObserver(clusterSize,
                new MetricsNodeObserver(metrics,
                        new LoggingNodeObserver(log, attention, NodeObserver.NONE)));

        final DisCasNode node = DisCasNodeFactory.create(config, peerBootstrap, wal, peerState);
        return peerState;
    }

    static void attentionTick(final DisCasNode node, final OperatorAttention attention) {
        node.loop().scheduleRepeat(OperatorAttention.CHECK_INTERVAL, attention::checkDue);
    }

    static void probes(final DisCasNode node, final NodeId nodeId,
                       final PeerStateObserver peerState, final MetricRegistry metrics)
            throws IOException {
        final ObservabilityServer observability = ObservabilityServer.start(
                ObservabilityConfig.builder().bindAddress("127.0.0.1").port(9600).build(),
                NodeEndpoints.router(nodeId, node.healthSource(), peerState, metrics));

        node.addLifecycleCloseable(observability);
    }

    static void clientAccess(final DisCasNode node, final Path tokenFile, final Path aclFile,
                             final ReloadObserver reloadObserver, final TlsConfig tlsConfig,
                             final InetSocketAddress clientBindAddress) {
        final FileClientTokenStore tokens = new FileClientTokenStore(tokenFile, reloadObserver);
        final ClientAuthenticator authenticator = new TokenClientAuthenticator(tokens);

        final ClientSecurityProvider clientSecurity = new TlsClientSecurityProvider(tlsConfig);

        node.registerClientAcl(new FileClientAcl(aclFile, reloadObserver));

        final TcpClientServerTransport clientServer = DisCasNodeFactory.createClientServer(node,
                new TcpClientServerBootstrap(clientBindAddress, ClientTransportConfig.defaults(),
                        authenticator, clientSecurity));
        node.addLifecycleCloseable(clientServer);
    }

    static ReloadObserver hotReload(final DisCasNode node, final MetricRegistry metrics,
                                    final Log log, final OperatorAttention attention,
                                    final Path membersFile) {
        final ReloadObserver reload = new MetricsReloadObserver(metrics,
                new LoggingReloadObserver(log, attention, ReloadObserver.NONE));

        final FileMembers members = new FileMembers(membersFile, reload);
        node.addLifecycleCloseable(members);
        return reload;
    }

    static void certRotation(final DisCasNode node, final ReloadableTlsContext reloadableTlsContext,
                             final Reloadable<TlsMaterial> tlsMaterialSource,
                             final TlsMaterial initialMaterial, final ReloadObserver reload) {
        final CertRotationManager rotator = new CertRotationManager(
                reloadableTlsContext, tlsMaterialSource, initialMaterial,
                RenewalPolicy.defaults(), node.loop()::execute, reload);
        rotator.start();
        node.addLifecycleCloseable(rotator);
    }

    static void shutdown(final DisCasNode node, final List<AutoCloseable> toClose,
                         final OperatorAttention attention) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                node.close();
            } catch (final Exception ignored) {
                // best-effort
            }
            Closeables.closeAll(toClose, failure -> attention.raise(
                    OperatorState.SHUTDOWN_INCOMPLETE, null,
                    "a resource threw while closing", failure));
        }, "discas-node-shutdown"));
    }
}
