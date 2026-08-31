/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;
import io.github.green4j.discas.common.transport.tls.CertRotationManager;
import io.github.green4j.discas.common.transport.tls.FileTlsMaterialSource;
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.RenewalPolicy;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;
import io.github.green4j.discas.common.transport.tls.TlsPeerSecurityProvider;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.FileMembers;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The full production assembly: a 3-node cluster whose peer mesh runs over
 * <b>mTLS</b> with a shared cluster CA, membership comes from a reloadable
 * <b>{@link FileMembers} list</b>, and each node's cert is renewed from disk by a
 * <b>{@link CertRotationManager}</b> -- then a live cert rotation is performed
 * with zero mesh disruption.
 * <p>
 * Run: {@code java -cp <module jars>
 * io.github.green4j.discas.example.SecureClusterFileMembersExample}
 */
public final class SecureClusterFileMembersExample {

    private static final ClusterId CLUSTER = ClusterId.of("secure-example-cluster");
    private static final List<NodeId> NODE_IDS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));

    private SecureClusterFileMembersExample() {
    }

    public static void main(final String[] args) throws Exception {
        final Path baseDir = Files.createTempDirectory("discas-secure-example");
        final Path pki = Files.createDirectories(baseDir.resolve("pki"));
        System.out.println("=== Secure cluster: mTLS + FileMembers + cert rotation ===");
        System.out.println("workdir: " + baseDir);

        // Provision the cluster PKI (stand-in for an external cert agent)
        final ExampleCa ca = new ExampleCa(pki, CLUSTER);
        for (final NodeId nodeId : NODE_IDS) {
            ca.issueNode(nodeId);
        }
        System.out.println("issued cluster CA + per-node certs (SAN discas://" + CLUSTER + "/<node>)");

        // Bind the peer sockets, then write the member list from the ports they actually hold.
        // Each bound socket is handed to its node's bootstrap below, so no port is ever merely
        // earmarked: choosing a port with a throwaway probe and binding it a moment later leaves a
        // window in which anything -- including the kernel, handing an ephemeral port to some
        // outbound connection -- can take it first.
        final Map<NodeId, ListenSocket> peerSockets = new LinkedHashMap<>();
        final Map<NodeId, Integer> ports = new LinkedHashMap<>();
        for (final NodeId nodeId : NODE_IDS) {
            final ListenSocket peerSocket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            peerSockets.put(nodeId, peerSocket);
            ports.put(nodeId, peerSocket.port());
        }
        final Path membersFile = baseDir.resolve("members.conf");
        writeMembers(membersFile, ports);
        System.out.println("wrote member list: " + membersFile);

        final TcpTransportConfig peerConfig = TcpTransportConfig.defaults();

        final List<DisCasNode> nodes = new ArrayList<>();
        final List<CertRotationManager> rotators = new ArrayList<>();
        final Map<NodeId, FileTlsMaterialSource> sources = new LinkedHashMap<>();

        // Assemble each node: FileMembers + mTLS (reloadable) + rotation
        for (final NodeId nodeId : NODE_IDS) {
            final FileMembers members = new FileMembers(membersFile);

            final Path keyStore = pki.resolve("node-" + nodeId.value() + ".p12");
            final Path trustStore = ca.trustStore();
            final FileTlsMaterialSource source = new FileTlsMaterialSource(
                    keyStore, ca.password(), trustStore, ca.password());
            sources.put(nodeId, source);

            final TlsMaterial initial = source.snapshot();
            final ReloadableTlsContext ctx = ReloadableTlsContext.create(initial);

            final PeerSecurityProvider tls =
                    new TlsPeerSecurityProvider(TlsConfig.of(ctx.sslContext()));

            final FileWal wal = new FileWal(StorageConfig.builder()
                    .baseDirectory(Files.createDirectories(baseDir.resolve("wal-" + nodeId.value())))
                    .walMaxFileBytes(16 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build());
            wal.initialize();

            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, CLUSTER, NODE_IDS.size()),
                    new TcpPeerBootstrap(peerSockets.get(nodeId), members, peerConfig, tls),
                    wal);

            // Cert rotation applies on THIS node's event loop (created above): the
            // source detects/parses on the shared daemon and pushes; the manager
            // marshals each swap onto the loop, serialized with the SSLEngine.
            final CertRotationManager rotator = new CertRotationManager(
                    ctx, source, initial, RenewalPolicy.defaults(), node.loop()::execute);
            rotator.start();
            rotators.add(rotator);

            node.registerClientMessages(registrar ->
                    InProcessClientRegistry.register(nodeId, node.loop(), registrar,
                            node.clusterSize()));
            nodes.add(node);
        }
        for (final DisCasNode node : nodes) {
            node.start();
        }

        final ClientId secureClientId = ClientId.of("secure-client");
        final DisCasClient client = DisCasClientFactory.create(
                secureClientId, new InProcessClientBootstrap(NODE_IDS));

        try {
            awaitReady(client);
            System.out.println("cluster formed quorum over the mTLS mesh");

            final ByteBuffer key = utf8("account:42");
            client.put(key.duplicate(), utf8("balance=100")).get(8, TimeUnit.SECONDS);
            System.out.println("put  account:42 -> balance=100");
            System.out.println("get  account:42 -> "
                    + utf8ToString(client.get(key.duplicate()).get(5, TimeUnit.SECONDS).value()));

            // Live cert rotation: renew every node's cert on disk, then ask each node to read it.
            // Nothing reads the file until then, so a half-written store cannot be picked up.
            // Existing peer links keep serving.
            System.out.println("--- rotating every node's certificate ---");
            for (final NodeId nodeId : NODE_IDS) {
                ca.issueNode(nodeId);                 // external agent renews the file
                sources.get(nodeId).reloadNow();      // read it, and push -> node loop
                System.out.println("rotated " + nodeId + " cert");
            }

            // Fenced on the version the read observed, so the write cannot apply twice if the
            // rotation costs us a coordinator mid-flight.
            final Version atBalance100 = client
                    .get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                    .get(8, TimeUnit.SECONDS)
                    .version();
            final CasResult cas =
                    client.cas(key.duplicate(), atBalance100, utf8("balance=250"))
                            .get(8, TimeUnit.SECONDS);
            System.out.println("cas  account:42 100->250 swapped=" + cas.swapped()
                    + " (consensus served through rotation)");
            System.out.println("get  account:42 -> "
                    + utf8ToString(client.get(key.duplicate()).get(5, TimeUnit.SECONDS).value()));
        } finally {
            try {
                client.close();
            } catch (final Exception ignored) {
            }
            for (final CertRotationManager rotator : rotators) {
                rotator.close();
            }
            for (final FileTlsMaterialSource source : sources.values()) {
                try {
                    source.close();
                } catch (final Exception ignored) {
                }
            }
            for (final DisCasNode node : nodes) {
                try {
                    node.close();
                } catch (final Exception ignored) {
                }
            }
            for (final NodeId nodeId : NODE_IDS) {
                InProcessClientRegistry.unregister(nodeId);
            }
        }
        System.out.println("=== done ===");
    }

    private static void writeMembers(final Path file, final Map<NodeId, Integer> ports) throws Exception {
        // No cluster_id here -- each node gets it at startup; the file lists members only
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<NodeId, Integer> e : ports.entrySet()) {
            sb.append("node.").append(e.getKey().value()).append("=127.0.0.1:").append(e.getValue()).append('\n');
        }
        Files.writeString(file, sb.toString());
    }

    private static void awaitReady(final DisCasClient client) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                client.get(utf8("__ready__")).get(2, TimeUnit.SECONDS);
                return;
            } catch (final Exception e) {
                last = e;
                Thread.sleep(100L);
            }
        }
        throw new IllegalStateException("Cluster did not form within 60s", last);
    }

    private static ByteBuffer utf8(final String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String utf8ToString(final ByteBuffer buffer) {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
