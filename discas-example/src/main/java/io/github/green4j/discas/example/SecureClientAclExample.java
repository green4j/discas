/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.auth.FileClientTokenStore;
import io.github.green4j.discas.common.client.auth.Pbkdf2;
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.acl.FileClientAcl;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A runnable demonstration of Client ACL: a single node whose client port requires a
 * <b>token</b> to authenticate, plus a file-based <b>authorization</b> policy that grants the
 * client GET/PUT/CAS under {@code app/} only. The example provisions the token- and ACL files
 * on disk (the formats operators use), connects a client with the token, performs an
 * authorized operation, then shows an out-of-policy operation being denied.
 *
 * <p>Run: {@code ./gradlew :discas-example:run -PmainClass=...SecureClientAclExample} or
 * execute {@link #main} directly from an IDE.
 */
public final class SecureClientAclExample {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClusterId CLUSTER = ClusterId.of("secure-client-acl");
    private static final ClientId WEB = ClientId.of("web-1");
    private static final String TOKEN = "s3cret-api-key";

    private SecureClientAclExample() {
    }

    public static void main(final String[] args) throws Exception {
        final Path baseDir = Files.createTempDirectory("secure-client-acl-");

        final Path tokenFile = baseDir.resolve("client-tokens.conf");
        Files.writeString(tokenFile, "client." + WEB.value() + "=" + tokenSpec(TOKEN) + "\n");

        final Path aclFile = baseDir.resolve("client-acl.conf");
        Files.writeString(aclFile, "acl." + WEB.value() + " = app/:GPC\n");

        final int peerPort;
        try (ServerSocket a = new ServerSocket(0)) {
            peerPort = a.getLocalPort();
        }
        final InetSocketAddress peerAddr = new InetSocketAddress("127.0.0.1", peerPort);

        final TcpTransportConfig peerConfig = TcpTransportConfig.defaults();
        final ClientTransportConfig clientConfig = ClientTransportConfig.defaults();

        final Path walBase = baseDir.resolve("node-1");
        Files.createDirectories(walBase);
        final FileWal wal = new FileWal(StorageConfig.builder()
                .baseDirectory(walBase)
                .walMaxFileBytes(16 * 1024 * 1024).snapshotRetentionCount(2).build());
        wal.initialize();

        final DisCasNode node = DisCasNodeFactory.create(
                new NodeConfig(NODE, CLUSTER, 1),
                new TcpPeerBootstrap(peerAddr, InMemoryMembers.ofTcp(Map.of(NODE, peerAddr)), peerConfig),
                wal);

        // Authentication: token store (hot-reloaded from disk); Authorization: file ACL.
        try (FileClientTokenStore tokens = new FileClientTokenStore(tokenFile);
                FileClientAcl acl = new FileClientAcl(aclFile)) {

            final TcpClientServerTransport clientServer =
                    DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                            new InetSocketAddress("127.0.0.1", 0), clientConfig,
                            new TokenClientAuthenticator(tokens)));
            final InetSocketAddress clientAddr =
                    new InetSocketAddress("127.0.0.1", clientServer.boundPort());
            node.registerClientAcl(acl);
            node.start();

            final DisCasClient client = DisCasClientFactory.create(
                    WEB, new TcpClientBootstrap(Map.of(NODE, clientAddr), clientConfig, TOKEN));

            System.out.println("=== Secure Client ACL example ===");
            System.out.println("token file: " + tokenFile);
            System.out.println("acl   file: " + aclFile);

            try {
                // Authorized: web-1 may PUT/GET under app/.
                client.put(ExampleBytes.encode("app/order:42"), ExampleBytes.encode("PENDING"))
                        .get(8, TimeUnit.SECONDS);
                System.out.println("put(app/order:42, PENDING)  -> OK (authorized)");
                System.out.println("get(app/order:42)           -> "
                        + ExampleBytes.decode(client.get(ExampleBytes.encode("app/order:42"))
                        .get(8, TimeUnit.SECONDS).value()));

                // Denied: web-1 has no grant outside app/.
                try {
                    client.put(ExampleBytes.encode("billing/secret"), ExampleBytes.encode("x"))
                            .get(8, TimeUnit.SECONDS);
                    System.out.println("put(billing/secret)         -> UNEXPECTEDLY ALLOWED");
                } catch (final ExecutionException e) {
                    System.out.println("put(billing/secret)         -> denied: " + e.getCause().getMessage());
                }
            } finally {
                client.close();
            }
        } finally {
            node.close();
        }
    }

    /** Build a {@code pbkdf2$...} token record for the file format (offline hashing). */
    private static String tokenSpec(final String token) {
        final byte[] salt = Pbkdf2.newSalt();
        final byte[] hash = Pbkdf2.hash(token, salt, Pbkdf2.DEFAULT_ITERATIONS);
        final Base64.Encoder b64 = Base64.getEncoder();
        return "pbkdf2$" + Pbkdf2.DEFAULT_ITERATIONS + "$"
                + b64.encodeToString(salt) + "$" + b64.encodeToString(hash) + "$" + Long.MAX_VALUE;
    }
}
