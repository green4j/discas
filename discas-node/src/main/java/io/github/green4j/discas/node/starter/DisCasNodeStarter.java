/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.starter;

import io.github.green4j.discas.common.io.LoggingReloadObserver;
import io.github.green4j.discas.common.io.MetricsReloadObserver;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.io.Closeables;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.client.auth.AllowAllClientAuthenticator;
import io.github.green4j.discas.common.client.auth.ClientAuthenticator;
import io.github.green4j.discas.common.client.auth.ClientTokenStore;
import io.github.green4j.discas.common.client.auth.DirectoryClientTokenStore;
import io.github.green4j.discas.common.client.auth.FileClientTokenStore;
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;
import io.github.green4j.discas.common.transport.tls.TlsClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.CertRotationManager;
import io.github.green4j.discas.common.transport.tls.FileTlsMaterialSource;
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.RenewalPolicy;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;
import io.github.green4j.discas.common.transport.tls.TlsPeerSecurityProvider;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.LoggingNodeObserver;
import io.github.green4j.discas.node.MetricsNodeObserver;
import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.PeerStateObserver;
import io.github.green4j.discas.common.observability.ObservabilityServer;
import io.github.green4j.discas.node.observability.NodeEndpoints;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.operator.OperatorAttention;
import io.github.green4j.discas.common.operator.OperatorState;
import io.github.green4j.discas.node.acl.FileClientAcl;
import io.github.green4j.discas.node.membership.FileMembers;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.membership.Members;
import io.github.green4j.discas.node.membership.TcpMemberInfo;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Optional command-line entry point that starts exactly one {@link DisCasNode}: TCP peer
 * transport + {@link FileWal} + a TCP client-server endpoint, with optional mTLS. All
 * configuration is resolved by {@link DisCasNodeConfig} from CLI flags and {@code DISCAS_*}
 * environment variables (CLI wins), and the effective values (with their source) are printed
 * at startup with secrets masked.
 *
 * <p>Since every value can come from the environment, an empty command line is <em>not</em>
 * treated as a help request; only an explicit {@code -h}/{@code --help} prints help. When
 * required values are absent from both sources, the error and usage are printed and the
 * process exits non-zero.
 */
public final class DisCasNodeStarter {

    private DisCasNodeStarter() {
    }

    public static void main(final String[] args) throws Exception {
        if (DisCasNodeConfig.isHelpRequested(args)) {
            System.out.print(DisCasNodeConfig.helpText());
            return;
        }

        final DisCasNodeConfig cfg;
        try {
            cfg = DisCasNodeConfig.resolve(args, System.getenv());
        } catch (final GetOpts.ParseException | IllegalArgumentException e) {
            ConfigSupport.usageError(
                    DisCasNodeConfig.PROGRAM, e.getMessage(), DisCasNodeConfig.usageText());
            return;
        }

        cfg.print(System.out);
        run(cfg);
    }

    private static void run(final DisCasNodeConfig cfg) throws Exception {
        final List<AutoCloseable> toClose = new ArrayList<>();

        // Observability, composed by decoration so each concern stays independent and any of them
        // can be dropped from a chain. Built first because the very first thing the node does --
        // load its member list -- already reports through the reload seam.
        final Log log = new Log(cfg.nodeId.value());
        final MetricRegistry metrics = new MetricRegistry();
        // One register for the whole process, shared by the reload seam and the node's own chain:
        // "the members file does not parse" and "a peer's storage rolled back" are one operator's
        // problem and belong on one surface.
        final OperatorAttention attention = new OperatorAttention(log);
        attention.registerMetrics(metrics);
        final ReloadObserver reload = new MetricsReloadObserver(metrics,
                new LoggingReloadObserver(log, attention, ReloadObserver.NONE));

        // Membership: hot-reloaded file or a fixed static list
        final Members<TcpMemberInfo> members;
        if (cfg.membersFromFile()) {
            final FileMembers fileMembers = new FileMembers(cfg.membersFile, reload);
            members = fileMembers;
            toClose.add(fileMembers);
        } else {
            members = InMemoryMembers.ofTcp(cfg.members);
        }

        final TcpTransportConfig tcpConfig = cfg.peerTransportConfig;

        // Peer security: plaintext or mTLS (optionally with certificate hot-reload).
        final TcpPeerBootstrap peerBootstrap;
        ReloadableTlsContext tlsContext = null;
        FileTlsMaterialSource tlsSource = null;
        TlsMaterial tlsInitial = null;
        if (cfg.tls) {
            tlsSource = new FileTlsMaterialSource(
                    cfg.tlsKeystore, cfg.tlsKeystorePassword,
                    cfg.tlsTruststore, cfg.tlsTruststorePassword, reload);
            tlsInitial = tlsSource.snapshot();
            tlsContext = ReloadableTlsContext.create(tlsInitial);
            final PeerSecurityProvider security =
                    new TlsPeerSecurityProvider(TlsConfig.of(tlsContext.sslContext()));
            peerBootstrap = new TcpPeerBootstrap(cfg.peerBind, members, tcpConfig, security);
            toClose.add(tlsSource);
        } else {
            peerBootstrap = new TcpPeerBootstrap(cfg.peerBind, members, tcpConfig);
        }

        final FileWal wal = new FileWal(cfg.storageConfig);
        wal.initialize();

        // The node's own chain, on the same registry and log as the reload seam above: the logging
        // decorator writes operator-facing events, the metrics decorator counts everything, and the
        // peer-state decorator accumulates the handshake events readiness is derived from. An
        // embedding application passes its own observer to DisCasNodeFactory instead and gets the
        // silent default -- a standalone daemon opts in, because a node that exits on a failed
        // recovery without saying so anywhere is not operable.
        final PeerStateObserver peerState = new PeerStateObserver(
                cfg.clusterSize,
                new MetricsNodeObserver(metrics,
                        new LoggingNodeObserver(log, attention, NodeObserver.NONE)));
        peerState.registerMetrics(metrics);

        final DisCasNode node = DisCasNodeFactory.create(
                cfg.nodeConfig,
                peerBootstrap,
                wal,
                peerState);

        // A state with a window says nothing until it has been true for long enough, and nothing
        // fires while a condition merely persists -- so the register needs a tick of its own. On the
        // node's loop rather than a thread of its own: every raise already happens there.
        node.loop().scheduleRepeat(OperatorAttention.CHECK_INTERVAL, attention::checkDue);

        // Endpoints last among the node's own wiring, so they only ever observe a fully built node.
        // Registered as a lifecycle closeable so the listening socket goes away with the node.
        final ObservabilityServer observability = ObservabilityServer.start(
                cfg.observabilityConfig,
                NodeEndpoints.router(cfg.nodeId, node.healthSource(), peerState, metrics));
        if (observability != null) {
            node.addLifecycleCloseable(observability);
            System.out.println(DisCasNodeConfig.PROGRAM + ": observability on "
                    + cfg.observabilityConfig.bindAddress() + ":" + observability.port()
                    + " (/health, /ready, /metrics)");
        }

        // Client access: authentication (who), TLS (channel), authorization (what). All three
        // are independent of the peer-mesh security configured above.
        final ClientAuthenticator clientAuthenticator = clientAuthenticator(cfg, toClose, reload);
        final ClientSecurityProvider clientSecurity = clientSecurity(cfg, node, toClose, reload);

        // Client-facing TCP endpoint, wired to the node's event loop and lifecycle.
        DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                cfg.clientBind, cfg.clientTransportConfig, clientAuthenticator, clientSecurity));

        // Authorization: a hot-reloaded prefix->ops ACL. Unbound means permissive, so warn.
        if (cfg.clientAclFile != null) {
            final FileClientAcl acl = new FileClientAcl(cfg.clientAclFile, reload);
            node.registerClientAcl(acl);
            toClose.add(acl);
        } else {
            System.out.println(DisCasNodeConfig.PROGRAM
                    + ": WARNING client authorization is disabled -- every authenticated client "
                    + "may read and write every key. Set --client-acl-file ("
                    + ConfigSupport.envName("client-acl-file") + ") to restrict access.");
        }
        if (cfg.clientAuth == ClientAuthMode.ALLOWALL) {
            System.out.println(DisCasNodeConfig.PROGRAM
                    + ": WARNING client authentication is 'allowall' -- any client may claim any "
                    + "id. Use --client-auth token|mtls outside a trusted network.");
        }

        // Certificate rotation runs on the node's loop once the node exists.
        if (cfg.tls && cfg.tlsCertRotation) {
            final CertRotationManager rotator = new CertRotationManager(
                    tlsContext, tlsSource, tlsInitial, RenewalPolicy.defaults(),
                    node.loop()::execute, reload);
            rotator.start();
            toClose.add(rotator);
        }

        node.start();
        System.out.println(DisCasNodeConfig.PROGRAM + ": node " + cfg.nodeId
                + " started (peer=" + cfg.peerBind + ", client=" + cfg.clientBind + ")");

        awaitShutdown(node, toClose, attention);
    }

    private static void awaitShutdown(final DisCasNode node, final List<AutoCloseable> toClose,
                                      final OperatorAttention attention)
            throws InterruptedException {
        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                node.close();
            } catch (final Exception ignored) {
                // best-effort shutdown
            }
            // The node's own resources report through NodeObserver; these are the starter's, and
            // reach the same state: it is the same fault, and an operator does not care which list
            // held the handle.
            Closeables.closeAll(toClose, failure -> attention.raise(
                    OperatorState.SHUTDOWN_INCOMPLETE, null, "a resource threw while closing",
                    failure));
            stopped.countDown();
        }, "discas-node-shutdown"));
        stopped.await();
    }

    /**
     * The client authenticator for the configured {@link ClientAuthMode}. Under mTLS the
     * certificate CN is authoritative and the transport cross-checks it against the claimed hello
     * id, so the authenticator itself stays permissive -- admission is the TLS handshake.
     */
    private static ClientAuthenticator clientAuthenticator(final DisCasNodeConfig cfg,
                                                           final List<AutoCloseable> toClose,
                                                           final ReloadObserver reload) {
        if (cfg.clientAuth != ClientAuthMode.TOKEN) {
            return AllowAllClientAuthenticator.INSTANCE;
        }
        // ClientTokenStore itself is not AutoCloseable; the watched implementations are, so hold
        // the concrete type long enough to register it for shutdown (as with FileMembers above).
        final ClientTokenStore store;
        if (cfg.clientTokenFile != null) {
            final FileClientTokenStore fileStore = new FileClientTokenStore(cfg.clientTokenFile, reload);
            toClose.add(fileStore);
            store = fileStore;
        } else {
            final DirectoryClientTokenStore dirStore =
                    new DirectoryClientTokenStore(cfg.clientTokenDir, reload);
            toClose.add(dirStore);
            store = dirStore;
        }
        return new TokenClientAuthenticator(store);
    }

    /**
     * TLS for the client port, where this node is the <em>server</em>: it always presents its own
     * certificate, and additionally requires one from the client under
     * {@link ClientAuthMode#MTLS}. Returns the plaintext provider when client TLS is off.
     * <p>
     * Independent of the peer-mesh TLS: the key/trust stores, the rotation flag and the CA may all
     * differ, since peers and clients are different trust domains.
     */
    private static ClientSecurityProvider clientSecurity(final DisCasNodeConfig cfg,
                                                         final DisCasNode node,
                                                         final List<AutoCloseable> toClose,
                                                         final ReloadObserver reload)
            throws Exception {
        if (!cfg.clientTls) {
            return PlaintextClientSecurity.PROVIDER;
        }
        final FileTlsMaterialSource source = new FileTlsMaterialSource(
                cfg.clientTlsKeystore, cfg.clientTlsKeystorePassword,
                cfg.clientTlsTruststore, cfg.clientTlsTruststorePassword, reload);
        final TlsMaterial initial = source.snapshot();
        final ReloadableTlsContext context = ReloadableTlsContext.create(initial);
        if (cfg.clientTlsCertRotation) {
            // Rotation applies on the node's loop, serialized with the SSLEngine handshakes that
            // also run there -- the same discipline as the peer-mesh rotator below.
            final CertRotationManager rotator = new CertRotationManager(
                    context, source, initial, RenewalPolicy.defaults(),
                    node.loop()::execute, reload);
            rotator.start();
            toClose.add(rotator);
            toClose.add(source);
        } else {
            source.close();
        }
        final TlsConfig tlsConfig = TlsConfig.of(context.sslContext());
        return cfg.clientAuth == ClientAuthMode.MTLS
                ? new TlsClientSecurityProvider(tlsConfig)              // requires a client cert
                : TlsClientSecurityProvider.serverAuthOnly(tlsConfig);  // encrypt only
    }

}
