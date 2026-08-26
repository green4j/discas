/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.LoggingClientObserver;
import io.github.green4j.discas.client.MetricsClientObserver;

import io.github.green4j.discas.agent.starter.DisCasAgentConfig;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.io.Closeables;
import io.github.green4j.discas.common.io.LoggingReloadObserver;
import io.github.green4j.discas.common.io.MetricsReloadObserver;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.common.io.ReloadableFileSource;
import io.github.green4j.discas.common.io.ReloadableFiles;
import io.github.green4j.discas.common.io.ReloadReport;
import io.github.green4j.discas.common.http.server.HttpServer;
import io.github.green4j.discas.common.http.server.HttpServer.AcceptHandler;
import io.github.green4j.discas.common.http.server.PrefixRouter;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.logging.LogThrottle;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.operator.OperatorAttention;
import io.github.green4j.discas.common.operator.OperatorState;
import io.github.green4j.discas.common.observability.MetricsHandler;
import io.github.green4j.discas.common.observability.ObservabilityServer;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;
import io.github.green4j.discas.common.transport.tls.CertRotationManager;
import io.github.green4j.discas.common.transport.tls.FileTlsMaterialSource;
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.RenewalPolicy;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsContexts;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;
import io.github.green4j.discas.common.transport.tls.TlsClientSecurityProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The running local DisCas agent: one long-lived {@code DisCasClient} to the configured cluster,
 * fronted by the on-board {@link HttpServer} that serves the KV/lock/health surface. Built from a
 * {@link DisCasAgentConfig} by the {@link #start(DisCasAgentConfig)} factory and stopped by
 * {@link #close()}; the client and the HTTP server run on their own event loops, and handlers bridge
 * client futures back onto the worker thread with {@code response.async(...)}.
 *
 * <p>This is the runtime object only; the command-line entry point that resolves configuration and
 * owns the process lifecycle lives in {@code DisCasAgentStarter}.
 */
public final class DisCasAgent implements AutoCloseable {

    private final HttpServer server;
    private final ObservabilityServer observability; // null when observability is disabled
    private final List<AutoCloseable> toClose;
    /** Held so a close failure has somewhere to go that is not {@code System.err}. */
    private final OperatorAttention attention;
    private volatile boolean closed = false;

    private DisCasAgent(final HttpServer server,
                        final ObservabilityServer observability,
                        final List<AutoCloseable> toClose,
                        final OperatorAttention attention) {
        this.server = server;
        this.observability = observability;
        this.toClose = toClose;
        this.attention = attention;
    }

    /**
     * What a resource failing to close reports. The same state the node raises for the same thing,
     * because it is the same thing: something the process owned did not let go, and the next start
     * may find it held.
     */
    private static Consumer<Exception> closeFailureHandler(final OperatorAttention attention) {
        return failure -> attention.raise(OperatorState.SHUTDOWN_INCOMPLETE, null,
                "a resource threw while closing", failure);
    }

    /**
     * How far below the HTTP request budget the client's own deadlines are set, so the client
     * settles first. Same reasoning as {@code KvHandler.WATCH_WAIT_MARGIN_MS}.
     */
    private static final Duration CLIENT_DEADLINE_MARGIN = Duration.ofSeconds(1);

    /** Floor for the derived deadlines, so an aggressively short request budget still leaves the
     * client time to do something rather than a zero-length window. */
    private static final long MIN_CLIENT_DEADLINE_MS = 500L;

    /**
     * Fit the client's deadlines inside the agent's request budget.
     * <p>
     * The client's defaults are tuned for an embedder with no deadline of its own; the agent has
     * one. Left at the defaults, a scan settles at exactly the ten seconds after which
     * {@code bridge()} answers {@code 504} -- so the two race, and a listing the client was about
     * to return (including a labelled partial one, which is a genuine answer) is discarded in
     * favour of a gateway timeout. Only ever shrinks the defaults: a longer request budget does
     * not make the client wait longer than it otherwise would.
     */
    static DisCasClientConfig clientConfigFor(final Duration requestTimeout) {
        final DisCasClientConfig defaults = DisCasClientConfig.defaults();
        final Duration cap = Duration.ofMillis(Math.max(MIN_CLIENT_DEADLINE_MS,
                requestTimeout.toMillis() - CLIENT_DEADLINE_MARGIN.toMillis()));
        return DisCasClientConfig.builder()
                .scanTimeout(shorter(defaults.scanTimeout(), cap))
                .perAttemptTimeout(shorter(defaults.perAttemptTimeout(), cap))
                .build();
    }

    private static Duration shorter(final Duration a, final Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    /**
     * Build the client + HTTP server for {@code cfg} and start listening. The returned agent owns
     * both (plus the optional nodes-file source and any TLS resources) and is closed to stop it.
     */
    public static DisCasAgent start(final DisCasAgentConfig cfg) throws Exception {
        // Client + TLS resources (material source, rotation, executor) and the optional nodes-file
        // source live as long as the agent; the agent owns and closes them (reverse acquisition
        // order). Closed here if start fails before the agent is built.
        final List<AutoCloseable> toClose = new ArrayList<>();
        final ReloadableClient clientHolder;

        // Observability, composed by decoration exactly as DisCasNodeStarter does it, so each
        // concern stays independent and any of them can be dropped from a chain. Built first
        // because the very first thing the agent does -- read its nodes file, load TLS material --
        // already reports through the reload seam.
        final Log log = new Log(cfg.clientId.value());
        final MetricRegistry metrics = new MetricRegistry();
        // One register for the whole process, shared by the reload seam and the client's chain, so
        // that "the nodes file does not parse" and "a node is unreachable" are one surface with one
        // alert rule over it. The agent has no events of its own; what it needs is an action on
        // each of the ones it forwards.
        final OperatorAttention attention = new OperatorAttention(log);
        attention.registerMetrics(metrics);
        final ReloadObserver reload = new MetricsReloadObserver(metrics,
                new LoggingReloadObserver(log, attention, ReloadObserver.NONE));
        final ClientObserver clientObserver = new MetricsClientObserver(metrics,
                new LoggingClientObserver(log, attention, ClientObserver.NONE));

        // A state with a window says nothing until it has been true long enough, and nothing fires
        // while a condition merely persists -- so the register needs a tick. A daemon thread of its
        // own here, unlike the node, which has a loop to hang it on: the client's loop is its own
        // business and the agent has none.
        final ScheduledExecutorService attentionTicker = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    final Thread thread = new Thread(r, "discas-agent-attention");
                    thread.setDaemon(true);
                    return thread;
                });
        // Throttled because the failure this catches is almost certainly not a one-off: whatever
        // makes checkDue throw will make it throw again on the next tick, so an unthrottled line
        // here is a stack trace every second for as long as the agent runs -- the log storm shape
        // exactly. The node needs no equivalent: its tick runs on the event loop, whose failures go
        // through the attention register, which reports the first occurrence per component and no
        // more.
        final LogThrottle tickFailures = new LogThrottle();
        attentionTicker.scheduleAtFixedRate(() -> {
            try {
                attention.checkDue();
            } catch (final RuntimeException e) {
                // A throw here would cancel the schedule silently and take the surface with it.
                tickFailures.error(log, "operator attention check failed", e);
            }
        }, OperatorAttention.CHECK_INTERVAL.toMillis(), OperatorAttention.CHECK_INTERVAL.toMillis(),
                TimeUnit.MILLISECONDS);
        toClose.add(attentionTicker::shutdownNow);

        try {
            final ClientSecurityProvider security = clientSecurity(cfg, toClose, reload);
            clientHolder = new ReloadableClient(
                    cfg.clientId, cfg.clientTransportConfig, cfg.token, security, cfg.nodes,
                    clientConfigFor(cfg.requestTimeout),
                    Executors.newSingleThreadExecutor(
                            r -> new Thread(r, "discas-agent-client-retire")),
                    reload,
                    clientObserver);
            toClose.add(clientHolder);
            if (cfg.nodesFromFile()) {
                // The target-node list: on each reload that changes it the holder rebuilds and
                // swaps the client. The source keeps the last good value if a reload is refused.
                final ReloadableFileSource<Map<NodeId, InetSocketAddress>> nodesSource =
                        new ReloadableFileSource<>(
                                List.of(cfg.nodesFile.toAbsolutePath()),
                                contents -> ConfigSupport.parseMembersProperties(contents.get(0), "nodes"),
                                DisCasAgent::nodesSummary,
                                reload);
                nodesSource.addListener(clientHolder::onNodesReloaded);
                toClose.add(nodesSource);
            }
        } catch (final Exception e) {
            Closeables.closeAll(toClose, closeFailureHandler(attention));
            throw e;
        }

        final PrefixRouter router = PrefixRouter.builder()
                .route(KvHandler.PREFIX, connection -> new KvHandler(clientHolder, cfg.requestTimeout))
                .route(LockHandler.PREFIX, connection -> new LockHandler(clientHolder, cfg.requestTimeout))
                .route("/v1/agent/health",
                        connection -> new HealthHandler(cfg.clientId.value(), clientHolder))
                .route(ReloadHandler.PREFIX, connection -> new ReloadHandler(clientHolder))
                .build(); // default fallback: bodyless 404

        final AcceptHandler acceptHandler = new AcceptHandler() {
            @Override
            public void onOpen(final ServerSocketChannel serverChannel) throws IOException {
                serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            }

            @Override
            public boolean onAccept(final ServerSocketChannel serverChannel,
                                    final SocketChannel channel) throws IOException {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                return true;
            }
        };

        final InetSocketAddress bind = cfg.httpBind;
        final HttpServer server = new HttpServer(
                HttpServer.builder()
                        .bindAddress(bind.getHostString())
                        .port(bind.getPort())
                        .workerCount(cfg.httpWorkers)
                        .build(),
                acceptHandler,
                router);
        try {
            server.start();
        } catch (final Exception e) {
            Closeables.closeAll(toClose, closeFailureHandler(attention));
            throw e;
        }

        // Metrics last, on their own port, so the endpoint only ever exposes a fully built agent.
        // Appended to the close list, which drains in reverse: it is torn down first, before the
        // client whose counters it reports.
        final ObservabilityServer observability;
        try {
            observability = ObservabilityServer.start(
                    cfg.observabilityConfig,
                    PrefixRouter.builder()
                            .route("/metrics", connection -> new MetricsHandler(metrics))
                            .build()); // default fallback: bodyless 404
            if (observability != null) {
                toClose.add(observability);
                log.info("Metrics on " + cfg.observabilityConfig.bindAddress()
                        + ":" + observability.port() + " (/metrics)");
            }
        } catch (final Exception e) {
            server.close();
            Closeables.closeAll(toClose, closeFailureHandler(attention));
            throw e;
        }
        return new DisCasAgent(server, observability, toClose, attention);
    }

    /** The bound HTTP port (useful with an ephemeral {@code http-bind} port of 0). */
    public int port() {
        return server.boundPort();
    }

    /**
     * The bound metrics port, or {@code -1} when {@code --observability-enabled} is off. Useful
     * with an ephemeral {@code observability-bind} port of 0, for the same reason as {@link #port()}.
     */
    public int metricsPort() {
        return observability == null ? -1 : observability.port();
    }

    /**
     * Re-read every file this agent is serving -- its nodes file, its TLS key and trust stores --
     * and apply the result, all of it or none of it. What {@code POST /v1/agent/reload} does; an
     * agent started with an inline {@code --nodes} list and no TLS has nothing to read and reports
     * an empty set.
     */
    public ReloadReport reloadFiles() {
        return ReloadableFiles.shared().reloadAll();
    }

    /**
     * Stop the agent (best-effort). The server is closed first to stop accepting requests, then
     * {@code toClose} is drained in reverse acquisition order (nodes file, then client, then TLS).
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            server.close();
        } catch (final Exception ignored) {
            // best-effort shutdown
        }
        Closeables.closeAll(toClose, closeFailureHandler(attention));
    }

    /**
     * Build the outbound client's channel security from {@code cfg}, mirroring the TLS wiring of
     * {@code DisCasNodeStarter} but on the client side. Returns {@code null} for plaintext (the
     * caller then uses the token-only bootstrap). Any long-lived resources (material source, cert
     * rotation, its executor) are appended to {@code toClose} in acquisition order.
     *
     * <ul>
     *   <li><b>mTLS</b> (keystore present): reloadable context from a {@link FileTlsMaterialSource};
     *   with {@code tls-cert-rotation} a {@link CertRotationManager} swaps in a rotated client cert on a
     *   dedicated single-thread executor (the client factory owns its event loop and does not expose
     *   it, so the agent supplies its own serializing executor for the atomic swaps).</li>
     *   <li><b>Server-authenticated TLS + token</b> (no keystore): a trust-only context loaded once;
     *   the client presents no cert and authenticates with the token.</li>
     * </ul>
     */
    /**
     * The nodes file's reload report: which nodes the agent will now dial. Sorted, because the
     * parse comes out of {@code Properties} in no order at all and a report that reshuffles itself
     * cannot be compared with the last one. Addresses are topology, and are the point of reading
     * it -- an agent that quietly kept the old list looks exactly like one that took the new.
     */
    static String nodesSummary(final Map<NodeId, InetSocketAddress> nodes) {
        final List<NodeId> ids = new ArrayList<>(nodes.keySet());
        Collections.sort(ids);
        final StringBuilder sb = new StringBuilder();
        sb.append(ids.size()).append(ids.size() == 1 ? " node: " : " nodes: ");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            final InetSocketAddress address = nodes.get(ids.get(i));
            sb.append(ids.get(i).value()).append('=')
                    .append(address.getHostString()).append(':').append(address.getPort());
        }
        return sb.toString();
    }

    private static ClientSecurityProvider clientSecurity(final DisCasAgentConfig cfg,
                                                         final List<AutoCloseable> toClose,
                                                         final ReloadObserver reload)
            throws Exception {
        if (!cfg.tls) {
            // The null object, as DisCasNodeStarter's counterpart returns -- returning null here
            // pushed the same normalisation onto ReloadableClient.
            return PlaintextClientSecurity.PROVIDER;
        }
        if (cfg.tlsKeystore == null) {
            // Server-authenticated TLS: verify the node cert, present none of our own.
            final KeyStore trustStore = loadPkcs12(cfg.tlsTruststore, cfg.tlsTruststorePassword);
            return new TlsClientSecurityProvider(TlsConfig.of(TlsContexts.buildTrustOnly(trustStore)));
        }
        // mTLS: present a client cert (CN=client-id) and verify the node cert.
        final FileTlsMaterialSource source = new FileTlsMaterialSource(
                cfg.tlsKeystore, cfg.tlsKeystorePassword,
                cfg.tlsTruststore, cfg.tlsTruststorePassword, reload);
        final TlsMaterial initial = source.snapshot();
        final ReloadableTlsContext context = ReloadableTlsContext.create(initial);
        if (cfg.tlsCertRotation) {
            final ExecutorService rotationExecutor = Executors.newSingleThreadExecutor(
                    r -> new Thread(r, "discas-agent-cert-rotation"));
            toClose.add(rotationExecutor::shutdownNow);
            toClose.add(source);
            final CertRotationManager rotator = new CertRotationManager(
                    context, source, initial, RenewalPolicy.defaults(),
                    rotationExecutor, reload);
            rotator.start();
            toClose.add(rotator);
        } else {
            source.close();
        }
        return new TlsClientSecurityProvider(TlsConfig.of(context.sslContext()));
    }

    private static KeyStore loadPkcs12(final Path path, final char[] password)
            throws Exception {
        final KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(path)) {
            ks.load(in, password);
        }
        return ks;
    }

    /** Close resources in reverse order of acquisition, best-effort. */
}
