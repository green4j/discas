/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;

import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A hot-swappable holder around a {@link DisCasClient}. The agent's HTTP handlers read {@link #current()}
 * once per request; the agent's nodes-file watcher calls {@link #onNodesReloaded} whenever the target-node
 * list changes.
 *
 * <p>Unlike the node's peer transport (which takes a live membership source), the client stack captures
 * its peer set once at construction ({@code DisCasClient.peers} drives routing, scan fan-out and retry
 * math). So a membership change is applied by building a <b>new</b> {@link DisCasClient} for the new node
 * map and atomically swapping it in; the outgoing client is shut down off-thread (its {@code shutdown()}
 * blocks on event-loop termination, which must not stall the file-watch thread). The {@link ClientSecurityProvider}
 * is supplied once and reused across rebuilds, so TLS/cert-rotation is independent of nodes-file reloads.
 *
 * <p>{@code onNodesReloaded} is only ever called from {@code WatchedFileSource}, which serializes listener
 * callbacks, so it never runs concurrently with itself.
 */
final class ReloadableClient implements AutoCloseable {

    private final ClientId clientId;
    private final ClientTransportConfig transportConfig;
    private final String token;
    private final ClientSecurityProvider security;
    // Deadlines are fixed for the agent's lifetime (derived from the request budget), so they
    // survive a nodes-file reload unchanged, exactly like the security provider.
    private final DisCasClientConfig clientConfig;

    private final AtomicReference<DisCasClient> current = new AtomicReference<>();
    // The node map behind the current client; guards against redundant rebuilds (e.g. the
    // replay-on-subscribe of the initial value). Only mutated from onNodesReloaded/construction.
    private volatile Map<NodeId, InetSocketAddress> currentNodes;

    // Shuts down superseded clients so a reload never blocks the file-watch thread. Injected,
    // like the executor CertRotationManager takes: this class had built its own while its own
    // module sibling handed one in.
    private final ExecutorService retirer;
    private final ReloadObserver reloadObserver;
    // Survives a rebuild: the observer chain is wired once by DisCasAgent and every replacement
    // client reports into the same log and the same metric registry, so a nodes-file reload does
    // not reset the agent's counters.
    private final ClientObserver clientObserver;

    private volatile boolean closed = false;

    ReloadableClient(final ClientId clientId,
                     final ClientTransportConfig transportConfig,
                     final String token,
                     final ClientSecurityProvider security,
                     final Map<NodeId, InetSocketAddress> initialNodes,
                     final DisCasClientConfig clientConfig,
                     final ExecutorService retirer,
                     final ReloadObserver reloadObserver,
                     final ClientObserver clientObserver) {
        this.retirer = retirer;
        this.reloadObserver = reloadObserver == null ? ReloadObserver.NONE : reloadObserver;
        this.clientObserver = clientObserver == null ? ClientObserver.NONE : clientObserver;
        this.clientId = clientId;
        this.transportConfig = transportConfig;
        this.token = token;
        this.security = security == null ? PlaintextClientSecurity.PROVIDER : security;
        this.clientConfig = clientConfig;
        this.currentNodes = new LinkedHashMap<>(initialNodes);
        this.current.set(build(this.currentNodes));
    }

    /** The client in effect right now; read once per request by handlers. */
    DisCasClient current() {
        return current.get();
    }

    /** The node ids the current client is configured to reach (for the health endpoint). */
    Set<NodeId> currentNodeIds() {
        return currentNodes.keySet();
    }

    /**
     * Apply a reloaded node map: no-op if unchanged, otherwise build a new client, swap it in, and
     * retire the previous one off-thread. Called only from the file-watch thread.
     */
    void onNodesReloaded(final Map<NodeId, InetSocketAddress> nodes) {
        if (closed || nodes.equals(currentNodes)) {
            return;
        }
        final Map<NodeId, InetSocketAddress> next = new LinkedHashMap<>(nodes);
        final DisCasClient fresh = build(next);
        currentNodes = next;
        final DisCasClient previous = current.getAndSet(fresh);
        retire(previous);
        reloadObserver.reloaded("nodes", next.keySet().toString());
    }

    private DisCasClient build(final Map<NodeId, InetSocketAddress> nodes) {
        // The agent is a standalone process, so it opts into a logging + metrics chain the way
        // DisCasNodeStarter does. The library default stays silent: an embedding application routes
        // these events into its own backend instead.
        final TcpClientBootstrap bootstrap = new TcpClientBootstrap(
                nodes, transportConfig, token,
                security,
                clientObserver);
        return DisCasClientFactory.create(clientId, bootstrap, clientConfig);
    }

    private void retire(final DisCasClient client) {
        if (client == null) {
            return;
        }
        try {
            retirer.execute(() -> shutdownQuietly(client));
        } catch (final RuntimeException rejected) {
            // Executor already shut down (racing close): fall back to inline shutdown.
            shutdownQuietly(client);
        }
    }

    private static void shutdownQuietly(final DisCasClient client) {
        try {
            client.close();
        } catch (final Exception ignored) {
            // best-effort shutdown
        }
    }

    @Override
    public void close() {
        closed = true;
        shutdownQuietly(current.get());
        retirer.shutdown();
    }
}
