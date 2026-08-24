/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.observability;

import io.github.green4j.discas.common.http.server.HttpServer;
import io.github.green4j.discas.common.http.server.HttpServer.AcceptHandler;
import io.github.green4j.discas.common.http.server.PrefixRouter;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * A process's observability endpoints, served by the project's own {@link HttpServer} on a port of
 * their own. What is served is the caller's {@link PrefixRouter}, because the routes differ by
 * process: a node exposes {@code /metrics}, {@code /health} and {@code /ready}, while the agent --
 * which holds a client and therefore has no view of quorum -- exposes {@code /metrics} only.
 * <p>
 * The socket setup is the part worth sharing: {@code SO_REUSEADDR} so a restart does not trip over
 * its own {@code TIME_WAIT}, and {@code TCP_NODELAY} because every response here is a single small
 * body that should not wait on Nagle. Both are what {@code discas-agent}'s own server does.
 * <p>
 * Closing the server closes the listening socket and its workers. Register it with the owning
 * process's lifecycle (a node's {@code addLifecycleCloseable}, the agent's close list) so it goes
 * away with the process rather than outliving it.
 */
public final class ObservabilityServer implements AutoCloseable {

    private final HttpServer server;

    private ObservabilityServer(final HttpServer server) {
        this.server = server;
    }

    /**
     * Builds and starts the endpoints.
     *
     * @param router the routes to serve; build it with a bodyless 404 default, so an unknown path
     *               costs nothing and reveals nothing
     * @return the started server, or {@code null} when {@code config.enabled()} is false -- the
     *         caller has nothing to close in that case
     */
    public static ObservabilityServer start(final ObservabilityConfig config,
                                            final PrefixRouter router) throws IOException {
        if (!config.enabled()) {
            return null;
        }

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

        final HttpServer server = new HttpServer(
                HttpServer.builder()
                        .bindAddress(config.bindAddress())
                        .port(config.port())
                        .workerCount(config.workerCount())
                        .build(),
                acceptHandler,
                router);
        server.start();
        return new ObservabilityServer(server);
    }

    /** The bound port, which is what a caller needs when the configured port was {@code 0}. */
    public int port() {
        return server.boundPort();
    }

    @Override
    public void close() {
        server.close();
    }
}
