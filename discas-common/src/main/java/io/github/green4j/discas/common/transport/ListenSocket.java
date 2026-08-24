/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;

/**
 * A listening socket that is already bound, so its address is a fact rather than a plan.
 * <p>
 * A transport that binds inside its own constructor can only be told <em>where</em> to listen,
 * which forces anyone who needs the address in advance to guess one: probe for a free port, close
 * it, and hope it is still free by the time the real bind happens. Binding first and passing the
 * socket in removes the guess -- and with it the window in which something else can take the port.
 * <p>
 * That matters most for a peer mesh, where every node's address has to appear in the members map
 * that every other node validates at construction: bind all of them, then build the map from what
 * they actually got.
 * <p>
 * Ownership transfers to the transport that accepts it: closing the transport closes this. Close
 * it yourself only if you never hand it over.
 */
public final class ListenSocket implements AutoCloseable {

    private final ServerSocketChannel channel;

    private ListenSocket(final ServerSocketChannel channel) {
        this.channel = channel;
    }

    /**
     * Bind {@code address} now. Port {@code 0} binds an ephemeral port, which {@link #port()}
     * then reports.
     *
     * @throws TransportSetupException if the address cannot be bound
     */
    public static ListenSocket bind(final InetSocketAddress address) {
        ServerSocketChannel opened = null;
        try {
            opened = ServerSocketChannel.open();
            opened.configureBlocking(false);
            // Explicit rather than load-bearing: the JDK already defaults this to true for a
            // ServerSocketChannel on the platforms we run on, but the initial value is documented
            // as platform-dependent (notably not on Windows). Stating it keeps a restart from
            // failing to rebind over lingering TIME_WAIT state wherever the default differs. Must
            // precede bind(). It permits reusing a local address still held by TIME_WAIT sockets;
            // it does not let a second live listener steal a port.
            opened.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            opened.bind(address);
            return new ListenSocket(opened);
        } catch (final IOException e) {
            if (opened != null) {
                try {
                    opened.close();
                } catch (final Exception ignored) {
                    // The bind already failed; a failure to close adds nothing to report.
                }
            }
            throw new TransportSetupException(TransportSetupException.Fault.INITIALIZATION_FAILED,
                    "Cannot bind " + address, e);
        }
    }

    /** The port actually bound -- never {@code 0}, even when {@code 0} was requested. */
    public int port() {
        return address().getPort();
    }

    /** The address actually bound. */
    public InetSocketAddress address() {
        try {
            return (InetSocketAddress) channel.getLocalAddress();
        } catch (final IOException e) {
            throw new TransportSetupException(TransportSetupException.Fault.INITIALIZATION_FAILED,
                    "Cannot read the bound address", e);
        }
    }

    /** The bound channel, for the transport taking ownership of it. */
    public ServerSocketChannel channel() {
        return channel;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (final Exception ignored) {
            // Best-effort, like every other close on the shutdown path.
        }
    }
}
