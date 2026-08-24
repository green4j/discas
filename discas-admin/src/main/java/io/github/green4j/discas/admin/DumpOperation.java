/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.dump.ClusterDump;
import io.github.green4j.discas.client.dump.ClusterDump.DumpSummary;
import io.github.green4j.discas.client.dump.DumpProgress;

import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Dumping a cluster to a file: connect a client, wait for the cluster to answer, run
 * {@link ClusterDump}, and hand back what it did.
 *
 * <p>No command line here and no printing -- {@code DumpCommand} owns the options an operator
 * types and the words they read. This is the part that would be identical if the same operation
 * were ever driven by something other than a terminal.
 */
public final class DumpOperation {

    private DumpOperation() {
    }

    /**
     * @param out  created, and only ever created: an existing file is refused rather than
     *             overwritten, which is the right default for the one file somebody reaches for
     *             when everything else is gone
     * @return what the finished dump carried
     */
    public static DumpSummary run(final Map<NodeId, InetSocketAddress> nodes,
                                  final ClientId clientId,
                                  final String token,
                                  final List<ByteBuffer> prefixes,
                                  final Path out,
                                  final long connectTimeoutMs,
                                  final DumpProgress progress) throws Exception {
        try (DisCasClient client = ClusterConnection.open(nodes, clientId, token);
                FileChannel channel = FileChannel.open(out,
                        EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            awaitCluster(client, prefixes, connectTimeoutMs);
            // No overall deadline: a dump of a large key space legitimately takes hours, and the
            // per-request timeouts that bound a stuck cluster are the client's.
            return ClusterDump.dump(client, prefixes, channel, progress).get();
        }
    }

    /**
     * Waits for the cluster to answer, by asking for one page of exactly the scan the dump is about
     * to run. A client connects lazily and learns {@code N} from the first handshake, so a dump
     * started the millisecond after {@code create} fails for want of a quorum it has not met yet.
     * Probing with the real ask also means a permission the dump lacks is reported before a file
     * exists rather than halfway through one.
     */
    private static void awaitCluster(final DisCasClient client,
                                     final List<ByteBuffer> prefixes,
                                     final long budgetMs) throws Exception {
        final ByteBuffer probe = prefixes.isEmpty() ? ByteBuffers.EMPTY : prefixes.get(0);
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMs);
        Exception last = null;
        do {
            try {
                client.scan(probe, null, 1).get(1, TimeUnit.SECONDS);
                return;
            } catch (final Exception notYet) {
                last = notYet;
                Thread.sleep(100);
            }
        } while (System.nanoTime() < deadline);
        throw new IOException("The cluster did not answer within " + budgetMs + "ms", last);
    }
}
