/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.dump.ClusterLoad;
import io.github.green4j.discas.client.dump.ClusterLoad.LoadSummary;
import io.github.green4j.discas.client.dump.LoadProgress;

import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Loading a dump into a running cluster: connect a client and write every pair.
 *
 * <p>No readiness probe, unlike a dump. A dump starts with a scan, which needs to know {@code N}
 * before it can tell a quorum from a subset, so it has to wait for the first handshake; a write
 * waits for a coordinator inside its own request deadline, which is the client's job and not this
 * one's.
 */
public final class LoadOperation {

    private LoadOperation() {
    }

    /**
     * @param cleanupPrefixes where to delete keys the dump did not carry, once every pair is in;
     *                        {@code null} for no cleanup at all, empty for the whole key space
     */
    public static LoadSummary run(final Map<NodeId, InetSocketAddress> nodes,
                                  final ClientId clientId,
                                  final String token,
                                  final Path dump,
                                  final List<ByteBuffer> cleanupPrefixes,
                                  final LoadProgress progress) throws IOException {
        try (DisCasClient client = ClusterConnection.open(nodes, clientId, token)) {
            return cleanupPrefixes == null
                    ? ClusterLoad.load(client, dump, progress)
                    : ClusterLoad.loadAndCleanup(client, dump, cleanupPrefixes, progress);
        }
    }
}
