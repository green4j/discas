/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.TestPorts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Node-id to peer-port map for the chaos harness. The allocation policy (hold every probe in a
 * batch open, never reissue a port within this JVM, retry with backoff) lives in
 * {@link TestPorts}; this only shapes the result into the map the harness wants.
 */
final class ChaosPortAllocator {

    private ChaosPortAllocator() {
    }

    static Map<Integer, Integer> allocateNodePorts(final int clusterSize) {
        final List<Integer> ports = TestPorts.allocate(clusterSize);
        final Map<Integer, Integer> byNodeId = new HashMap<>();
        for (int nodeId = 1; nodeId <= clusterSize; nodeId++) {
            byNodeId.put(nodeId, ports.get(nodeId - 1));
        }
        return byNodeId;
    }
}
