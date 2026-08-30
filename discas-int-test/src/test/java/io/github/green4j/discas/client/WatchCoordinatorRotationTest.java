/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.NodeState;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Which members a watch addresses across its polls, counted at the nodes rather than inferred.
 * <p>
 * A serializable poll is answered from one member's local state, so a watch that keeps asking one
 * member keeps getting one member's answer; rotating the polls is what makes the whole membership
 * observable within one watch. A linearizable poll does not rotate -- it runs a full round and
 * therefore contends on the register like a write, so spreading those over coordinators would be
 * the ballot duel coordinator affinity exists to avoid.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Watch polling -- which members a serializable watch addresses")
class WatchCoordinatorRotationTest {

    private static final List<NodeId> MEMBERS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));
    /** Long enough for several polls at the 200-1000 ms watch backoff. */
    private static final Duration WATCH_BUDGET = Duration.ofSeconds(4);

    private final Map<NodeId, DisCasNode> nodes = new LinkedHashMap<>();
    private final Map<NodeId, AtomicInteger> serializableReads = new LinkedHashMap<>();
    private final List<EventLoop> loops = new ArrayList<>();

    private DisCasClient client;

    @BeforeEach
    void setup() throws Exception {
        for (final NodeId member : MEMBERS) {
            final AtomicInteger reads = new AtomicInteger();
            serializableReads.put(member, reads);

            final EventLoop loop = new EventLoop("rotation-node-" + member.value());
            loops.add(loop);
            final DisCasNode node = new DisCasNode(
                    new NodeConfig(member, ClusterId.of("rotation"), MEMBERS.size()),
                    new InMemoryWal(), loop,
                    new InProcessPeerTransport(member, MEMBERS.size(), loop,
                            InMemoryMembers.ofNodes(MEMBERS)),
                    new CountingObserver(reads));
            node.registerClientMessages(ingress ->
                    InProcessClientRegistry.register(member, loop, ingress, node.clusterSize()));
            nodes.put(member, node);
        }
        for (final DisCasNode node : nodes.values()) {
            node.start();
        }
        TestAwait.until("Every node serves", Duration.ofSeconds(60), () -> {
            for (final DisCasNode node : nodes.values()) {
                final NodeState state = node.healthSource().state();
                if (state != NodeState.SERVING) {
                    throw new IllegalStateException(node.nodeId().value() + " is " + state);
                }
            }
        });

        client = DisCasClientFactory.create(ClientId.of("rotation-client"),
                new InProcessClientBootstrap(MEMBERS));
        TestAwait.awaitReady(client);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            try {
                client.close();
            } catch (final Exception ignored) {
            }
        }
        for (final DisCasNode node : nodes.values()) {
            try {
                node.close();
            } catch (final Exception ignored) {
            }
        }
        for (final NodeId member : MEMBERS) {
            InProcessClientRegistry.unregister(member);
        }
    }

    @Test
    @DisplayName("A serializable watch addresses every member across its polls")
    void serializableWatchRotates() throws Exception {
        final ByteBuffer key = TestBytes.utf8("rotation/quiet");
        final Version version = client.put(key.duplicate(), TestBytes.utf8("v"))
                .get(8, TimeUnit.SECONDS);
        resetCounters();

        // Nothing writes during the watch, so it polls until its budget runs out. What it reports
        // is not the point here; where it asked is.
        final WatchResult result = client
                .watch(key.duplicate(), version, WATCH_BUDGET, ReadConsistency.SERIALIZABLE)
                .get(30, TimeUnit.SECONDS);
        assertFalse(result.changed(), "Nothing wrote, so the watch must report the key as quiet");

        int addressed = 0;
        for (final NodeId member : MEMBERS) {
            if (serializableReads.get(member).get() > 0) {
                addressed++;
            }
        }
        assertEquals(MEMBERS.size(), addressed,
                "Every member must have answered a poll, saw " + serializableReads);
    }

    /**
     * The negative control: a linearizable watch must not take the serializable path at all.
     * <p>
     * That its polls also keep the key's home coordinator is <b>not</b> asserted here, because
     * nothing outside the node reveals it: a linearizable read short-circuits after its prepare
     * quorum ({@code allowReadOnlyShortCircuit}) and never reaches the completion that would name
     * the coordinator in an observer callback. The property is carried by one line in
     * {@code watchAttempt} instead of by a test, and this case guards the half that is visible.
     */
    @Test
    @DisplayName("A linearizable watch takes no serializable reads")
    void linearizableWatchIsNotRerouted() throws Exception {
        final ByteBuffer key = TestBytes.utf8("rotation/linearizable");
        final Version version = client.put(key.duplicate(), TestBytes.utf8("v"))
                .get(8, TimeUnit.SECONDS);
        resetCounters();

        final WatchResult result = client
                .watch(key.duplicate(), version, WATCH_BUDGET, ReadConsistency.LINEARIZABLE)
                .get(30, TimeUnit.SECONDS);
        assertFalse(result.changed(), "Nothing wrote, so the watch must report the key as quiet");

        for (final NodeId member : MEMBERS) {
            assertEquals(0, serializableReads.get(member).get(),
                    "A linearizable watch must run rounds, not local reads, saw " + serializableReads);
        }
    }

    private void resetCounters() {
        for (final NodeId member : MEMBERS) {
            serializableReads.get(member).set(0);
        }
    }

    /** Counts the one callback that names the member a serializable poll was served by. */
    private static final class CountingObserver implements NodeObserver {
        private final AtomicInteger serializableReads;

        private CountingObserver(final AtomicInteger serializableReads) {
            this.serializableReads = serializableReads;
        }

        @Override
        public void serializableRead(final HashedBytes key) {
            serializableReads.incrementAndGet();
        }
    }
}
