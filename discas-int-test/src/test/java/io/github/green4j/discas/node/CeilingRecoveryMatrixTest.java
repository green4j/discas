/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Every way a cluster of three can start, and what each member ends up doing.
 *
 * <p>At the moment it starts, a member is in one of three conditions, and nothing else about its
 * storage matters here:
 * <ul>
 *   <li><b>P</b> -- it can prove its own promise ceiling (an intact log).</li>
 *   <li><b>U</b> -- it cannot (no log, a hole in one, or a snapshot deleted from under a compacted
 *       one). A node that fails to start at all -- a torn segment, an unreadable marker -- is
 *       <b>D</b> as far as the cluster is concerned.</li>
 *   <li><b>D</b> -- not started.</li>
 * </ul>
 *
 * <p>That is ten combinations up to permutation, and this drives nine of them (DDD has nothing to
 * assert). Two claims are checked in each: <b>no member is stuck part-way</b> -- every started node
 * reaches either {@code SERVING} or {@code AWAITING_FLOOR}, and never sits in {@code REPLAYING} --
 * and <b>each reaches the best state that is safe</b>, which for the waiting rows means that no
 * safe floor exists to be had:
 *
 * <pre>
 *   PPP PPD PDD   every member serves; nothing to recover
 *   PPU           the replacement gets two witnesses and serves -- the disk-swap case
 *   UUU           nobody holds anything, so there is no history to be below: all serve at floor 0
 *   PUU PUD UUD   one witness at most, and a quorum it does not intersect: the U members wait
 *   UDD           nothing answers at all: waits
 * </pre>
 *
 * <p>The waiting rows are not a limitation of the mechanism. With {@code N=3} a promise quorum is
 * two members, so a single witness always leaves exactly one quorum -- the other two members --
 * unaccounted for; if those two are the ones that lost their state, the bound they promised at
 * exists nowhere. Waiting is the strongest safe answer, and it ends by itself if a member with an
 * intact disk turns up.
 *
 * @see CeilingVerdictTest for the rule itself, driven with hand-made answers
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
@DisplayName("Ceiling recovery -- every start combination of a cluster of three")
class CeilingRecoveryMatrixTest {

    private static final ClusterId CLUSTER = ClusterId.of("matrix-cluster");
    private static final List<NodeId> MEMBERS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));
    /**
     * How long a reached state must hold to count as settled. A sleep rather than a wait, and the
     * only kind that earns one: the claim is that nothing further happens, so the elapsed time is
     * the substance rather than a stand-in for a condition. Several retry rounds at the 100ms cap
     * below.
     */
    private static final Duration SETTLE = Duration.ofSeconds(3);

    private final Map<NodeId, DisCasNode> started = new LinkedHashMap<>();
    private final List<EventLoop> loops = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (final DisCasNode node : started.values()) {
            try {
                node.close();
            } catch (final Exception ignored) {
                // a node parked in AWAITING_FLOOR closes like any other
            }
        }
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
        // Conditions at start, then the state each started member must be in. One row per branch
        // of the rule, not per shape of cluster: PPP/PPD/PDD all have every started member proving
        // its own ceiling, so no node ever enters recovery and the count of absent members is not
        // read by anyone; PUU/PUD are both "one witness where two are needed"; UUD/UDD are both
        // "a member never answers, so the sweep can never be clean". The rule itself is driven
        // exhaustively, with hand-made answers, by the ceiling-verdict unit tests.
        "PPD, SERVING SERVING -",
        // A replacement disk among proven members: the sweep finds its two witnesses.
        "PPU, SERVING SERVING SERVING",
        // Nobody can prove anything, and the sweep is clean: the floor is 0, or the cluster
        // deadlocks waiting for evidence that no longer exists anywhere.
        "UUU, SERVING SERVING SERVING",
        "PUU, SERVING AWAITING_FLOOR AWAITING_FLOOR",
        "UUD, AWAITING_FLOOR AWAITING_FLOOR -",
    })
    void everyStartCombination(final String conditions, final String expected) throws Exception {
        for (int i = 0; i < MEMBERS.size(); i++) {
            switch (conditions.charAt(i)) {
                case 'P': start(MEMBERS.get(i), provenWal()); break;
                case 'U': start(MEMBERS.get(i), new InMemoryWal()); break;
                default: break; // D: not started
            }
        }

        final String[] want = expected.split(" ");
        // One settle for the whole cluster: a state that is going to change does so within a few
        // retry rounds, and a state that is not will still be there afterwards.
        awaitSettled(want);
        for (int i = 0; i < MEMBERS.size(); i++) {
            final DisCasNode node = started.get(MEMBERS.get(i));
            if ("-".equals(want[i])) {
                continue;
            }
            assertEquals(NodeState.valueOf(want[i]), node.healthSource().state(),
                    "member " + MEMBERS.get(i).value() + " of " + conditions);
        }
    }

    /**
     * Wait until every member is in its expected state, then confirm it holds. The confirmation is
     * what makes the {@code AWAITING_FLOOR} rows mean something: a node that had not got round to
     * serving yet would satisfy the first check and fail the second.
     */
    private void awaitSettled(final String[] want) throws Exception {
        TestAwait.until("every member reaches its expected state", Duration.ofSeconds(30), () -> {
            for (int i = 0; i < MEMBERS.size(); i++) {
                final DisCasNode node = started.get(MEMBERS.get(i));
                if (node == null) {
                    continue;
                }
                final NodeState state = node.healthSource().state();
                if (state != NodeState.valueOf(want[i])) {
                    throw new IllegalStateException(
                            MEMBERS.get(i).value() + " is " + state + ", want " + want[i]);
                }
            }
        });
        Thread.sleep(SETTLE.toMillis());
    }

    /** A log holding a forced ceiling: a member that can prove one. */
    private static InMemoryWal provenWal() {
        final InMemoryWal wal = new InMemoryWal();
        wal.append(new Wal.Entry.PromiseCeiling(4096L));
        wal.force();
        return wal;
    }

    private void start(final NodeId id, final InMemoryWal wal) {
        final EventLoop loop = new EventLoop("matrix-" + id.value());
        loops.add(loop);
        final DisCasNode node = new DisCasNode(
                NodeConfig.builder()
                        .nodeId(id)
                        .clusterId(CLUSTER)
                        .clusterSize(MEMBERS.size())
                        .ceilingRecoveryRetryCap(Duration.ofMillis(100))
                        .build(),
                wal, loop,
                new InProcessPeerTransport(id, MEMBERS.size(), loop, InMemoryMembers.ofNodes(MEMBERS)));
        started.put(id, node);
        node.start();
    }
}
