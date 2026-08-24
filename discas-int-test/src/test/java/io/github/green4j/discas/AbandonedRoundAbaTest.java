/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.common.KeyHash;
import io.github.green4j.discas.common.client.ReadConsistency;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.github.green4j.discas.TestBytes.string;
import static io.github.green4j.discas.TestBytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A coordinator the client has given up on is still driving its proposal, and it can commit after
 * the client has written the attempt off. This is the interleaving that decided the store's write
 * API.
 * <p>
 * Against a <b>value-compared</b> CAS it is fatal: when the register returns to the value the
 * abandoned attempt expected, that attempt's compare succeeds a second time and silently reverts
 * whoever wrote in between. No caller can detect it afterwards -- "not applied" and "applied, then
 * reverted" are the same observation -- which is why the store offers no such operation.
 * <p>
 * Against a <b>version-fenced</b> CAS the same interleaving is a no-op: the intervening write has
 * already overtaken the ballot the stale attempt carries, so the duplicate is rejected before
 * anything is proposed.
 * <p>
 * {@link LinearizabilityCheckerChaosTest} cannot express this: it <em>drops</em> traffic for a
 * window shorter than the round timeout, so an abandoned round fails rather than completing late.
 * The interleaving here needs the opposite -- traffic <em>deferred</em> past the client's
 * per-attempt timeout and delivered afterwards.
 */
@DisplayName("A version-fenced round must not re-apply across an intervening write")
class AbandonedRoundAbaTest {

    private static final int CLUSTER_SIZE = 3;
    private static final String KEY = "aba/register";

    /** Short, so the client abandons the held coordinator quickly. */
    private static final Duration PER_ATTEMPT = Duration.ofMillis(300);

    /** Long, so the held coordinator's round waits rather than failing while we hold it. */
    private static final Duration ROUND_TIMEOUT = Duration.ofSeconds(60);

    /** How long the released round gets to land before we read the verdict. */
    private static final Duration SETTLE = Duration.ofSeconds(5);

    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(15);

    @Test
    @DisplayName("The same interleaving is rejected when the CAS is fenced on the version")
    void versionedCasRejectsTheAbandonedRound() throws Exception {
        final DisCasClientConfig clientConfig = DisCasClientConfig.builder()
                .perAttemptTimeout(PER_ATTEMPT)
                .requestDeadline(Duration.ofSeconds(10))
                .build();

        try (TestCluster cluster = new TestCluster(CLUSTER_SIZE, 1,
                b -> b.roundTimeout(ROUND_TIMEOUT).maxRoundRetries(5),
                clientConfig)) {

            cluster.start();
            cluster.awaitReady();

            final DisCasClient client = cluster.client(0);
            final ByteBuffer key = utf8(KEY);
            final int firstCoordinator = coordinatorFor(key, CLUSTER_SIZE);

            client.put(key.duplicate(), utf8("A")).get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            final Version atA = version(client, key);

            cluster.transport(firstCoordinator).holdOutbound();

            // Same shape as above, except the swap carries the ballot it expects to find.
            final CasResult toB = client
                    .cas(key.duplicate(), atA, utf8("B"))
                    .get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertTrue(toB.swapped(), "Fenced CAS(@A -> B) should succeed via failover");
            assertEquals("B", read(client, key));

            final CasResult backToA = client
                    .cas(key.duplicate(), toB.version(), utf8("A"))
                    .get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertTrue(backToA.swapped(), "Fenced CAS(@B -> A) should succeed");
            assertEquals("A", read(client, key));

            final int released = cluster.transport(firstCoordinator).releaseOutbound();
            assertTrue(released > 0, "Nothing was parked: wrong coordinator held");

            // The abandoned round re-reads the register and finds A -- the same value it expected.
            // Its ballot, however, was overtaken twice over, so the fence rejects it before
            // anything is proposed.
            assertEquals("A", awaitDivergence(client, key),
                    "A version-fenced round must not re-apply across an intervening write");
        }
    }

    private static Version version(final DisCasClient client, final ByteBuffer key)
            throws Exception {
        return client.get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                .get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .version();
    }

    /**
     * The peer the client sends attempt 0 to. Mirrors {@code DisCasClient.peerIndex}:
     * {@code distributionHash(key) mod clusterSize}, over a peer list that TestCluster fills
     * with node ids 1..N in order.
     */
    private static int coordinatorFor(final ByteBuffer key, final int clusterSize) {
        final int index = Integer.remainderUnsigned(
                KeyHash.distributionHash(key.duplicate()), clusterSize);
        return index + 1;
    }

    private static String read(final DisCasClient client, final ByteBuffer key) throws Exception {
        return string(client.get(key.duplicate())
                .get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).value());
    }

    /**
     * Poll until the register stops holding A, or {@link #SETTLE} elapses. Returning early on
     * divergence keeps a failing run fast; a passing run costs the full budget, which is the
     * right trade for a test whose whole purpose is to prove an absence.
     */
    private static String awaitDivergence(final DisCasClient client, final ByteBuffer key)
            throws Exception {
        final long deadline = System.nanoTime() + SETTLE.toNanos();
        String last = read(client, key);
        while (System.nanoTime() < deadline) {
            if (!"A".equals(last)) {
                return last;
            }
            Thread.sleep(100L);
            last = read(client, key);
        }
        return last;
    }
}
