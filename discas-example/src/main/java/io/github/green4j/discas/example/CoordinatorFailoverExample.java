/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.RequestFailedException;
import io.github.green4j.discas.client.ScanPage;
import io.github.green4j.discas.client.ScanResult;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockAcquireStatus;
import io.github.green4j.discas.client.lock.LockInfoStatus;
import io.github.green4j.discas.client.lock.LockWriteResult;
import io.github.green4j.discas.client.lock.LockWriteStatus;
import io.github.green4j.discas.common.client.ReadConsistency;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * What each class of operation does when the coordinator it was sent to goes away, run against a
 * real 3-node cluster with a node actually killed underneath it.
 * <p>
 * Each scenario <b>asserts</b> its claim about which failures the client hides and which it
 * surfaces, and throws if it does not hold, so the example fails rather than printing something
 * reassuring.
 *
 * <ol>
 *   <li><b>Read-modify-write with versioned CAS</b> -- survives the loss with no lost update, and
 *       the caller never has to know it happened.</li>
 *   <li><b>Replaying a stale version</b> -- the case the fence exists for: it must lose, and the
 *       write that overtook it must survive.</li>
 *   <li><b>An unfenced write whose outcome is unknown</b> -- why re-reading cannot settle it, and
 *       the author-marker pattern that can.</li>
 *   <li><b>tryLock recovery</b> -- you may already hold a lock you were never told about;
 *       {@code getLockInfo} plus {@code release} is the recovery.</li>
 *   <li><b>Resumable scan</b> -- a page is an independent quorum read, so pagination continues
 *       across the loss from the page cursor you already hold.</li>
 * </ol>
 *
 * <p>The cluster is 3 nodes, so quorum is 2 and killing one leaves the cluster writable. That is
 * the point: what breaks is not the cluster's availability but the client's <em>knowledge</em> of
 * what happened to the request in flight.
 */
public final class CoordinatorFailoverExample {

    private static final Duration CALL = Duration.ofSeconds(15);
    private static final ByteBuffer NO_PREFIX = ByteBuffer.allocate(0);

    private CoordinatorFailoverExample() {
    }

    public static void main(final String[] args) throws Exception {
        final Path baseDir = Files.createTempDirectory("discas-failover-example");
        try (ExampleCluster.RunningCluster cluster =
                     ExampleCluster.startTcpPeersWithFileWalCluster(1, baseDir)) {

            final DisCasClient client = cluster.client(0);
            System.out.println("=== Coordinator failover example (3 nodes, quorum 2) ===");

            // Seed everything before the cluster loses a node, so a scenario that needs existing
            // state is not also testing whether a write got in.
            seed(client);

            System.out.println();
            System.out.println("-- killing node 1 of 3; quorum survives, the client's knowledge does not --");
            cluster.nodes.get(0).close();
            // Give the surviving nodes a moment to notice the peer is gone, so the first scenario
            // is not paying a connect timeout that has nothing to teach.
            Thread.sleep(500L);

            readModifyWriteAcrossTheLoss(client);
            staleVersionLosesAndTheWinnerSurvives(client);
            unfencedWriteAndTheAuthorMarker(client);
            lockRecoveryAfterAnUnknownAcquire(client);
            resumableScanAcrossTheLoss(client);

            System.out.println();
            System.out.println("=== every claim in the failover table held ===");
        } finally {
            ExampleFiles.deleteRecursively(baseDir);
        }
    }

    private static void seed(final DisCasClient client) throws Exception {
        put(client, "failover/counter", "0");
        put(client, "failover/fenced", "v1");
        for (int i = 0; i < 12; i++) {
            put(client, String.format("failover/scan/%02d", i), "x");
        }
        System.out.println("seeded failover/counter, failover/fenced, and 12 failover/scan keys");
    }

    /**
     * Scenario 1. Five read-modify-write cycles over a cluster missing a node. Every one must
     * produce a definite answer: the client walks to another coordinator without telling us, and
     * a fenced write is safe to re-send because a duplicate provably cannot apply.
     * <p>
     * The assertion is the counter's final value. A lost update -- two cycles reading the same
     * version and both committing -- would leave it short, and no exception would be thrown
     * anywhere to reveal it.
     */
    private static void readModifyWriteAcrossTheLoss(final DisCasClient client) throws Exception {
        System.out.println();
        System.out.println("[1] read-modify-write with versioned CAS");
        final ByteBuffer key = ExampleBytes.encode("failover/counter");

        for (int cycle = 0; cycle < 5; cycle++) {
            // The loop is the pattern, not a retry: on a lost compare you re-read rather than
            // re-sending a version you already know is stale. Written out here so the failover is
            // visible step by step; client.update(key, v -> v + 1) is the same loop, built in.
            while (true) {
                final GetResult current = client
                        .get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                        .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
                final int next = Integer.parseInt(ExampleBytes.decode(current.value())) + 1;
                final CasResult result = client
                        .cas(key.duplicate(), current.version(), ExampleBytes.encode(Integer.toString(next)))
                        .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
                if (result.swapped()) {
                    break;
                }
                System.out.println("    compare lost to version " + result.version().token()
                        + "; recomputing rather than re-sending");
            }
        }

        final String finalValue = get(client, "failover/counter");
        require("5".equals(finalValue),
                "the counter must be exactly 5 -- a lost update would leave it short, was " + finalValue);
        System.out.println("    5 cycles, counter = " + finalValue + ": no lost update, no surfaced failure");
    }

    /**
     * Scenario 2. The interleaving the whole design exists to prevent, driven deliberately: hold a
     * version, let the key move on, then replay the held version.
     * <p>
     * A byte-compared CAS in the same position can apply a second time whenever the register
     * happens to hold {@code expected} again, silently reverting whoever wrote in between.
     */
    private static void staleVersionLosesAndTheWinnerSurvives(final DisCasClient client) throws Exception {
        System.out.println();
        System.out.println("[2] replaying a stale version");
        final ByteBuffer key = ExampleBytes.encode("failover/fenced");

        final Version stale = client.get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS)
                .version();

        // Somebody else writes twice. The second write returns the register to the value the stale
        // attempt expected -- which is exactly what defeats a byte-compared CAS and not this one.
        requireSwapped(client.cas(key.duplicate(), stale, ExampleBytes.encode("v2"))
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS), "the first write should win");
        final Version afterV2 = client.get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS).version();
        requireSwapped(client.cas(key.duplicate(), afterV2, ExampleBytes.encode("v1"))
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS), "the inverse write should win");

        final CasResult replayed = client
                .cas(key.duplicate(), stale, ExampleBytes.encode("clobbered"))
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS);

        require(!replayed.swapped(),
                "a stale version must lose even though the value it expected is back");
        require("v1".equals(get(client, "failover/fenced")),
                "the intervening write must survive the replay");
        System.out.println("    stale replay rejected, register still v1;"
                + " the version that won is " + replayed.version().token());

        // The fenced delete is the same operation committing a tombstone, and answers the same way.
        final Version live = client.get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS).version();
        require(!client.delete(key.duplicate(), stale)
                        .get(CALL.toMillis(), TimeUnit.MILLISECONDS).swapped(),
                "a stale fenced delete must not tombstone");
        require(client.delete(key.duplicate(), live)
                        .get(CALL.toMillis(), TimeUnit.MILLISECONDS).swapped(),
                "a fenced delete at the live version must apply");
        System.out.println("    fenced delete: stale one refused, live one applied");
    }

    /**
     * Scenario 3. An unfenced write has no fence, so the client will not move it to another
     * coordinator once it has reached the wire: a duplicate {@code put} that lands after somebody
     * else committed reverts them. The caller is told so, with
     * {@link RequestFailedException.Cause#INDETERMINATE}, and is then holding the one outcome that
     * cannot be settled by asking the register -- "not applied" and "applied and since
     * overwritten" read identically.
     * <p>
     * What settles it is putting the writer's identity <em>in the value</em>. The read-back then
     * answers "did mine land?" rather than "is something there?", and that answer is what makes
     * the re-issue below safe: it happens only after the marker has proved the first attempt did
     * not apply.
     * <p>
     * Killing a coordinator is enough to reach this branch, so the loop below really runs rather
     * than merely being described.
     */
    private static void unfencedWriteAndTheAuthorMarker(final DisCasClient client) throws Exception {
        System.out.println();
        System.out.println("[3] an unfenced write, and attributing it afterwards");
        final ByteBuffer key = ExampleBytes.encode("failover/unfenced");
        final String writerId = "writer-a";
        final String payload = writerId + ":payload";

        boolean mine = false;
        // Bounded: each pass either proves our value is in the register or proves it is not, so
        // this is a loop over definite answers rather than a blind retry.
        for (int attempt = 1; attempt <= 3 && !mine; attempt++) {
            try {
                client.put(key.duplicate(), ExampleBytes.encode(payload))
                        .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
                System.out.println("    attempt " + attempt + ": put returned normally");
            } catch (final ExecutionException failed) {
                // The honest branch, and the one that actually runs here: the coordinator took the
                // write and died, so the client refuses to re-send it and says the outcome is open.
                require(failed.getCause() instanceof RequestFailedException
                                && ((RequestFailedException) failed.getCause()).cause()
                                    == RequestFailedException.Cause.INDETERMINATE,
                        "an unfenced write to a dying coordinator must report INDETERMINATE,"
                                + " not a definite-looking failure; got " + rootCause(failed));
                System.out.println("    attempt " + attempt + ": " + rootCause(failed));
            }

            // The question a version cannot answer: is the value that is there now the one I wrote?
            final String observed = get(client, "failover/unfenced");
            mine = observed.startsWith(writerId + ":");
            System.out.println("      read back " + observed + " -> written by me: " + mine);
        }

        require(mine, "the marker must eventually attribute the write to us");
        System.out.println("    resolved by the marker, not by the version: 'absent' and 'applied"
                + " then overwritten' are indistinguishable by version alone");
    }

    /**
     * Scenario 4. {@code tryLock} is the operation whose surfaced failure is worst: a caller that
     * saw an error may nevertheless hold the lock, and a lock nobody knows they hold is held until
     * its lease expires.
     * <p>
     * The recovery is not a retry -- retrying would at best wait out our own lease. It is
     * {@code recoverLock(key, ownerId)}, and it works only because the owner id was ours to choose
     * before the write. Nothing else in the record could serve: the token is handed back in the
     * very response that went missing, and the generation is not knowable in advance.
     */
    private static void lockRecoveryAfterAnUnknownAcquire(final DisCasClient client) throws Exception {
        System.out.println();
        System.out.println("[4] tryLock recovery via recoverLock");
        final ByteBuffer key = ExampleBytes.encode("failover/job-lock");
        final String ownerId = "worker-a";

        try {
            final LockAcquireResult acquired = client
                    .tryLock(key.duplicate(), Duration.ofSeconds(30), ownerId)
                    .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
            System.out.println("    tryLock -> " + acquired.status());
        } catch (final Exception failed) {
            System.out.println("    tryLock failed (" + rootCause(failed) + ")"
                    + " -- we may hold it anyway; asking");
        }

        // Run unconditionally so the example demonstrates the recovery rather than describing it,
        // and deliberately without keeping the token from the acquire above: a caller whose
        // tryLock threw has no token, and that is the case this has to work for.
        final LockAcquireResult recovered = client.recoverLock(key.duplicate(), ownerId)
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
        System.out.println("    recoverLock -> " + recovered.status());

        if (recovered.status() == LockAcquireStatus.ACQUIRED) {
            // A rebuilt lock, not a description of one: releasing through it really does release
            // the lease the lost acquire took.
            final LockWriteResult released = recovered.lock().release()
                    .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
            require(released.status() == LockWriteStatus.APPLIED,
                    "a recovered lock carries the holder's own token, so release must succeed");
            System.out.println("    released the lock we were holding without knowing it");
        }

        require(client.getLockInfo(key.duplicate())
                        .get(CALL.toMillis(), TimeUnit.MILLISECONDS).status() != LockInfoStatus.LOCKED,
                "no lock may be left held after recovery");
        System.out.println("    lock is free: no orphaned lease waiting out its ttl");
    }

    /**
     * Scenario 5. Paging is the one multi-round-trip read, and the cursor is held by the caller.
     * That is what makes it resumable across a coordinator loss with no server-side session to
     * lose -- and also what stops any page from being a snapshot.
     */
    private static void resumableScanAcrossTheLoss(final DisCasClient client) throws Exception {
        System.out.println();
        System.out.println("[5] resumable scan");
        final ByteBuffer prefix = ExampleBytes.encode("failover/scan/");

        final List<String> keys = new ArrayList<>();
        ByteBuffer cursor = null;
        int pages = 0;
        while (true) {
            final ScanPage page = client
                    .scan(prefix.duplicate(), cursor, 5)
                    .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
            pages++;
            for (final ScanResult result : page.results()) {
                keys.add(ExampleBytes.decode(result.key()));
            }
            require(page.quorumReached(),
                    "each page is its own quorum read and must reach one with 2 of 3 nodes alive");
            if (page.complete()) {
                break;
            }
            // The cursor is ours. Nothing on the node remembers this walk, which is exactly why
            // the next page may be served by a different coordinator.
            cursor = page.nextCursor();
        }

        require(keys.size() == 12, "all 12 seeded keys must be enumerated, saw " + keys.size());
        System.out.println("    " + keys.size() + " keys over " + pages
                + " pages, every page a quorum read, cursor held by the caller");
    }

    private static void put(final DisCasClient client, final String key, final String value)
            throws Exception {
        client.put(ExampleBytes.encode(key), ExampleBytes.encode(value))
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static String get(final DisCasClient client, final String key) throws Exception {
        return ExampleBytes.decode(client.get(ExampleBytes.encode(key))
                .get(CALL.toMillis(), TimeUnit.MILLISECONDS).value());
    }

    private static void requireSwapped(final CasResult result,
                                       final String what) {
        require(result.swapped(), what);
    }

    /** An example that only prints is not an assertion; this is what makes the claims checkable. */
    private static void require(final boolean condition, final String what) {
        if (!condition) {
            throw new AssertionError("FAILOVER CLAIM BROKEN: " + what);
        }
    }

    private static String rootCause(final Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
