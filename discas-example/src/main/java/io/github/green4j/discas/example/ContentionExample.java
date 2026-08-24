/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.common.client.ReadConsistency;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two clients incrementing the same key at the same time, on a realistic topology (TCP peers, one
 * file WAL per node). What it demonstrates is what a losing compare is <em>for</em>.
 * <p>
 * A lost compare is a result, not a failure. It arrives with the version and value that won, so the
 * loser recomputes from those and re-attempts -- it never re-sends a version it already knows is
 * stale, and it never has to read again to find out what happened. Run it and the final counter is
 * exactly the number of increments both clients performed: no lost update, and no coordination
 * between the two beyond the register itself.
 * <p>
 * A value-compared CAS, which this store does not offer, could not do the same: two clients
 * writing the same <em>value</em> cannot tell their own write from the other's, so
 * "swapped == false" and "swapped == true, then overwritten with identical bytes" are the same
 * observation. Fencing on the version removes the ambiguity by construction.
 * <p>
 * <b>Why this does not simply call {@code update}.</b> It would, in application code:
 * {@code client.update(key, v -> v + 1)} is this loop, built in. The loop stays open here because
 * of its last branch -- the one that re-sends the fenced CAS after an <em>unknown</em> outcome.
 * That is safe only because a duplicate carries an overtaken ballot, and it is precisely what
 * {@code update} refuses to do on the caller's behalf: re-running a read-transform-write after a
 * lost response would increment twice. Written out, the difference between the two is visible.
 */
public final class ContentionExample {

    private static final long CALL_MS = 10_000L;
    private static final int INCREMENTS_PER_CLIENT = 25;

    private ContentionExample() {
    }

    public static void main(final String[] args) throws Exception {
        final Path baseDir = Files.createTempDirectory("discas-contention-");
        try (ExampleCluster.RunningCluster cluster =
                     ExampleCluster.startTcpPeersWithFileWalCluster(2, baseDir)) {

            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);
            final ByteBuffer key = ExampleBytes.encode("counter");

            System.out.println("=== Contention: 2 clients, TCP peers + file WAL ===");
            ExampleOutcome.of(clientA.put(key.duplicate(), ExampleBytes.encode("0")), CALL_MS)
                    .require("seed counter=0");

            final AtomicInteger lostComparesA = new AtomicInteger();
            final AtomicInteger lostComparesB = new AtomicInteger();

            final CompletableFuture<Integer> a = CompletableFuture.supplyAsync(
                    () -> increment(clientA, key, "A", lostComparesA));
            final CompletableFuture<Integer> b = CompletableFuture.supplyAsync(
                    () -> increment(clientB, key, "B", lostComparesB));

            final int appliedA = a.join();
            final int appliedB = b.join();

            final String finalValue = ExampleBytes.decode(ExampleOutcome
                    .of(clientA.get(key.duplicate()), CALL_MS)
                    .require("final read").value());

            System.out.println("clientA applied " + appliedA + " increments, lost "
                    + lostComparesA.get() + " compares");
            System.out.println("clientB applied " + appliedB + " increments, lost "
                    + lostComparesB.get() + " compares");
            System.out.println("counter = " + finalValue
                    + " (expected " + (appliedA + appliedB) + ")");

            if (Integer.parseInt(finalValue) != appliedA + appliedB) {
                // An example that only prints is not evidence. This is the claim being made.
                throw new AssertionError("Lost update: counter is " + finalValue
                        + " but " + (appliedA + appliedB) + " increments were applied");
            }
            System.out.println("no lost update: every applied increment is in the register");
        } finally {
            ExampleFiles.deleteRecursively(baseDir);
        }
    }

    /**
     * Read, add one, write fenced on the version read. On a lost compare, recompute from the
     * version that won rather than retrying the same attempt -- and on an unknown outcome, simply
     * re-send: the fence makes a duplicate a provable no-op.
     * <p>
     * The first two branches are what {@link DisCasClient#update} does internally. The third one
     * is not, and cannot be: see the class javadoc.
     */
    private static int increment(final DisCasClient client, final ByteBuffer key,
                                 final String who, final AtomicInteger lostCompares) {
        int applied = 0;
        for (int i = 0; i < INCREMENTS_PER_CLIENT; i++) {
            while (true) {
                final ExampleOutcome<GetResult> read = ExampleOutcome.of(
                        client.get(key.duplicate(), ReadConsistency.LINEARIZABLE), CALL_MS);
                if (!read.applied()) {
                    // A failed read changes nothing, so it is always safe to simply read again.
                    System.out.println("  " + who + " " + read.describe("read") + "; retrying");
                    continue;
                }
                final GetResult current = read.value();
                final Version expected = current.version();
                final int next = Integer.parseInt(ExampleBytes.decode(current.value())) + 1;

                final ExampleOutcome<CasResult> swap = ExampleOutcome.of(
                        client.cas(key.duplicate(), expected,
                                ExampleBytes.encode(Integer.toString(next))), CALL_MS);

                if (swap.applied()) {
                    if (swap.value().swapped()) {
                        applied++;
                        break;
                    }
                    // The other client got there first. Its version came back with the refusal, so
                    // nothing has to be read again to make progress.
                    lostCompares.incrementAndGet();
                    continue;
                }
                if (swap.refused()) {
                    // Determinate: nothing was written. Start the cycle over.
                    System.out.println("  " + who + " " + swap.describe("cas") + "; recomputing");
                    continue;
                }
                // Unknown -- and harmless here, which is the point of the fence. Re-sending the
                // same fenced write either applies once or is rejected as stale; it cannot apply
                // twice, so the loop is safe without any deduplication of our own.
                System.out.println("  " + who + " " + swap.describe("cas") + "; re-sending (fenced)");
            }
        }
        return applied;
    }
}
