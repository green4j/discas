/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.lock.Lock;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockWriteStatus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * The claim every lease-based lock has to make and cannot keep on its own: <b>the lease is not the
 * safety mechanism, the fencing token is</b>. This is that sentence as a running program.
 * <p>
 * The scenario is the one no lock can prevent. A holder stalls past its lease -- a long GC, a frozen
 * VM, a partition -- and while it is away a successor legitimately takes the lock. Then the
 * straggler resumes and writes to whatever the lock was guarding, and DisCas never sees that write:
 * it goes straight to the resource.
 * <p>
 * Checking first does not close this. {@link Lock#remainingLease()} does report zero here, so a
 * careful holder could look -- but looking is check-then-act, and the stall that displaced it is
 * just as free to land between the check and the write arriving. There is no lease reading taken
 * before a write that is still true when the write lands.
 * <p>
 * So the resource has to be the one to refuse it, and the only thing that lets it is a number
 * carried <em>by</em> the write. {@link Lock#fencingToken()} is a per-key generation that strictly
 * increases with every acquire, so a resource that remembers the highest it has accepted can reject
 * anything not above it -- locally, with no round trip, and with no window to race in.
 * <p>
 * Both halves are run here against the same sequence of events: {@link GuardedResource} applies that
 * rule, {@link UnguardedResource} does not. The unguarded one loses the successor's work to a writer
 * that had already been displaced, and it loses it silently. That difference is the whole argument
 * for threading the token through to the thing you are protecting -- and for being honest with
 * yourself when the resource cannot take one, because then the lock is an optimisation and not a
 * guarantee.
 */
public final class FencingExample {

    private static final long CALL_MS = 8_000L;
    private static final Duration SHORT_LEASE = Duration.ofMillis(400);
    private static final Duration STALL = Duration.ofMillis(700);

    private FencingExample() {
    }

    public static void main(final String[] args) throws Exception {
        final Path baseDir = Files.createTempDirectory("discas-fencing-");
        try (ExampleCluster.RunningCluster cluster =
                     ExampleCluster.startTcpPeersWithFileWalCluster(2, baseDir)) {

            final DisCasClient straggler = cluster.client(0);
            final DisCasClient successor = cluster.client(1);
            final String key = "invoices/nightly";

            System.out.println("=== Fencing: the lease cannot save you, the token can ===");

            // [1] The holder takes the lock and does some of its work.
            final Lock stale = ExampleOutcome
                    .of(straggler.tryLock(key, SHORT_LEASE, "worker-A"), CALL_MS)
                    .require("worker-A tryLock")
                    .lock();
            System.out.println("[1] worker-A holds " + key + " at generation " + stale.fencingToken());

            final GuardedResource guarded = new GuardedResource();
            final UnguardedResource unguarded = new UnguardedResource();
            require(guarded.write(stale.fencingToken(), "A's first page"),
                    "the holder's own write must be accepted");
            unguarded.write("A's first page");

            // [2] worker-A stalls past its lease. Nothing happens in its process; the only thing
            // that changes is the clock, which is exactly why it cannot notice.
            System.out.println("[2] worker-A stalls for " + STALL.toMillis() + "ms on a "
                    + SHORT_LEASE.toMillis() + "ms lease");
            Thread.sleep(STALL.toMillis());

            final LockAcquireResult taken = ExampleOutcome
                    .of(successor.tryLock(key, Duration.ofSeconds(30), "worker-B"), CALL_MS)
                    .require("worker-B tryLock");
            require(taken.acquired(), "a lapsed lease must be takeable, or nothing here is a lock");
            final Lock live = taken.lock();
            System.out.println("    worker-B took it at generation " + live.fencingToken());
            require(live.fencingToken() > stale.fencingToken(),
                    "every acquire must strictly increase the generation");

            require(guarded.write(live.fencingToken(), "B's work"), "the current holder must be able to write");
            unguarded.write("B's work");

            // [3] worker-A resumes and writes. Its remaining lease does read zero, so a holder that
            // checked would see it -- but a check before a write is only ever a guess about the
            // moment the write lands, and this stall is proof that the gap can be arbitrarily wide.
            System.out.println("[3] worker-A resumes, its lock object unchanged at generation "
                    + stale.fencingToken());
            System.out.println("    remainingLease() reads " + stale.remainingLease().toMillis()
                    + "ms -- a check would catch this one, but not the stall that lands after it");

            final boolean acceptedFromStraggler = guarded.write(stale.fencingToken(), "A's second page");
            unguarded.write("A's second page");
            System.out.println("    guarded resource accepted it: " + acceptedFromStraggler);

            require(!acceptedFromStraggler,
                    "a write fenced at generation " + stale.fencingToken() + " must be refused once "
                            + live.fencingToken() + " has been seen");
            require("B's work".equals(guarded.content()),
                    "the guarded resource must still hold the current holder's work");
            require("A's second page".equals(unguarded.content()),
                    "without the token there is nothing to refuse with -- this is the write that "
                            + "would have overwritten B silently");

            System.out.println("    guarded   -> " + guarded.content() + " (B's work survives)");
            System.out.println("    unguarded -> " + unguarded.content() + " (B's work is gone)");

            // The cluster does fence the lock record itself, which is worth seeing but is not what
            // saved the resource above: that write never went near DisCas.
            final LockWriteStatus stragglerRelease = ExampleOutcome
                    .of(stale.release(), CALL_MS).require("worker-A release").status();
            System.out.println("[4] worker-A's release -> " + stragglerRelease
                    + " (the record is fenced too, but only the record)");
            require(stragglerRelease == LockWriteStatus.HELD_BY_OTHER,
                    "a displaced holder must not be able to release its successor's lock");

            ExampleOutcome.of(live.release(), CALL_MS).require("worker-B release");
            System.out.println("=== the straggler was stopped by the resource, not by the lock ===");
        } finally {
            ExampleFiles.deleteRecursively(baseDir);
        }
    }

    private static void require(final boolean condition, final String claim) {
        if (!condition) {
            throw new AssertionError(claim);
        }
    }

    /**
     * What the lock is protecting, doing the one thing that makes the protection real: remember the
     * highest fencing token accepted and refuse anything that does not exceed it.
     * <p>
     * Note what is <em>not</em> here. It never asks DisCas whether the writer still holds the lock:
     * that answer would be stale before it arrived, and asking it would make the resource depend on
     * the cluster being reachable. A monotonic number needs neither.
     */
    private static final class GuardedResource {
        private long highestSeen;
        private String content = "";

        synchronized boolean write(final long fencingToken, final String payload) {
            if (fencingToken <= highestSeen) {
                return false;
            }
            highestSeen = fencingToken;
            content = payload;
            return true;
        }

        synchronized String content() {
            return content;
        }
    }

    /** The same resource with the rule taken out -- last writer wins, whoever they turned out to be. */
    private static final class UnguardedResource {
        private String content = "";

        synchronized void write(final String payload) {
            content = payload;
        }

        synchronized String content() {
            return content;
        }
    }
}
