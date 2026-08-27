/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.common.transport.TransportException;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientException;
import io.github.green4j.discas.client.DisCasOperationException;
import io.github.green4j.discas.common.client.ClientErrorCode;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ChaosAssertions {
    /**
     * Universal safety wall for cluster-wide convergence. A healthy cluster
     * reaches quiescence in milliseconds; this cap only fires if divergence
     * is permanent, in which case the diagnostics dump identifies the culprit.
     */
    static final Duration QUIESCENCE_WALL = Duration.ofSeconds(60);

    /** Single-attempt wall for a solitary awaitGet read. */
    private static final int AWAIT_GET_ATTEMPT_SECONDS = 25;

    /** Fixed retry budget for awaitGet (no wall-clock deadline arithmetic). */
    private static final int AWAIT_GET_RETRIES = 5;
    private static final long AWAIT_GET_BACKOFF_MS = 100L;

    private ChaosAssertions() {
    }

    /**
     * Reads every key from every client until the cluster reaches a fixed
     * point where all clients agree. Returns the agreed key->value map.
     */
    static Map<String, String> snapshotState(final ChaosClusterHarness harness,
                                             final List<String> allKeys) throws Exception {
        return harness.awaitClusterQuiescent(allKeys, QUIESCENCE_WALL);
    }

    static void assertConvergedAcrossClients(final ChaosClusterHarness harness,
                                             final List<String> allKeys) throws Exception {
        harness.awaitClusterQuiescent(allKeys, QUIESCENCE_WALL);
    }

    static void assertCounterExactness(final ChaosClusterHarness harness,
                                       final Map<String, AtomicLong> successfulSwaps,
                                       final long tolerance) throws Exception {
        final List<String> counterKeys = successfulSwaps.keySet().stream()
                .collect(Collectors.toList());
        final Map<String, String> quiesced = harness.awaitClusterQuiescent(counterKeys, QUIESCENCE_WALL);
        for (final Map.Entry<String, AtomicLong> entry : successfulSwaps.entrySet()) {
            final long confirmedSwaps = entry.getValue().get();
            final String observedStr = quiesced.get(entry.getKey());
            final long observed = observedStr == null ? 0L : Long.parseLong(observedStr);
            assertTrue(observed + tolerance >= confirmedSwaps,
                    "Counter went backwards for key=" + entry.getKey()
                            + ", final=" + observed + ", confirmedSwaps=" + confirmedSwaps
                            + ", tolerance=" + tolerance);
        }
    }

    static void assertDurabilityAfterFullRestart(final ChaosClusterHarness harness,
                                                 final List<String> allKeys,
                                                 final Duration outage) throws Exception {
        final Map<String, String> before = harness.awaitClusterQuiescent(allKeys, QUIESCENCE_WALL);
        harness.restartAllSequentially(outage);
        final Map<String, String> after = harness.awaitClusterQuiescent(allKeys, QUIESCENCE_WALL);
        assertEquals(before, after, "State changed after sequential full restart");
    }

    static void assertOnlyTransientWorkloadErrors(final List<Throwable> errors) {
        final List<Throwable> fatal = errors.stream()
                .filter(t -> !isTransientChaosError(t))
                .collect(Collectors.toList());
        assertTrue(fatal.isEmpty(),
                "Fatal workload errors during chaos: "
                        + fatal.stream().map(Throwable::toString).collect(Collectors.toList()));
    }

    static void assertNoErrors(final List<Throwable> errors, final String messagePrefix) {
        assertTrue(errors.isEmpty(),
                messagePrefix + " errors: "
                        + errors.stream().map(Throwable::toString).collect(Collectors.toList()));
    }

    /**
     * Reads {@code key} from {@code client} with a fixed retry budget. Each
     * attempt has a 25 s per-attempt safety wall; retries use a 100 ms cheap
     * backoff. There is no wall-clock deadline arithmetic -- this method either
     * returns a value in under a second on a healthy cluster or fails after
     * a bounded number of attempts.
     */
    static ByteBuffer awaitGet(final DisCasClient client, final String key) throws Exception {
        Throwable last = null;
        for (int attempt = 0; attempt < AWAIT_GET_RETRIES; attempt++) {
            try {
                return client.get(ChaosWorkload.buf(key))
                        .get(AWAIT_GET_ATTEMPT_SECONDS, TimeUnit.SECONDS).value();
            } catch (final Throwable t) {
                last = t;
                Thread.sleep(AWAIT_GET_BACKOFF_MS);
            }
        }
        if (last instanceof Exception) {
            throw (Exception) last;
        }
        throw new RuntimeException("awaitGet failed for key=" + key + " after "
                + AWAIT_GET_RETRIES + " attempts", last);
    }

    /**
     * Whether a node's verdict is something the nemesis manufactures on purpose, rather than a
     * defect the chaos run has caught.
     * <p>
     * Every constant is named, and the {@code default} throws rather than answering. A code added
     * to the enum later and not classified here would otherwise be graded silently -- as a defect
     * if the default said {@code false}, turning the chaos suites red for the wrong reason, or as
     * routine if it said {@code true}, hiding a real one. Failing on the code itself says which.
     */
    private static boolean isNemesisProduced(final ClientErrorCode code) {
        switch (code) {
            // Manufactured on purpose by partitions, drops, restarts and contention. All are
            // determinate or retryable, and the workload's own retry loop absorbs them.
            case UNAVAILABLE:               // accept stalled; outcome unknown
            case NOT_READY:                 // node still replaying its log after a restart
            case NO_QUORUM_AT_COORDINATOR:  // this coordinator is on the wrong side of a partition
            case BALLOT_LOST:               // proposers duelling over one key
            case PROPOSAL_EXPIRED:          // the operation outlived its budget under pressure
                return true;
            // Properties of the request or of the node, not of the network. A nemesis cannot
            // produce these, so if one appears the run has found something.
            case ACCESS_DENIED:
            case INVALID_ARGUMENT:
            case INTERNAL:
            case NONE:                      // a failure carrying NONE is itself a defect
            case STORE_FULL:                // the cluster is full: sizing or a leak, not the nemesis
                return false;
            default:
                throw new IllegalArgumentException("Unclassified error code: " + code);
        }
    }

    /**
     * Whether {@code t} is a condition the nemesis is expected to create, rather than a defect.
     * <p>
     * Entirely type-driven: matching substrings of exception messages would make a contended CAS
     * reported as "Prepare rejected" matched none of them -- so a routine chaos outcome was
     * intermittently graded a fatal error. Message text is for humans and may be rephrased at any
     * time; every failure the system raises now carries its case in its type.
     */
    private static boolean isTransientChaosError(final Throwable t) {
        if (t instanceof TimeoutException) {
            return true;
        }
        if ((t instanceof ExecutionException || t instanceof CompletionException)
                && t.getCause() != null) {
            return isTransientChaosError(t.getCause());
        }
        if (t instanceof DisCasOperationException) {
            return isNemesisProduced(((DisCasOperationException) t).code());
        }
        if (t instanceof DisCasClientException) {
            // No verdict was obtained: a timed-out or peer-exhausted attempt, or a shutdown race.
            return ((DisCasClientException) t).isTransient();
        }
        if (t instanceof TransportException) {
            // A send that never made it to the wire: closed, backoff, connect failure or a limit.
            // Only a setup fault (unknown target, failed initialisation) is fatal.
            return ((TransportException) t).isTransient();
        }
        if (t instanceof RejectedExecutionException) {
            // The client's callback executor refusing work: it is shutting down mid-run.
            return true;
        }
        return false;
    }
}
