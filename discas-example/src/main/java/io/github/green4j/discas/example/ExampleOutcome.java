/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.ClientLifecycleException;
import io.github.green4j.discas.client.DisCasOperationException;
import io.github.green4j.discas.client.RequestFailedException;
import io.github.green4j.discas.common.client.ClientErrorCode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The whole failure surface of a client call, reduced to the one question a caller has to branch
 * on: <b>is the outcome known, and if so, did it happen?</b>
 * <p>
 * Every example goes through this rather than calling {@code future.get()} and hoping. Calling
 * {@code get()} directly is not shorter code, it is the same code with the failure branches deleted
 * -- and the branches are the part worth showing. Three outcomes, exhaustively:
 *
 * <ul>
 *   <li>{@link #applied()} -- committed by a quorum and durable.</li>
 *   <li>{@link #refused()} -- <b>determinate:</b> it did not happen. {@link #code()} says why, and
 *       the operation may be re-issued or abandoned on that basis.</li>
 *   <li>{@link #unknown()} -- the outcome is not known. Only {@link ClientErrorCode#UNAVAILABLE}
 *       and a client-side deadline land here. A version-fenced write may simply be re-sent; an
 *       unfenced one needs the author-marker pattern (see {@code CoordinatorFailoverExample}).</li>
 * </ul>
 *
 * <p>A new {@link ClientErrorCode} cannot be silently mis-sorted: {@link #determinate} switches on
 * the enum exhaustively and names each case.
 */
final class ExampleOutcome<T> {

    private final T value;
    private final ClientErrorCode code;
    private final boolean determinate;
    private final Throwable failure;

    private ExampleOutcome(final T value, final ClientErrorCode code,
                           final boolean determinate, final Throwable failure) {
        this.value = value;
        this.code = code;
        this.determinate = determinate;
        this.failure = failure;
    }

    /** Await {@code future} and classify however it ends -- normally, refused, or unresolved. */
    static <T> ExampleOutcome<T> of(final CompletableFuture<T> future, final long timeoutMs) {
        try {
            return new ExampleOutcome<>(future.get(timeoutMs, TimeUnit.MILLISECONDS),
                    ClientErrorCode.NONE, true, null);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            // We stopped waiting; nothing about the cluster is known.
            return new ExampleOutcome<>(null, ClientErrorCode.UNAVAILABLE, false, e);
        } catch (final TimeoutException e) {
            // Our own patience ran out before the client's deadline. Says nothing either way.
            return new ExampleOutcome<>(null, ClientErrorCode.UNAVAILABLE, false, e);
        } catch (final ExecutionException e) {
            return classify(e.getCause() == null ? e : e.getCause());
        }
    }

    private static <T> ExampleOutcome<T> classify(final Throwable cause) {
        if (cause instanceof DisCasOperationException) {
            final ClientErrorCode code = ((DisCasOperationException) cause).code();
            return new ExampleOutcome<>(null, code, determinate(code), cause);
        }
        if (cause instanceof RequestFailedException) {
            final RequestFailedException.Cause why = ((RequestFailedException) cause).cause();
            // ALL_PEERS_EXHAUSTED means no request ever left the socket, which is determinate.
            // TIMED_OUT and SCAN_NO_QUORUM mean we stopped waiting on something that may still
            // be in flight, which is not.
            final boolean known = why == RequestFailedException.Cause.ALL_PEERS_EXHAUSTED;
            return new ExampleOutcome<>(null, ClientErrorCode.UNAVAILABLE, known, cause);
        }
        if (cause instanceof ClientLifecycleException) {
            // Our own client is closing. A request already dispatched may still land.
            return new ExampleOutcome<>(null, ClientErrorCode.UNAVAILABLE, false, cause);
        }
        // Anything unrecognised is treated as unknown rather than as "did not happen": guessing
        // "did not happen" is the guess that loses data.
        return new ExampleOutcome<>(null, ClientErrorCode.INTERNAL, false, cause);
    }

    /**
     * Whether a failure carrying {@code code} proves the operation did not happen. Enumerated
     * rather than defaulted, so adding a code forces a decision here instead of inheriting one.
     */
    private static boolean determinate(final ClientErrorCode code) {
        switch (code) {
            case NONE:
            case ACCESS_DENIED:       // the request is refused, and identically everywhere
            case INVALID_ARGUMENT:    // malformed; never reached a round
            case NOT_READY:           // this node is replaying its log; nothing was proposed
            case NO_QUORUM_AT_COORDINATOR: // refused before a round started
            case BALLOT_LOST:         // lost the duel at prepare; Accept never happened
            case PROPOSAL_EXPIRED:    // abandoned before the Accept broadcast
                return true;
            case UNAVAILABLE:         // the one genuinely indeterminate code
            case INTERNAL:            // an exception on the node side, at an unknown phase
            default:
                return false;
        }
    }

    boolean applied() {
        return failure == null;
    }

    /** Determinate failure: it did not happen. */
    boolean refused() {
        return failure != null && determinate;
    }

    /** The outcome is not known -- the case an unfenced write cannot resolve by re-reading. */
    boolean unknown() {
        return failure != null && !determinate;
    }

    T value() {
        return value;
    }

    private ClientErrorCode code() {
        return code;
    }

    /** The value, or a failure if there is none -- for a step whose whole point is to succeed. */
    T require(final String what) {
        if (!applied()) {
            throw new IllegalStateException(describe(what), failure);
        }
        return value;
    }

    /** One line naming the outcome and, when it matters, what the caller may conclude. */
    String describe(final String what) {
        if (applied()) {
            return what + ": applied";
        }
        if (refused()) {
            return what + ": did not happen (" + code + ") -- safe to re-issue or abandon";
        }
        return what + ": OUTCOME UNKNOWN (" + code + ") -- may still apply; "
                + rootMessage();
    }

    private String rootMessage() {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
