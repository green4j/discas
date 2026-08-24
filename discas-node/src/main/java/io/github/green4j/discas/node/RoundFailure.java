/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

/**
 * Why a consensus round failed, and therefore whether another attempt can help.
 * <p>
 * The distinction is load-bearing. A round rejected by a higher ballot lost a duel, and re-running
 * it above the ballot it saw is exactly the resolution. A round that timed out without collecting a
 * quorum of promises did not lose anything -- it never heard from enough acceptors -- and re-asking
 * the same unreachable peers only multiplies the latency of a failure that is already decided. Told
 * apart, the second can hand the caller straight to another coordinator instead of spending the
 * whole retry budget first.
 * <p>
 * Branch on this, never on the human-readable reason text that travels beside it.
 */
public enum RoundFailure {

    /**
     * A higher ballot was promised elsewhere -- a duel between proposers. Retrying past the ballot
     * we saw is the resolution, so this backs off and tries again.
     */
    BALLOT_NACK(true),

    /**
     * Prepare timed out without a quorum of promises: too few acceptors answered at all. This node
     * cannot see a majority right now, and no number of retries changes that. Fail immediately so
     * the caller can go somewhere that can.
     */
    INSUFFICIENT_RESPONDERS(false),

    /**
     * Prepare reached quorum but Accept did not complete in time. Deliberately <em>not</em>
     * {@link #INSUFFICIENT_RESPONDERS}: those acceptors answered moments ago, so this is a stall
     * rather than proven unreachability and another attempt is worth making.
     */
    ACCEPT_TIMEOUT(true),

    /**
     * The operation outlived its {@code proposalExpiry} and the coordinator gave up driving it
     * before proposing anything. <b>Nothing was accepted</b>: the check sits between prepare and
     * the Accept broadcast, so this is determinate in the same way {@link #BALLOT_NACK} is.
     * <p>
     * This is what puts an end to "may still be applied". Without it the only bound on how long an
     * abandoned proposal can keep trying to commit is the retry chain, whose length is
     * {@code roundTimeout x (maxRoundRetries + 1)} plus backoff -- a number no caller is told and
     * every attempt resets. With it, an indeterminate answer carries a stated horizon: the write
     * may still apply within {@code proposalExpiry} of its start, and never afterwards.
     * <p>
     * Not retryable, because the budget being retried against is the thing that ran out.
     */
    PROPOSAL_EXPIRED(false);

    private final boolean retryable;

    RoundFailure(final boolean retryable) {
        this.retryable = retryable;
    }

    /** Whether re-running the round has any prospect of a different outcome. */
    public boolean retryable() {
        return retryable;
    }
}
