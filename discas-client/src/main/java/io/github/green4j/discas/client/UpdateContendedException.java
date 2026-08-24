/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/**
 * A {@link DisCasClient#update} ran out of retry budget while it was still losing the compare.
 * <p>
 * Kept distinct from {@link RequestFailedException} because the two say opposite things about the
 * cluster: that one means nobody answered, this one means everybody did and somebody else kept
 * winning. The key is hot, not unreachable, so backing off and trying again is the useful
 * response -- where a timeout would call for failing over or giving up.
 * <p>
 * Nothing was written. An update that runs out mid-loop has always just lost a compare, so the
 * last thing it did was read, not write.
 */
public final class UpdateContendedException extends DisCasClientException {

    private static final long serialVersionUID = 1L;

    private final int attempts;
    // Version is a value type in the client API, not a Serializable one. This exception carries it
    // for the caller that catches it, and is not something anybody serialises.
    @SuppressWarnings("serial")
    private final Version lastObserved;

    UpdateContendedException(final int attempts, final Version lastObserved) {
        super("update lost the compare " + attempts + " time(s) within its retry budget");
        this.attempts = attempts;
        this.lastObserved = lastObserved == null ? Version.INITIAL : lastObserved;
    }

    /** How many read-transform-write attempts were made, all of them losing. */
    public int attempts() {
        return attempts;
    }

    /** The version that won the last compare -- how far the key had moved on by the end. */
    public Version lastObserved() {
        return lastObserved;
    }

    @Override
    public boolean isTransient() {
        return true;
    }
}
