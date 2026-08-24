/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.common.client.ReadConsistency;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Read-then-fenced-write, for tests whose subject is something else entirely (TLS, dual ports,
 * recovery) but which need "swap this value for that one" as a step.
 * <p>
 * There is one compare-and-set and it fences on the version, so a test that wants a
 * <em>value</em> precondition has to spell it out: read, check the value is what it expected, then
 * write fenced on the version that read observed. That is three operations rather than one, which
 * is the honest cost of a value precondition.
 * <p>
 * Not production code and not a pattern to copy: real callers keep the version they read rather
 * than re-deriving a precondition from bytes.
 */
public final class TestCas {

    private TestCas() {
    }

    /**
     * Swap {@code desired} in if the key currently holds {@code expected}.
     *
     * @return the fenced CAS's result, or a not-swapped result carrying what was actually there
     *         when the value precondition did not hold
     */
    public static CasResult swapValue(final DisCasClient client,
                                                   final ByteBuffer key,
                                                   final ByteBuffer expected,
                                                   final ByteBuffer desired,
                                                   final long timeoutMs) throws Exception {
        final GetResult current = client
                .get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        if (!sameBytes(current.value(), expected)) {
            return new CasResult(false, current.value(), current.version());
        }
        return client.cas(key.duplicate(), current.version(), desired)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private static boolean sameBytes(final ByteBuffer a, final ByteBuffer b) {
        if (a == null || b == null) {
            return a == b;
        }
        return a.duplicate().equals(b.duplicate());
    }
}
