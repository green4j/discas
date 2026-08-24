/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import java.time.Duration;

/**
 * Controls how {@link CertRotationManager} watches the current leaf for expiry. discas
 * never renews certificates itself -- an external issuer rewrites the key/trust files and
 * the source hot-swaps them -- so this policy only governs the near-expiry warning:
 * <ul>
 *   <li>{@code checkInterval} -- how often the manager re-checks the leaf. (Renewed
 *       material is picked up independently by the source's own file watch and pushed to
 *       the manager; this interval does not gate that.)</li>
 *   <li>{@code warnBeforeExpiry} -- how long before the leaf's {@code notAfter} the manager
 *       starts warning when no renewed material has arrived. It is <b>clamped to at least
 *       the cert half-life</b>, so the default is sane for any lifetime: a short (~24h)
 *       automated cert effectively warns at its half-life, while a long-lived (months-years)
 *       cert warns {@code warnBeforeExpiry} before it expires.</li>
 * </ul>
 * Certificate lifetime is the operator's choice; long-lived leaves are the default expectation.
 */
public final class RenewalPolicy {

    private final Duration checkInterval;
    private final Duration warnBeforeExpiry;

    public RenewalPolicy(final Duration checkInterval, final Duration warnBeforeExpiry) {
        if (checkInterval == null || checkInterval.isZero() || checkInterval.isNegative()) {
            throw new IllegalArgumentException("checkInterval must be positive");
        }
        if (warnBeforeExpiry == null || warnBeforeExpiry.isZero() || warnBeforeExpiry.isNegative()) {
            throw new IllegalArgumentException("warnBeforeExpiry must be positive");
        }
        this.checkInterval = checkInterval;
        this.warnBeforeExpiry = warnBeforeExpiry;
    }

    /** {@code checkInterval = 60s}, {@code warnBeforeExpiry = 7d}. */
    public static RenewalPolicy defaults() {
        return new RenewalPolicy(Duration.ofSeconds(60), Duration.ofDays(7));
    }

    public Duration checkInterval() {
        return checkInterval;
    }

    public Duration warnBeforeExpiry() {
        return warnBeforeExpiry;
    }
}
