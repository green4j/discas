/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/**
 * How many nodes a scan must hear from before it will return a result.
 * <p>
 * Purely a client-side merge policy -- nodes answer a scan identically either way, and nothing
 * about this travels on the wire. It decides only what the client does with the answers it got.
 * <p>
 * Distinct from {@code ReadConsistency}, which is about <em>freshness</em> of a single key. This is
 * about <em>coverage</em> of the key set.
 */
public enum ScanCoverage {

    /**
     * Require a majority of the cluster to answer; fail otherwise. The default, because it is the
     * only setting under which the result carries a guarantee: a committed key was accepted by
     * some majority, any majority intersects it, so a majority of answers is certain to include
     * it. Below that, a key committed on the nodes that did not answer can be missing from every
     * node that did.
     */
    QUORUM,

    /**
     * Return whatever answered, even below a majority. The result may omit committed keys, so it
     * carries no completeness guarantee -- {@link ScanPage#quorumReached()} and the
     * responded/cluster counts say how far it can be trusted.
     * <p>
     * For cases where an incomplete listing still beats none: an operator inspecting a cluster
     * that has lost quorum, or a client configured with a subset of the membership
     * (the agent's {@code --nodes-file} bootstrap). Still fails if <em>no</em> node answers -- a
     * scan that reached nobody is not a partial result, it is no result.
     */
    ANY_AVAILABLE
}
