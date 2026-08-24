/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.operator;

/**
 * Who an {@link OperatorState} belongs to, and what class of action it asks for.
 * <p>
 * <b>Not a subsystem name.</b> Metric names are already grouped by the code that produced them,
 * which tells an operator which part of the source to read and not which part of the building to
 * walk to. This groups by the answer instead: every state in one group is fixed by the same kind of
 * intervention, so a page that arrives at three in the morning starts with a category of action
 * rather than a component.
 */
public enum OperatorGroup {

    /** This node's durable state, or another member's: a disk, a directory, a wipe-and-rejoin. */
    STORAGE,

    /** Who is in the cluster and on what terms: the member list, {@code N}, identities, versions. */
    MEMBERSHIP,

    /** Another member's reachability: bring it back, or decide to run short. */
    PEER,

    /** This process's configuration and the material it loads: a file, a certificate, NTP. */
    CONFIG,

    /**
     * The workload against what the process is configured to sustain: a budget or a tuning
     * decision rather than a fault. A state belongs here only when there is a configured number to
     * measure against, never an invented threshold.
     */
    CAPACITY,

    /**
     * The software. An operator cannot fix a defect, but escalating one is an action, and an
     * exception the event loop caught and nobody was told about is the cheapest way to lose it.
     */
    INTERNAL
}
