/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The invariant <b>a member's storage may never come back holding less than it left with</b>, as
 * the only form of it a machine can check.
 *
 * <p>The two halves are equally load-bearing and pull in opposite directions, which is why they are
 * tested against each other rather than one at a time: a <b>lower ceiling under the same
 * incarnation</b> must be caught, and a <b>lower ceiling under a new incarnation</b> must not be.
 * The second is a member whose disk was legitimately replaced; refusing it would keep the one node
 * that needs a floor from the cluster from ever asking for one.
 */
@DisplayName("Promise ceiling history -- has this storage come back with less than it left with?")
class PromiseCeilingHistoryTest {

    private static final NodeId PEER = NodeId.of("n2");
    private static final NodeId OTHER = NodeId.of("n3");
    private static final IncarnationId RUN_ONE = IncarnationId.generate();
    private static final IncarnationId RUN_TWO = IncarnationId.generate();

    private final PromiseCeilingHistory history = new PromiseCeilingHistory();

    private long check(final NodeId peer, final IncarnationId incarnation, final long ceiling) {
        final long rolledBackFrom = history.rolledBackFrom(peer, incarnation, ceiling);
        if (rolledBackFrom == PromiseCeilingHistory.NO_ROLLBACK) {
            history.record(peer, incarnation, ceiling);
        }
        return rolledBackFrom;
    }

    @Test
    @DisplayName("A member nobody has seen before is admitted, whatever it claims")
    void firstSightingIsAlwaysAdmissible() {
        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(PEER, RUN_ONE, 0L));
        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(OTHER, RUN_ONE, 900L),
                "There is nothing to compare a first claim against, and inventing one would refuse "
                        + "every member of a cluster that has just started");
    }

    @Test
    @DisplayName("A ceiling that rises, or repeats, is the ordinary case")
    void risingIsAdmissible() {
        check(PEER, RUN_ONE, 100L);

        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(PEER, RUN_ONE, 100L),
                "A reconnect from a member that has promised nothing since claims the same ceiling");
        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(PEER, RUN_ONE, 140L));
    }

    @Test
    @DisplayName("The same storage claiming less than it proved is the invariant broken")
    void aLowerCeilingUnderTheSameIncarnationIsARollback() {
        check(PEER, RUN_ONE, 140L);

        assertEquals(140L, history.rolledBackFrom(PEER, RUN_ONE, 139L),
                "And the diagnosis is the ceiling it had proved, which is what an operator needs");
    }

    @Test
    @DisplayName("The highest ever seen is what a claim is measured against, not the last one")
    void theBoundIsTheHighestNotTheLatest() {
        check(PEER, RUN_ONE, 500L);
        check(PEER, RUN_ONE, 500L);

        // Nothing lowers the bound within a run: a member that reconnects repeatedly at the same
        // ceiling has not un-promised anything, so the bound stays where the highest claim put it.
        assertEquals(500L, history.rolledBackFrom(PEER, RUN_ONE, 499L));
    }

    @Test
    @DisplayName("A replaced disk claiming less is NOT a rollback -- the case that must not be caught")
    void aNewIncarnationStartsOver() {
        check(PEER, RUN_ONE, 900L);

        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(PEER, RUN_TWO, 0L),
                "A wiped member has no promises to have lost; it takes its floor from the cluster "
                        + "before it serves anything, and refusing it would stop it asking");

        // And the old run's bound is gone with the disk it described, rather than lingering to
        // refuse the new one as it climbs back up.
        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(PEER, RUN_TWO, 5L));
    }

    @Test
    @DisplayName("Members are judged separately")
    void oneMemberIsNotEvidenceAboutAnother() {
        check(PEER, RUN_ONE, 900L);

        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(OTHER, RUN_ONE, 3L));
    }

    @Test
    @DisplayName("A node that cannot state a ceiling is neither refused here nor remembered")
    void nonClaimsAreNotEvidence() {
        for (final long nonClaim : new long[] {
                PeerTransport.PROMISE_CEILING_REPLAYING, PeerTransport.PROMISE_CEILING_UNPROVEN}) {
            assertEquals(PromiseCeilingHistory.NO_ROLLBACK,
                    history.rolledBackFrom(PEER, RUN_ONE, nonClaim),
                    "Nothing was claimed, so nothing is contradicted");
            history.record(PEER, RUN_ONE, nonClaim);
        }
        // Recording a non-claim must not have created a bound out of a sentinel -- which, being
        // negative, would otherwise sit below every real ceiling and admit everything forever, or
        // above nothing and refuse it.
        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, check(PEER, RUN_ONE, 7L));
        assertEquals(7L, history.rolledBackFrom(PEER, RUN_ONE, 6L),
                "The first real claim is what starts the bound");
    }

    @Test
    @DisplayName("A member dropped from the config is forgotten, so its return is a first sighting")
    void forgettingAMemberClearsItsBound() {
        check(PEER, RUN_ONE, 900L);

        history.forget(PEER);

        assertEquals(PromiseCeilingHistory.NO_ROLLBACK, history.rolledBackFrom(PEER, RUN_ONE, 1L));
    }
}
