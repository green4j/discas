/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.NodeId;

import java.util.List;
import java.util.Locale;

/**
 * What one sweep of the tombstone queue found and did -- the whole operator surface of collection,
 * reported once per sweep whether or not there was anything to collect.
 *
 * <p>One report rather than an event per outcome, because the numbers an operator reads are not
 * events: a gauge that only moved when something was collected would freeze at its last value in
 * exactly the cluster that has stopped collecting, which is the one it exists for. A sweep that
 * finds nothing is as much a reading as one that purges a key.
 *
 * <p>A number is in here only if seeing it change tells somebody to do something. An unfinished
 * sweep is not such a number: collection's normal state is not finishing, so reporting every one
 * would page a healthy cluster nightly and teach an operator to ignore the signal that matters.
 */
public final class TombstoneSweep {

    /**
     * A member that stopped a collection, and what it said.
     *
     * <p>The two are different problems for an operator, which is why the answer travels with the
     * member rather than being flattened into a count: {@code null} is silence, a member that could
     * not be reached at all. A {@link PurgeAnswer#RETAINED} from a member that is otherwise healthy
     * is a replica that is merely behind, and needs nothing -- anti-entropy ends it.
     */
    public static final class Blocker {
        private final NodeId peer;
        private final PurgeAnswer answer;

        public Blocker(final NodeId peer, final PurgeAnswer answer) {
            this.peer = peer;
            this.answer = answer;
        }

        public NodeId peer() {
            return peer;
        }

        /** What that member answered, or {@code null} when it did not answer at all. */
        public PurgeAnswer answer() {
            return answer;
        }

        /**
         * How this blocker is named to an operator, in a log line or as a metric label. One
         * spelling, here, so the line and the label cannot disagree.
         */
        public String label() {
            return answer == null ? "silent" : answer.name().toLowerCase(Locale.ROOT);
        }

        @Override
        public String toString() {
            return peer.value() + "=" + label();
        }
    }

    private final HashedBytes key;
    private final int tombstones;
    private final boolean collected;
    private final List<Blocker> blockers;

    public TombstoneSweep(final HashedBytes key,
                          final int tombstones,
                          final boolean collected,
                          final List<Blocker> blockers) {
        this.key = key;
        this.tombstones = tombstones;
        this.collected = collected;
        this.blockers = blockers;
    }

    /** The candidate this sweep took, or {@code null} when nothing had been left alone long enough. */
    public HashedBytes key() {
        return key;
    }

    /** Keys this node holds a tombstone for: key space that cannot shrink until they go. */
    public int tombstones() {
        return tombstones;
    }

    /** Whether this sweep collected its candidate. */
    public boolean collected() {
        return collected;
    }

    /**
     * Who stopped it, empty unless this sweep had a candidate and did not collect it. The action
     * belongs to the members named here, not to collection.
     */
    public List<Blocker> blockers() {
        return blockers;
    }

    /** Whether this sweep had something to collect and did not. */
    public boolean blocked() {
        return key != null && !collected;
    }

    @Override
    public String toString() {
        return "TombstoneSweep[tombstones=" + tombstones
                + ", collected=" + collected + ", blockers=" + blockers + "]";
    }
}
