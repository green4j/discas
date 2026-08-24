/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;

import java.time.Duration;
import java.util.List;

/**
 * Picks the tombstone to collect next, one at a time, and paces how often that happens.
 *
 * <p>One number is the whole policy, and it is also the ceiling on how fast this node can collect:
 * <b>one candidate per {@code sweepInterval}</b>. Each node sweeps the keys it owns first
 * ({@link SweepAffinity}), so those ceilings add up rather than overlapping: the cluster collects
 * {@code N} keys per interval. A workload deleting faster than that grows the key space with no
 * fault anywhere, which is a capacity decision rather than a bug.
 *
 * <p><b>No minimum age.</b> A tombstone written a moment ago is as valid a candidate as one from
 * last week: what makes collection safe is that every member permits it, not how long anyone
 * waited. Sweeping a delete that has not reached everyone costs one round trip and is refused, and
 * the member that refuses is genuinely behind, which is what the blocked report is for. A minimum
 * age would need a clock that does not survive a restart, after which every tombstone would wait
 * again while the operator surface called a cluster that had stopped collecting healthy.
 *
 * <p>The next sweep is scheduled when the previous one has been decided, never on a fixed repeat.
 * That is what keeps a blocked cluster from turning into a retry storm: a member that is down blocks
 * every sweep, and each blocked sweep still costs its own deadline before the next one is even
 * armed.
 *
 * <p><b>Cheap when there is nothing to collect</b>, which is the ordinary case -- most stores hold no
 * tombstones at all. A sweep that finds nothing is one lookup ({@link LocalStore#tombstoneCandidate})
 * and no messages.
 *
 * <p>Nothing here decides anything: {@link TombstoneCollector} owns the decision, and this class
 * knows nothing about the answers it collects.
 */
final class TombstoneSweeper {

    private final LocalStore store;
    private final SweepAffinity affinity;
    private final TombstoneCollector collector;
    private final EventLoop loop;
    private final NodeObserver observer;
    private final Duration sweepInterval;

    private EventLoop.TimerHandle timer;
    private boolean stopped;

    /**
     * @param sweepInterval how long after one sweep is decided the next one starts. One candidate
     *                      per interval is this node's collection rate.
     */
    TombstoneSweeper(final LocalStore store,
                            final SweepAffinity affinity,
                            final TombstoneCollector collector,
                            final EventLoop loop,
                            final NodeObserver observer,
                            final Duration sweepInterval) {
        this.store = store;
        this.affinity = affinity;
        this.collector = collector;
        this.loop = loop;
        this.observer = observer == null ? NodeObserver.NONE : observer;
        this.sweepInterval = sweepInterval;
    }

    /**
     * Begin sweeping, one interval from now rather than at once: a node that has just started is a
     * node whose peers may not have finished handshaking, and a sweep that asks a member it cannot
     * reach yet is one that can only be refused.
     */
    public void start() {
        scheduleNext();
    }

    public void close() {
        stopped = true;
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    /**
     * The one place collection is reported. The counts are read here rather than by whoever renders
     * them, because the store is confined to this loop and a scrape arrives on another thread.
     */
    private void report(final HashedBytes key, final boolean collected,
                        final List<TombstoneSweep.Blocker> blockers) {
        observer.tombstoneSwept(new TombstoneSweep(
                key, store.tombstoneCount(), collected, blockers));
    }

    private void scheduleNext() {
        if (stopped) {
            return;
        }
        timer = loop.schedule(sweepInterval, this::sweep);
    }

    private void sweep() {
        timer = null;
        if (stopped) {
            return;
        }
        final HashedBytes candidate = store.tombstoneCandidate(affinity::ownedByMe);
        if (candidate == null) {
            // Nothing of this node's to collect, which is the ordinary case and the one that must
            // stay cheap -- an empty queue settles it in one lookup. Still
            // reported: the tombstone count is a reading, and a gauge that only moved when something
            // was collected would say nothing at all about a store that is simply full of them.
            report(null, false, List.of());
            scheduleNext();
            return;
        }
        // The ballot the candidate is a tombstone at: what the whole decision is about, so a key
        // rewritten between this sweep and the next is a different question and gets asked again.
        final KeyState state = store.get(candidate);
        if (!collector.collect(candidate, state.accepted, outcome -> {
            report(candidate, outcome.collected(), outcome.blockers());
            scheduleNext();
        })) {
            // The collector is busy or closed. Busy cannot happen -- this is its only caller, and
            // it is called once per decision -- and closed means the node is going down, where the
            // guard above ends the loop.
            scheduleNext();
        }
    }
}
