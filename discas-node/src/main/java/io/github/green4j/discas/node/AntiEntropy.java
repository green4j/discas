/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerTransport;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

final class AntiEntropy {

    private final NodeId nodeId;
    private final LocalStore store;
    private final Proposer proposer;
    private final PeerTransport transport;
    private final EventLoop loop;
    private final CorrelationIdGenerator correlationIdGenerator;
    private final NodeObserver observer;

    private final Map<Long, PendingDigest> pendingDigests = new HashMap<>();
    private final Map<Long, PendingKeys> pendingKeys = new HashMap<>();

    private boolean cycleActive = false;
    private boolean stopped = false;
    private Duration repairInterval = null;
    private EventLoop.TimerHandle nextCycleTimer;

    /**
     * Ranges whose repair this cycle has started and not yet finished -- the single source of
     * truth for cycle progress, so completion is derived from it rather than from a parallel
     * counter that could disagree with it.
     */
    private final Set<Integer> activeRepairRanges = new HashSet<>();

    /**
     * True while {@link #compareAndDrill} is still issuing {@code KeysReq}s. Completion is not
     * evaluated during that window: a range that finished immediately would otherwise empty
     * {@link #activeRepairRanges} mid-dispatch and end the cycle before its remaining ranges had
     * even been requested. A transport that delivers on the loop cannot produce that; the guard
     * keeps the invariant independent of the timing.
     */
    private boolean dispatchingRanges = false;

    private static final int MAX_CONCURRENT_REPAIRS = 8;
    /** Keys requested per range page; the responder clamps to the same {@code KvLimits} cap. */
    private static final int KEYS_PAGE_LIMIT = KvLimits.MAX_ANTI_ENTROPY_KEYS_PER_PAGE;
    private final Duration responseTimeout;

    AntiEntropy(final NodeId nodeId,
                       final LocalStore store,
                       final Proposer proposer,
                       final PeerTransport transport,
                       final EventLoop loop,
                       final CorrelationIdGenerator correlationIdGenerator) {
        this(nodeId, store, proposer, transport, loop, correlationIdGenerator, NodeObserver.NONE);
    }

    /**
     * The response timeout defaults to {@link NodeConfig}'s -- a bare builder is the source of
     * every default, so no value is duplicated here as a constant.
     */
    AntiEntropy(final NodeId nodeId,
                       final LocalStore store,
                       final Proposer proposer,
                       final PeerTransport transport,
                       final EventLoop loop,
                       final CorrelationIdGenerator correlationIdGenerator,
                       final NodeObserver observer) {
        this(nodeId, store, proposer, transport, loop, correlationIdGenerator, observer,
                NodeConfig.builder().peerResponseTimeout());
    }

    /** @param responseTimeout how long a digest or key-page exchange waits for a peer. */
    AntiEntropy(final NodeId nodeId,
                       final LocalStore store,
                       final Proposer proposer,
                       final PeerTransport transport,
                       final EventLoop loop,
                       final CorrelationIdGenerator correlationIdGenerator,
                       final NodeObserver observer,
                       final Duration responseTimeout) {
        this.responseTimeout = responseTimeout;
        this.nodeId = nodeId;
        this.store = store;
        this.proposer = proposer;
        this.transport = transport;
        this.loop = loop;
        this.correlationIdGenerator = correlationIdGenerator;
        this.observer = observer == null ? NodeObserver.NONE : observer;
    }

    /**
     * Start repairing. The first cycle runs immediately; each later one is scheduled the given
     * interval after the previous one completes. Called once, after recovery.
     */
    void startPeriodicRepair(final Duration interval) {
        repairInterval = interval;
        runRepairCycle();
    }

    public void close() {
        stopped = true;
        if (nextCycleTimer != null) {
            nextCycleTimer.cancel();
            nextCycleTimer = null;
        }
        for (final PendingDigest pd : pendingDigests.values()) {
            if (pd.timerHandle != null) {
                pd.timerHandle.cancel();
            }
        }
        pendingDigests.clear();
        for (final PendingKeys pk : pendingKeys.values()) {
            if (pk.timerHandle != null) {
                pk.timerHandle.cancel();
            }
        }
        pendingKeys.clear();
        // Clearing the set means a repair round still in flight finds its range already gone
        // in onRangeRepairComplete and returns without touching the (now stopped) cycle.
        activeRepairRanges.clear();
        dispatchingRanges = false;
        cycleActive = false;
    }

    /** Run one repair cycle, or nothing if one is already active. */
    void runRepairCycle() {
        if (stopped || cycleActive) {
            return;
        }
        cycleActive = true;
        // activeRepairRanges is necessarily empty here: a cycle only completes once it drains
        // (onRangeRepairComplete), and cycleActive keeps cycles from overlapping.
        observer.repairCycleStarted();

        store.clearRecentlyTouched();
        final long correlationId = correlationIdGenerator.next();
        // Collect from ALL peers instead of quorum-1. With quorum-1,
        // a consistently slow peer holding newer data is never consulted,
        // and divergence on that peer is never detected from this node's cycle
        // The existing responseTimeout handles non-responsive peers.
        // NOTE: this is the LIVE connectivity set, deliberately NOT the frozen
        // consensus quorum (Proposer.clusterSize). Anti-entropy only reconciles/
        // repairs; it never decides values, so best-effort over whoever is
        // currently connected is correct and must not be tied to quorum.
        final int peersNeeded = transport.peers().size();
        final PendingDigest pendingDigest = new PendingDigest(correlationId, peersNeeded);
        pendingDigests.put(correlationId, pendingDigest);

        // Arm the timeout BEFORE the send loop: if transport.send() throws, the timeout is still
        // active and drives the cycle to completion rather than leaving pendingDigest stranded
        // with cycleActive=true forever.
        // Decouple timeout-path threshold from send target. Sending
        // to all peers maximizes detection coverage in the happy path. But
        // requiring all to respond before proceeding blocks all repair when
        // any peer is slow/down. Anti-entropy detection (stages 1-2) is a
        // best-effort heuristic; safety comes from the Paxos repair round
        // (stage 3). Even a single peer response is sufficient to detect and
        // trigger repairs. Missing divergence with unreachable peers is caught
        // by the next cycle or by the peer's own anti-entropy cycle
        pendingDigest.timerHandle = loop.schedule(responseTimeout, () -> {
            final PendingDigest removed = pendingDigests.remove(correlationId);
            if (removed == null) {
                return;
            }

            if (!removed.peerDigests.isEmpty()) {
                compareAndDrill(removed);
            } else {
                onCycleComplete();
            }
        });

        broadcast(new PeerMessage.DigestReq(nodeId, correlationId));
    }

    /**
     * Send to every peer, best-effort: one send failure must not stop the others being asked, and
     * the request's own timeout handles the answers that never come.
     */
    private void broadcast(final PeerMessage request) {
        for (final NodeId peerId : transport.peers()) {
            try {
                transport.send(peerId, request);
            } catch (final Exception e) {
                // Deliberately swallowed; see above.
            }
        }
    }

    void handleDigestReq(final PeerMessage.DigestReq request) {
        try {
            final Map<Integer, HashedBytes> digests = store.computeRangeDigests();
            transport.send(request.senderId(),
                    new PeerMessage.DigestResp(nodeId, request.correlationId(), digests));
        } catch (final Exception e) {
            observer.digestRequestFailed(request.senderId(), e);
        }
    }

    void onDigestResp(final PeerMessage.DigestResp response) {
        final PendingDigest pendingDigest = pendingDigests.get(response.correlationId());
        if (pendingDigest == null) {
            return;
        }
        if (!pendingDigest.respondedPeers.add(response.senderId())) {
            return;
        }
        pendingDigest.peerDigests.add(response.rangeDigests());

        if (pendingDigest.peerDigests.size() >= pendingDigest.peersNeeded) {
            pendingDigests.remove(response.correlationId());
            if (pendingDigest.timerHandle != null) {
                pendingDigest.timerHandle.cancel();
            }
            compareAndDrill(pendingDigest);
        }
    }

    private void compareAndDrill(final PendingDigest pendingDigest) {
        final Map<Integer, HashedBytes> localDigests = store.computeRangeDigests();
        final Set<Integer> suspectRanges = new HashSet<>();

        // Collect the union of all peer range indices so we can
        // detect local-only ranges (ranges that exist locally but no peer
        // reported). Without this, keys written only to this node are never
        // pushed to peers via anti-entropy
        final Set<Integer> allPeerRanges = new HashSet<>();

        for (final Map<Integer, HashedBytes> peerMap : pendingDigest.peerDigests) {
            allPeerRanges.addAll(peerMap.keySet());
            for (final Map.Entry<Integer, HashedBytes> rangeEntry : peerMap.entrySet()) {
                final HashedBytes localDigest = localDigests.get(rangeEntry.getKey());
                if (!rangeEntry.getValue().equals(localDigest)) {
                    suspectRanges.add(rangeEntry.getKey());
                }
            }
            for (final int rangeIndex : peerMap.keySet()) {
                if (!localDigests.containsKey(rangeIndex)) {
                    suspectRanges.add(rangeIndex);
                }
            }
        }

        // Flag local-only ranges -- ranges present locally but
        // absent from every peer's digest. These need repair too.
        for (final int localRange : localDigests.keySet()) {
            if (!allPeerRanges.contains(localRange)) {
                suspectRanges.add(localRange);
            }
        }

        if (suspectRanges.isEmpty()) {
            onCycleComplete();
            return;
        }

        // Dispatch every suspect range not already being repaired. requestKeysForRange adds to
        // activeRepairRanges, so that set alone tracks what this cycle is waiting on -- no
        // separate count to keep in step with it.
        dispatchingRanges = true;
        try {
            for (final int rangeIndex : suspectRanges) {
                if (!activeRepairRanges.contains(rangeIndex)) {
                    requestKeysForRange(rangeIndex);
                }
            }
        } finally {
            dispatchingRanges = false;
        }

        // Nothing outstanding means either every suspect range was already in flight or each
        // finished immediately; either way this cycle has no more work of its own.
        if (activeRepairRanges.isEmpty()) {
            onCycleComplete();
        }
    }

    private void requestKeysForRange(final int rangeIndex) {
        requestKeysForRange(rangeIndex, null);
    }

    /**
     * Request one page of a range's keys from every peer.
     * <p>
     * A range is compared page by page rather than in one message: a dense range would otherwise
     * produce a {@code KeysResp} large enough to breach the peer transport's frame and inflight
     * budgets, and the connection would be torn down by its own backpressure. The range stays in
     * {@code activeRepairRanges} across its pages, so cycle accounting still sees one unit of work.
     *
     * @param startAfter cursor from the previous page, or {@code null} to start the range
     */
    private void requestKeysForRange(final int rangeIndex, final ByteBuffer startAfter) {
        activeRepairRanges.add(rangeIndex);

        final long correlationId = correlationIdGenerator.next();
        // Collect from all peers (same rationale as runRepairCycle)
        final int peersNeeded = transport.peers().size();
        final PendingKeys pendingKeys =
                new PendingKeys(correlationId, rangeIndex, peersNeeded, startAfter);
        this.pendingKeys.put(correlationId, pendingKeys);

        // Arm timeout before send loop (same rationale as runRepairCycle).
        // Same decoupling as digest phase -- proceed with partial
        // responses on timeout. A single peer's key list is enough to detect
        // divergent keys and trigger Paxos repair rounds
        pendingKeys.timerHandle = loop.schedule(responseTimeout, () -> {
            final PendingKeys removed = this.pendingKeys.remove(correlationId);
            if (removed == null) {
                return;
            }

            if (!removed.peerResponses.isEmpty()) {
                repairRange(removed);
            } else {
                // No peer answered this page. End the range for this cycle rather than paging on
                // against silence; the next cycle restarts it from the beginning.
                onRangeRepairComplete(removed.rangeIndex);
            }
        });

        final PeerMessage.KeysReq request = new PeerMessage.KeysReq(
                nodeId, correlationId, rangeIndex, startAfter, KEYS_PAGE_LIMIT);
        broadcast(request);
    }

    void handleKeysReq(final PeerMessage.KeysReq request) {
        try {
            final LocalStore.KeyDigestPage page = store.keyDigestsForRange(
                    request.range(), request.startAfter(), request.limit());
            transport.send(request.senderId(),
                    new PeerMessage.KeysResp(nodeId, request.correlationId(),
                            request.range(), page.digests(), page.hasMore()));
        } catch (final Exception e) {
            observer.keysRequestFailed(request.senderId(), e);
        }
    }

    void onKeysResp(final PeerMessage.KeysResp response) {
        final PendingKeys pendingKeysEntry = pendingKeys.get(response.correlationId());
        if (pendingKeysEntry == null) {
            return;
        }
        if (!pendingKeysEntry.respondedPeers.add(response.senderId())) {
            return;
        }
        // Reject responses whose range doesn't match. Correlation IDs
        // should be unique per request, but a peer bug or correlation collision
        // could deliver data for the wrong range, contaminating repair decisions
        if (response.range() != pendingKeysEntry.rangeIndex) {
            return;
        }
        pendingKeysEntry.peerResponses.add(response);

        if (pendingKeysEntry.peerResponses.size() >= pendingKeysEntry.peersNeeded) {
            pendingKeys.remove(response.correlationId());
            if (pendingKeysEntry.timerHandle != null) {
                pendingKeysEntry.timerHandle.cancel();
            }
            repairRange(pendingKeysEntry);
        }
    }

    private void repairRange(final PendingKeys pendingKeysEntry) {
        final LocalStore.KeyDigestPage localPage = store.keyDigestsForRange(
                pendingKeysEntry.rangeIndex, pendingKeysEntry.startAfter, KEYS_PAGE_LIMIT);

        // How far this page's comparison is sound. Beyond it a key missing from some responder's
        // page may simply not have been sent, and treating that as divergence would fire a repair
        // round per key for nothing.
        final ByteBuffer bound = pageTrustBound(localPage, pendingKeysEntry.peerResponses);

        final Map<HashedBytes, KeyDigest> localByKey = new HashMap<>();
        for (final KeyDigest keyDigest : localPage.digests()) {
            if (withinBound(keyDigest.key(), bound)) {
                localByKey.put(keyDigest.key(), keyDigest);
            }
        }

        final Set<HashedBytes> allKeys = new HashSet<>(localByKey.keySet());
        final List<Map<HashedBytes, KeyDigest>> peerKeyMaps = new ArrayList<>();
        for (int i = 0; i < pendingKeysEntry.peerResponses.size(); i++) {
            final Map<HashedBytes, KeyDigest> peerMap = new HashMap<>();
            final List<KeyDigest> peerList = pendingKeysEntry.peerResponses.get(i).digests();
            for (int j = 0; j < peerList.size(); j++) {
                final KeyDigest keyDigest = peerList.get(j);
                if (!withinBound(keyDigest.key(), bound)) {
                    continue;
                }
                peerMap.put(keyDigest.key(), keyDigest);
                allKeys.add(keyDigest.key());
            }
            peerKeyMaps.add(peerMap);
        }

        // Only with every member's answer in hand may a key be dropped for want of confirmation:
        // a peer that did not answer confirms nothing either way.
        final boolean everyPeerAnswered =
                pendingKeysEntry.peerResponses.size() == transport.peers().size();

        final Queue<HashedBytes> divergentKeys = new ArrayDeque<>();
        for (final HashedBytes key : allKeys) {
            if (store.wasTouchedRecently(key)) {
                continue;
            }
            final KeyDigest localDigest = localByKey.get(key);
            if (localDigest != null && everyPeerAnswered && noPeerHolds(key, peerKeyMaps)
                    && store.dropUnaccountedFor(key)) {
                // A key only this node has, on a node that cannot account for its own history: it
                // was never chosen, and pushing it into a repair round is how a hole that swallowed
                // a tombstone resurrects the value underneath. See LocalStore.dropUnaccountedFor.
                observer.unaccountedKeyDropped(key, localDigest.accepted());
                continue;
            }
            for (final Map<HashedBytes, KeyDigest> peerMap : peerKeyMaps) {
                final KeyDigest peerDigest = peerMap.get(key);
                if (isDivergent(localDigest, peerDigest)) {
                    divergentKeys.add(key);
                    break;
                }
            }
        }

        if (divergentKeys.isEmpty()) {
            advanceRange(pendingKeysEntry.rangeIndex, bound);
            return;
        }

        runThrottledRepairs(divergentKeys, pendingKeysEntry.rangeIndex, bound);
    }

    /**
     * The highest key this page's comparison covers, or {@code null} when every responder --
     * including this node -- returned its whole remaining range (the range is then finished).
     * <p>
     * Identical reasoning to the client scan merge: a responder that truncated its page may hold
     * keys it did not send, so nothing above the <em>smallest</em> last-key among truncated
     * responders can be reasoned about. The local page counts as a responder; leaving it out would
     * make every peer key beyond the local cut look locally missing.
     */
    static ByteBuffer pageTrustBound(final LocalStore.KeyDigestPage localPage,
                                     final List<PeerMessage.KeysResp> peerResponses) {
        // Returned as a view rather than a copy, and safe to retain across the next KeysReq
        // round-trip: every candidate is a key held by an immutable HashedBytes -- either one of
        // this node's own store keys, or one the codec already copied out of the peer's frame --
        // so nothing here aliases a transport buffer.
        ByteBuffer bound = null;
        if (localPage.hasMore() && !localPage.digests().isEmpty()) {
            bound = localPage.digests().get(localPage.digests().size() - 1).key().toBuffer();
        }
        for (int i = 0; i < peerResponses.size(); i++) {
            final PeerMessage.KeysResp response = peerResponses.get(i);
            final List<KeyDigest> digests = response.digests();
            if (!response.hasMore() || digests.isEmpty()) {
                continue;
            }
            final HashedBytes last = digests.get(digests.size() - 1).key();
            if (bound == null || last.compareTo(bound) < 0) {
                bound = last.toBuffer();
            }
        }
        return bound;
    }

    private static boolean noPeerHolds(final HashedBytes key,
                                       final List<Map<HashedBytes, KeyDigest>> peerKeyMaps) {
        for (int i = 0; i < peerKeyMaps.size(); i++) {
            if (peerKeyMaps.get(i).containsKey(key)) {
                return false;
            }
        }
        return true;
    }

    private static boolean withinBound(final HashedBytes key, final ByteBuffer bound) {
        return bound == null || key.compareTo(bound) <= 0;
    }

    /**
     * Move to the range's next page, or finish the range when {@code nextCursor} is {@code null}.
     * A cursor is always a key that was just compared, so the next page starts strictly after it
     * and paging cannot stall.
     */
    private void advanceRange(final int rangeIndex, final ByteBuffer nextCursor) {
        if (nextCursor == null) {
            onRangeRepairComplete(rangeIndex);
            return;
        }
        requestKeysForRange(rangeIndex, nextCursor);
    }

    /**
     * Whether two replicas hold different <em>state</em> for a key -- which is not the same question
     * as whether they hold different records.
     * <p>
     * <b>A tombstone and a key that is not there are the same state: no value.</b> One side holding
     * a tombstone the other has collected is therefore agreement, not divergence, and this is what
     * makes collection stick: a member that applied a purge would otherwise be handed the tombstone
     * straight back by a member that had not applied it yet, undoing every collection as fast as it
     * was decided. The member still holding it collects it on a later sweep, where the ones that
     * already purged answer {@code ABSENT} and permit.
     * <p>
     * Nothing is lost by not pushing a tombstone to a replica that holds nothing for the key,
     * because there is nothing there for it to suppress. The replica that <em>has</em> not seen a
     * delete is the one still holding the <b>value</b>, and that is divergence by the same rule --
     * which is the whole job tombstones exist for, and is unaffected.
     */
    private boolean isDivergent(final KeyDigest localDigest, final KeyDigest peerDigest) {
        if (localDigest == null && peerDigest == null) {
            return false;
        }
        if (localDigest == null) {
            return !peerDigest.tombstone();
        }
        if (peerDigest == null) {
            return !localDigest.tombstone();
        }
        return !localDigest.accepted().equals(peerDigest.accepted())
                || !localDigest.valueHash().equals(peerDigest.valueHash())
                || localDigest.tombstone() != peerDigest.tombstone();
    }

    private void runThrottledRepairs(final Queue<HashedBytes> keys, final int rangeIndex,
                                     final ByteBuffer nextCursor) {
        final int[] inflightCount = {0};
        repairNext(keys, inflightCount, rangeIndex, nextCursor);
    }

    private void repairNext(final Queue<HashedBytes> keys, final int[] inflightCount,
                            final int rangeIndex, final ByteBuffer nextCursor) {
        while (!keys.isEmpty() && inflightCount[0] < MAX_CONCURRENT_REPAIRS) {
            final HashedBytes nextKey = keys.poll();
            inflightCount[0]++;
            observer.keyRepaired(nextKey);
            proposer.repairRound(nextKey).whenCompleteAsync((result, error) -> {
                inflightCount[0]--;
                repairNext(keys, inflightCount, rangeIndex, nextCursor);
            }, loop.rejectingExecutor());
        }
        if (keys.isEmpty() && inflightCount[0] == 0) {
            // This page is done: continue the range, or complete it if the page was the last.
            advanceRange(rangeIndex, nextCursor);
        }
    }

    /** One range is done: repaired, empty, or timed out. */
    private void onRangeRepairComplete(final int rangeIndex) {
        if (!activeRepairRanges.remove(rangeIndex)) {
            // Already accounted for -- a duplicate completion must not advance the cycle.
            return;
        }
        if (!dispatchingRanges && activeRepairRanges.isEmpty()) {
            onCycleComplete();
        }
    }

    /**
     * Called when the entire repair cycle is done. Schedules the next cycle after the
     * configured interval.
     * <p>
     * Idempotent: {@code cycleActive} gates re-entry so a second call for the same cycle
     * cannot schedule a second timer. Without that guard an extra call would overwrite
     * {@link #nextCycleTimer} and orphan the previous handle -- leaking a timer that
     * {@link #close()} can no longer cancel, and letting two cycle chains run at once.
     */
    private void onCycleComplete() {
        if (!cycleActive) {
            return;
        }
        cycleActive = false;
        if (repairInterval != null && !stopped) {
            // Cancel before overwriting: the handle field holds at most one live timer.
            if (nextCycleTimer != null) {
                nextCycleTimer.cancel();
            }
            nextCycleTimer = loop.schedule(repairInterval, this::runRepairCycle);
        }
    }

    private static final class PendingDigest {
        final long correlationId;
        final int peersNeeded;
        final List<Map<Integer, HashedBytes>> peerDigests;
        final Set<NodeId> respondedPeers = new HashSet<>();
        EventLoop.TimerHandle timerHandle;

        PendingDigest(final long correlationId, final int peersNeeded) {
            this.correlationId = correlationId;
            this.peersNeeded = peersNeeded;
            this.peerDigests = new ArrayList<>();
        }
    }

    private static final class PendingKeys {
        final long correlationId;
        final int rangeIndex;
        final int peersNeeded;
        /** The cursor this page was requested from; {@code null} for the range's first page. */
        final ByteBuffer startAfter;
        /** Whole responses, not just digest lists: the comparison needs each peer's hasMore. */
        final List<PeerMessage.KeysResp> peerResponses;
        final Set<NodeId> respondedPeers = new HashSet<>();
        EventLoop.TimerHandle timerHandle;

        PendingKeys(final long correlationId, final int rangeIndex, final int peersNeeded,
                    final ByteBuffer startAfter) {
            this.correlationId = correlationId;
            this.rangeIndex = rangeIndex;
            this.peersNeeded = peersNeeded;
            this.startAfter = startAfter;
            this.peerResponses = new ArrayList<>();
        }
    }
}
