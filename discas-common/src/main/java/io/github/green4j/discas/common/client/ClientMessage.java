/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.ByteBuffers;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * The client-to-node protocol message family: get, put, cas, delete, scan and their responses.
 * The peer-to-peer protocol is an independent hierarchy sharing no base with this one.
 * <p>
 * <b>Buffer ownership: the producer copies.</b> Keys, values and cursors are held as read-only
 * {@link ByteBuffer} views rather than copies, so whoever hands bytes over copies them first if
 * they are not already stable -- a decoder copies out of the frame it recycles, a client copies
 * the caller's buffers because a request outlives one attempt, and a node answering a read hands
 * over a view of immutable stored bytes and copies nothing. Do not mutate a buffer's content
 * after handing it over; reading is fine, since accessors return independent duplicates.
 */
public interface ClientMessage {

    /**
     * The human-readable {@code error} text a node returns when authorization refuses an
     * operation. Diagnostic only -- callers distinguish this case by
     * {@link ClientErrorCode#ACCESS_DENIED}, never by comparing this text.
     */
    String ERR_ACCESS_DENIED = "access denied";

    /**
     * Every response states its {@link ClientErrorCode} explicitly; none is inferred from the
     * presence of {@code error} text. Callers branch on the code, and an inferred fallback would
     * flatten a node-side failure into {@link ClientErrorCode#INTERNAL} -- not retryable -- where
     * {@link ClientErrorCode#UNAVAILABLE} was the truth.
     */
    static ClientErrorCode requireCode(final ClientErrorCode errorCode) {
        if (errorCode == null) {
            throw new IllegalArgumentException("errorCode is required");
        }
        return errorCode;
    }

    /** Who sent it: the client id on a request, the node id on a response. */
    String senderId();

    /** Matches a response to its request on a connection that multiplexes them. */
    long correlationId();

    final class ClientGetReq implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final ByteBuffer key;
        private final ReadConsistency consistency;

        public ClientGetReq(final String senderId, final long correlationId,
                            final ByteBuffer key) {
            this(senderId, correlationId, key, ReadConsistency.LINEARIZABLE);
        }

        public ClientGetReq(final String senderId, final long correlationId,
                            final ByteBuffer key, final ReadConsistency consistency) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = ByteBuffers.readOnly(key);
            this.consistency = consistency == null ? ReadConsistency.LINEARIZABLE : consistency;
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public ByteBuffer key() {
            return ByteBuffers.duplicate(key);
        }

        public ReadConsistency consistency() {
            return consistency;
        }

        @Override
        public String toString() {
            return "ClientGetReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", key=" + ByteBuffers.preview(key) + ", consistency=" + consistency + "]";
        }
    }

    final class ClientPutReq implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final ByteBuffer key;
        private final ByteBuffer value;

        public ClientPutReq(final String senderId, final long correlationId,
                            final ByteBuffer key, final ByteBuffer value) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = ByteBuffers.readOnly(key);
            this.value = ByteBuffers.readOnly(value);
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public ByteBuffer key() {
            return ByteBuffers.duplicate(key);
        }

        public ByteBuffer value() {
            return ByteBuffers.duplicate(value);
        }

        @Override
        public String toString() {
            return "ClientPutReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", key=" + ByteBuffers.preview(key)
                    + ", value=" + ByteBuffers.preview(value) + "]";
        }
    }

    /**
     * Compare-and-set fenced on the key's accepted ballot rather than on its bytes.
     * <p>
     * The fence is what makes a write safe to re-send after a coordinator stops answering. A
     * value-compared CAS re-applies whenever the register happens to hold {@code expected}
     * again, so an abandoned round that lands after an intervening inverse write applies the swap
     * a second time. A version-compared one carries a ballot that the intervening write has
     * already overtaken, so the late duplicate is rejected instead.
     * <p>
     * {@link Ballot#ZERO} is a legitimate expected version, not a wildcard: it means "the key
     * has no committed value", i.e. create-if-absent.
     */
    final class ClientCasReq implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final ByteBuffer key;
        private final Ballot expectedVersion;
        private final ByteBuffer desired;

        public ClientCasReq(final String senderId, final long correlationId,
                                     final ByteBuffer key, final Ballot expectedVersion,
                                     final ByteBuffer desired) {
            if (expectedVersion == null) {
                throw new IllegalArgumentException("expectedVersion is required");
            }
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = ByteBuffers.readOnly(key);
            this.expectedVersion = expectedVersion;
            this.desired = ByteBuffers.readOnly(desired);
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public ByteBuffer key() {
            return ByteBuffers.duplicate(key);
        }

        public Ballot expectedVersion() {
            return expectedVersion;
        }

        /** {@code null} deletes the key -- a tombstone is a value like any other here. */
        public ByteBuffer desired() {
            return ByteBuffers.duplicate(desired);
        }

        @Override
        public String toString() {
            return "ClientCasReq[senderId=" + senderId
                    + ", correlationId=" + correlationId
                    + ", key=" + ByteBuffers.preview(key)
                    + ", expectedVersion=" + expectedVersion
                    + ", desired=" + ByteBuffers.preview(desired) + "]";
        }
    }

    /**
     * The answer to a {@link ClientCasReq}, carrying the key's version as well as its value, so a
     * caller that lost the compare can recompute and retry without a second round trip.
     */
    final class ClientCasResp implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final boolean ok;
        private final boolean swapped;
        private final ByteBuffer value;
        private final Ballot version;
        private final String error;
        private final ClientErrorCode errorCode;

        public ClientCasResp(final String senderId, final long correlationId,
                                      final boolean ok, final boolean swapped,
                                      final ByteBuffer value, final Ballot version,
                                      final String error, final ClientErrorCode errorCode) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.ok = ok;
            this.swapped = swapped;
            this.value = ByteBuffers.readOnly(value);
            this.version = version == null ? Ballot.ZERO : version;
            this.error = error;
            this.errorCode = requireCode(errorCode);
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public boolean ok() {
            return ok;
        }

        public boolean swapped() {
            return swapped;
        }

        public ByteBuffer value() {
            return ByteBuffers.duplicate(value);
        }

        /**
         * The key's version as this round observed it -- after the swap when
         * {@link #swapped()}, and as it stood when the compare failed otherwise.
         */
        public Ballot version() {
            return version;
        }

        public String error() {
            return error;
        }

        public ClientErrorCode errorCode() {
            return errorCode;
        }

        @Override
        public String toString() {
            return "ClientCasResp[senderId=" + senderId
                    + ", correlationId=" + correlationId
                    + ", ok=" + ok + ", swapped=" + swapped
                    + ", value=" + ByteBuffers.preview(value)
                    + ", version=" + version
                    + ", error=" + error + ", errorCode=" + errorCode + "]";
        }
    }

    final class ClientDeleteReq implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final ByteBuffer key;

        public ClientDeleteReq(final String senderId, final long correlationId,
                               final ByteBuffer key) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.key = ByteBuffers.readOnly(key);
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public ByteBuffer key() {
            return ByteBuffers.duplicate(key);
        }

        @Override
        public String toString() {
            return "ClientDeleteReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", key=" + ByteBuffers.preview(key) + "]";
        }
    }

    /**
     * A request for one page of a key enumeration. Nodes answer from local state in ascending key
     * order, so a client can merge pages from several nodes and know how far the merge is
     * trustworthy.
     */
    final class ClientScanReq implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final ByteBuffer prefix;
        private final ByteBuffer startAfter;
        private final int limit;

        /**
         * @param prefix     keys must start with these bytes; empty matches all
         * @param startAfter exclusive lower bound in byte order, or {@code null} to start at the
         *                   first key -- the page cursor
         * @param limit      maximum entries to return; must be positive
         */
        public ClientScanReq(final String senderId, final long correlationId,
                             final ByteBuffer prefix, final ByteBuffer startAfter, final int limit) {
            if (limit < 1) {
                throw new IllegalArgumentException("Scan limit must be >= 1, got " + limit);
            }
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.prefix = prefix == null ? ByteBuffers.EMPTY : ByteBuffers.readOnly(prefix);
            this.startAfter = ByteBuffers.readOnly(startAfter);
            this.limit = limit;
        }

        public ByteBuffer prefix() {
            return ByteBuffers.duplicate(prefix);
        }

        /** Exclusive lower bound, or {@code null} for "from the beginning". */
        public ByteBuffer startAfter() {
            return ByteBuffers.duplicate(startAfter);
        }

        public int limit() {
            return limit;
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        @Override
        public String toString() {
            return "ClientScanReq[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", prefix=" + ByteBuffers.preview(prefix)
                    + ", startAfter=" + ByteBuffers.preview(startAfter) + ", limit=" + limit + "]";
        }
    }

    final class ClientGetResp implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final boolean ok;
        private final ByteBuffer value;
        private final String error;
        private final ClientErrorCode errorCode;
        private final Ballot version;

        public ClientGetResp(final String senderId, final long correlationId,
                             final boolean ok, final ByteBuffer value, final String error,
                             final ClientErrorCode errorCode) {
            this(senderId, correlationId, ok, value, error, errorCode, Ballot.ZERO);
        }

        public ClientGetResp(final String senderId, final long correlationId,
                             final boolean ok, final ByteBuffer value, final String error,
                             final ClientErrorCode errorCode, final Ballot version) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.ok = ok;
            this.value = ByteBuffers.readOnly(value);
            this.error = error;
            this.errorCode = requireCode(errorCode);
            this.version = version == null ? Ballot.ZERO : version;
        }

        public ClientErrorCode errorCode() {
            return errorCode;
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public boolean ok() {
            return ok;
        }

        public ByteBuffer value() {
            return ByteBuffers.duplicate(value);
        }

        public String error() {
            return error;
        }

        /**
         * The key's per-key version: the accepted ballot of the returned value
         * ({@link Ballot#ZERO} when the key is absent). Monotonic per key; advances on every
         * commit including tombstones. Used by watch/blocking-query callers to detect change.
         */
        public Ballot version() {
            return version;
        }

        @Override
        public String toString() {
            return "ClientGetResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", ok=" + ok + ", value=" + ByteBuffers.preview(value) + ", error=" + error
                    + ", version=" + version + "]";
        }
    }

    final class ClientPutResp implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final boolean ok;
        private final String error;
        private final ClientErrorCode errorCode;
        private final Ballot version;

        /** Failure form: no round committed, so there is no version to report. */
        public ClientPutResp(final String senderId, final long correlationId,
                             final boolean ok, final String error,
                             final ClientErrorCode errorCode) {
            this(senderId, correlationId, ok, error, errorCode, Ballot.ZERO);
        }

        public ClientPutResp(final String senderId, final long correlationId,
                             final boolean ok, final String error,
                             final ClientErrorCode errorCode, final Ballot version) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.ok = ok;
            this.error = error;
            this.errorCode = requireCode(errorCode);
            this.version = version == null ? Ballot.ZERO : version;
        }

        public ClientErrorCode errorCode() {
            return errorCode;
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public boolean ok() {
            return ok;
        }

        public String error() {
            return error;
        }

        /**
         * The accepted ballot this write committed at ({@link Ballot#ZERO} when the round did
         * not commit). It is the key's new version, so a writer can watch or fence on
         * what it just wrote without reading it back.
         */
        public Ballot version() {
            return version;
        }

        @Override
        public String toString() {
            return "ClientPutResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", ok=" + ok + ", error=" + error + ", version=" + version + "]";
        }
    }

    final class ClientDeleteResp implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final boolean ok;
        private final String error;

        private final ClientErrorCode errorCode;
        private final Ballot version;

        /** Failure form: no round committed, so there is no version to report. */
        public ClientDeleteResp(final String senderId, final long correlationId,
                                final boolean ok, final String error,
                                final ClientErrorCode errorCode) {
            this(senderId, correlationId, ok, error, errorCode, Ballot.ZERO);
        }

        public ClientDeleteResp(final String senderId, final long correlationId,
                                final boolean ok, final String error,
                                final ClientErrorCode errorCode, final Ballot version) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.ok = ok;
            this.error = error;
            this.errorCode = requireCode(errorCode);
            this.version = version == null ? Ballot.ZERO : version;
        }

        public ClientErrorCode errorCode() {
            return errorCode;
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public boolean ok() {
            return ok;
        }

        public String error() {
            return error;
        }

        /**
         * The accepted ballot the tombstone committed at ({@link Ballot#ZERO} when the round did
         * not commit). A delete advances the key's version like any other commit, so this is the
         * version a watcher of the deleted key should resume from.
         */
        public Ballot version() {
            return version;
        }

        @Override
        public String toString() {
            return "ClientDeleteResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", ok=" + ok + ", error=" + error + ", version=" + version + "]";
        }
    }

    /** One page of this node's local key inventory. */
    final class ClientScanResp implements ClientMessage {
        private final String senderId;
        private final long correlationId;
        private final List<ScanEntry> entries;
        private final boolean hasMore;

        /**
         * @param entries matching entries in ascending key order, at most the request's limit
         * @param hasMore whether this node holds further matching keys after the last entry.
         *                The client can only trust a merged page up to the smallest last-key
         *                among nodes reporting {@code true}; beyond that a node may hold keys
         *                it did not send, and returning them would silently skip keys.
         */
        public ClientScanResp(final String senderId, final long correlationId,
                              final List<ScanEntry> entries, final boolean hasMore) {
            this.senderId = senderId;
            this.correlationId = correlationId;
            this.entries = entries;
            this.hasMore = hasMore;
        }

        public boolean hasMore() {
            return hasMore;
        }

        @Override
        public String senderId() {
            return senderId;
        }

        @Override
        public long correlationId() {
            return correlationId;
        }

        public List<ScanEntry> entries() {
            return entries;
        }

        @Override
        public String toString() {
            return "ClientScanResp[senderId=" + senderId + ", correlationId=" + correlationId
                    + ", entries=" + entries + "]";
        }
    }

    /** One key in a scan page, with the version that produced it and whether it is a tombstone. */
    final class ScanEntry {
        private final ByteBuffer key;
        private final Ballot version;
        private final boolean tombstone;

        public ScanEntry(final ByteBuffer key, final Ballot version, final boolean tombstone) {
            this.key = ByteBuffers.readOnly(key);
            this.version = version;
            this.tombstone = tombstone;
        }

        public ByteBuffer key() {
            return ByteBuffers.duplicate(key);
        }

        /** The accepted ballot that produced this entry -- the key's version at scan time. */
        public Ballot version() {
            return version;
        }

        public boolean tombstone() {
            return tombstone;
        }

        @Override
        public String toString() {
            return "scanEntry[key=" + ByteBuffers.preview(key) + ", version=" + version
                    + ", tombstone=" + tombstone + "]";
        }
    }
}
