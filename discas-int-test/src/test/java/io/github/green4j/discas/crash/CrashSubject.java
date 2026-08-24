/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.crash;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.Acceptor;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.LocalStore;
import io.github.green4j.discas.node.PeerMessage;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * A separate JVM that writes to a real {@link FileWal} and waits to be killed.
 *
 * <p>It exists because there is no way to test what survives a crash without crashing something.
 * Everything else in the suite simulates the fault -- {@code InMemoryWal.loseUnforcedTail()} drops
 * entries in memory, and a "restart" in the chaos harness is {@code close()} followed by
 * {@code start()} in the same JVM, which runs every orderly path the real thing skips. This process
 * runs none of them: it is stopped with {@code SIGKILL}, so no shutdown hook fires, no final
 * {@code force()} happens, the open segment is never sealed and any temp file stays where it was.
 *
 * <p><b>What SIGKILL does not simulate.</b> The kernel keeps the page cache, so bytes that reached
 * {@code write()} still reach the platter. Losing them needs the machine to go, not the process --
 * which the parent test models by truncating the log to the last offset this process reported as
 * forced. That distinction is the whole point of printing {@code FORCED}.
 *
 * <p>Every line is flushed as it is printed: the parent reads them to know how far this got before
 * it was killed, and an unflushed line is a fact the test would then be missing.
 */
public final class CrashSubject {

    /** Written after each promise the acceptor granted, so the parent knows what was acknowledged. */
    private static final String PROMISED = "PROMISED ";
    /**
     * Written after a promise whose ceiling reservation forced the log, with the total bytes the WAL
     * directory then held. Everything past this offset is what a power cut would take.
     */
    private static final String FORCED = "FORCED ";
    /** Written once the requested state is set up, so the parent can kill at a known point. */
    private static final String READY = "READY";

    private CrashSubject() {
    }

    public static void main(final String[] args) throws Exception {
        final Path directory = Path.of(args[0]);
        final Mode mode = Mode.valueOf(args[1]);

        final FileWal wal = new FileWal(StorageConfig.builder().baseDirectory(directory).build());
        wal.initialize();
        final LocalStore store = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        while (loader.loadBatch(256)) {
            // drain: recover whatever a previous run of this directory left
        }

        switch (mode) {
            case JUST_INITIALIZED:
                // Killed with a marker on disk and nothing else -- the shape of a node that died
                // during its very first start.
                say(READY);
                break;
            case PROMISES:
                promiseForever(wal, store);
                break;
            case COMMITTED_THEN_SNAPSHOT:
                commitThenSnapshot(wal, store);
                break;
            default:
                throw new IllegalArgumentException("Unknown mode " + mode);
        }

        // Wait to be killed. Nothing here closes the log or forces it, which is the point.
        while (true) {
            Thread.sleep(50L);
        }
    }

    /**
     * Promise ballots one at a time, for ever. Each granted promise is acknowledged on stdout the
     * way a real acceptor acknowledges it to a proposer -- after the append, and only if the ceiling
     * that authorises it was forced first.
     */
    private static void promiseForever(final FileWal wal, final LocalStore store) throws Exception {
        final Acceptor acceptor = new Acceptor(NodeId.of("crash"), store);
        final NodeId proposer = NodeId.of("p");
        say(READY);
        long ceilingSeen = store.promisedUpTo();
        for (long counter = store.recoveryPromiseFloor() + 1; ; counter++) {
            final PeerMessage.PrepareResp response = acceptor.handlePrepare(new PeerMessage.PrepareReq(
                    proposer, counter, key(counter), new Ballot(counter, proposer)));
            if (store.promisedUpTo() != ceilingSeen) {
                // The ceiling moved, which means the log was forced on the way. Reported before the
                // acknowledgement below, in that order deliberately: a kill landing between the two
                // leaves the parent with the newer boundary and one fewer acknowledged promise,
                // which errs towards expecting less rather than towards a false failure.
                //
                // The number comes from the WAL, not from having called force() -- a subject that
                // reported its own intentions would let a log that never syncs pass this test.
                ceilingSeen = store.promisedUpTo();
                say(FORCED + wal.forcedBytes() + " ceiling=" + ceilingSeen);
            }
            if (response.ok()) {
                say(PROMISED + counter);
            }
            Thread.sleep(1L);
        }
    }

    /** Commit values, force them, then die in the middle of writing a snapshot of them. */
    private static void commitThenSnapshot(final FileWal wal, final LocalStore store)
            throws Exception {
        for (int i = 0; i < 200; i++) {
            store.writeAccept(key(i), new Ballot(1000 + i, NodeId.of("p")),
                    HashedBytes.copyOf(ByteBuffer.wrap(
                            ("v" + i).getBytes(StandardCharsets.UTF_8))), false);
        }
        wal.force();
        say(FORCED + wal.forcedBytes() + " committed=200");

        final LocalStore.ThrottledSnapshot snapshot = store.beginSnapshot();
        // One batch only, so the snapshot is open and unfinished when the kill lands.
        snapshot.writeBatch(8);
        say(READY);
    }

    private static HashedBytes key(final long i) {
        return HashedBytes.copyOf(ByteBuffer.wrap(
                ("k" + (i % 32)).getBytes(StandardCharsets.UTF_8)));
    }

    private static void say(final String line) {
        System.out.println(line);
        System.out.flush();
    }

    enum Mode {
        /** Initialize the directory and stop there. */
        JUST_INITIALIZED,
        /** Keep promising ballots until killed. */
        PROMISES,
        /** Commit and force a batch of values, then die mid-snapshot. */
        COMMITTED_THEN_SNAPSHOT
    }
}
