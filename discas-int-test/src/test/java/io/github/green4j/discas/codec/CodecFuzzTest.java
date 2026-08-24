/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.codec;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.lock.LockValueCodec;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ClientMessageCodec;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.node.PeerMessage;
import io.github.green4j.discas.node.PeerMessageCodec;
import io.github.green4j.discas.node.PurgeAnswer;
import io.github.green4j.discas.node.wal.EntryCodec;
import io.github.green4j.discas.node.wal.Wal;
import io.github.green4j.discas.node.wal.WalRecordCodec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structured fuzzing of every codec that parses bytes it did not produce.
 * <p>
 * These decoders sit on the WAL-replay and network-ingress paths, where the input is a buffer whose
 * contents may be truncated, reordered or corrupted. The existing per-codec tests cover round-trips
 * plus a handful of hand-picked malformed cases -- which is exactly the coverage shape that let a
 * real defect through earlier: snapshot recovery failed whenever an entry straddled the reader's
 * 64 KiB window, invisible because every snapshot test used an in-memory WAL.
 * <p>
 * The contract asserted here is deliberately narrow, because it is the one that holds for all of
 * them: <b>a decoder handed corrupt bytes must fail as a {@link RuntimeException}, not as an
 * {@link Error}, and must terminate.</b> An {@code OutOfMemoryError} means a length field was
 * believed without being bounded -- the classic length-prefix defect, and one that takes the whole
 * JVM down rather than the one connection or the one WAL record. A hang means a decode loop whose
 * progress depends on the very bytes being corrupted.
 * <p>
 * What is <em>not</em> asserted is which exception, or that a truncated message is rejected at all.
 * Several of these decoders tolerate a short buffer by design (a client predating a trailing field
 * must still parse), so "decodes to something" is a legitimate outcome and pinning it per-codec
 * here would just duplicate the round-trip tests.
 * <p>
 * Mutations are exhaustive and deterministic rather than random, so a failure names an exact
 * offset and is reproducible: every byte offset is flipped, zeroed and set to {@code 0xFF}, the
 * buffer is truncated at every length, and lengths are extended with trailing garbage. A seeded
 * random pass adds multi-byte corruption on top.
 */
@Tag("chaos")
@DisplayName("Codecs -- structured fuzzing of every decoder")
class CodecFuzzTest {

    /** Deterministic: a failing case must be reproducible from the report alone. */
    private static final long SEED = 20260809L;
    private static final int RANDOM_CASES_PER_SAMPLE = 200;

    /** One decoder under test, plus the valid encodings to mutate. */
    private static final class Target {
        final String name;
        final Consumer<byte[]> decode;
        final List<ByteBuffer> samples = new ArrayList<>();

        Target(final String name, final Consumer<byte[]> decode) {
            this.name = name;
            this.decode = decode;
        }

        Target sample(final ByteBuffer encoded) {
            samples.add(encoded.duplicate());
            return this;
        }
    }

    private static List<Target> targets() {
        final List<Target> targets = new ArrayList<>();
        targets.add(new Target("ClientMessageCodec", raw -> ClientMessageCodec.decode(ByteBuffer.wrap(raw)))
                .sample(ClientMessageCodec.encode(new ClientMessage.ClientGetReq(
                        "c1", 7L, TestBytes.utf8("k"), ReadConsistency.SERIALIZABLE)))
                .sample(ClientMessageCodec.encode(new ClientMessage.ClientGetResp(
                        "n1", 7L, true, TestBytes.utf8("value"), null, ClientErrorCode.NONE)))
                .sample(ClientMessageCodec.encode(new ClientMessage.ClientScanReq(
                        "c1", 9L, TestBytes.utf8("pre"), null, 100)))
                .sample(ClientMessageCodec.encode(new ClientMessage.ClientCasReq(
                        "c1", 11L, TestBytes.utf8("k"), new Ballot(4L, NodeId.of("1")),
                        TestBytes.utf8("desired")))));
        targets.add(new Target("PeerMessageCodec", raw -> PeerMessageCodec.decode(ByteBuffer.wrap(raw)))
                .sample(PeerMessageCodec.encode(new PeerMessage.PrepareReq(
                        NodeId.of("1"), 3L, TestBytes.hashed("k"), new Ballot(5L, NodeId.of("1")))))
                .sample(PeerMessageCodec.encode(new PeerMessage.AcceptReq(
                        NodeId.of("1"), 4L, TestBytes.hashed("k"), new Ballot(6L, NodeId.of("1")),
                        TestBytes.hashed("v"), false)))
                // Carries an enum as one byte, so every corruption of it lands on the code the
                // decoder maps back -- the shape that turns into an ArrayIndexOutOfBounds when the
                // mapping trusts the byte.
                .sample(PeerMessageCodec.encode(new PeerMessage.PurgeCheckResp(
                        NodeId.of("1"), 5L, PurgeAnswer.HELD))));

        targets.add(new Target("EntryCodec(ACCEPT)",
                raw -> EntryCodec.decode(EntryCodec.TYPE_ACCEPT, ByteBuffer.wrap(raw)))
                .sample(EntryCodec.encode(new Wal.Entry.Accept(
                        TestBytes.hashed("k"), new Ballot(1L, NodeId.of("1")), TestBytes.hashed("v"), false))));

        targets.add(new Target("EntryCodec(PROMISE)",
                raw -> EntryCodec.decode(EntryCodec.TYPE_PROMISE, ByteBuffer.wrap(raw)))
                .sample(EntryCodec.encode(new Wal.Entry.Promise(
                        TestBytes.hashed("k"), new Ballot(2L, NodeId.of("1"))))));

        targets.add(new Target("EntryCodec(PURGE)",
                raw -> EntryCodec.decode(EntryCodec.TYPE_PURGE, ByteBuffer.wrap(raw)))
                .sample(EntryCodec.encode(new Wal.Entry.Purge(
                        TestBytes.hashed("k"), new Ballot(3L, NodeId.of("1"))))));

        targets.add(new Target("LockValueCodec", raw -> LockValueCodec.decode(ByteBuffer.wrap(raw)))
                .sample(LockValueCodec.encode(new LockValueCodec.LockRecord(
                        "owner", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}), 1000L, 2000L, 1L))));

        // The wire entry point: reads a length prefix straight off the socket buffer and loops, so
        // a corrupted length can wedge the drain loop as well as mis-size a frame.
        final FrameCodec frameCodec = new FrameCodec(256 * 1024);
        targets.add(new Target("FrameCodec.drain", raw -> {
            final ByteBuffer rx = ByteBuffer.allocate(Math.max(1, raw.length) + 64);
            rx.put(raw); // left in write mode: drain() flips it itself
            frameCodec.drain(rx);
        }).sample(frameCodec.encode(FrameCodec.TYPE_CLIENT_MESSAGE,
                ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}))));

        // Read back from disk during WAL replay, where the bytes may be a torn tail.
        final ByteBuffer walRecord = ByteBuffer.allocate(256);
        WalRecordCodec.encode(walRecord, 42L, EntryCodec.TYPE_ACCEPT,
                EntryCodec.encode(new Wal.Entry.Accept(
                        TestBytes.hashed("k"), new Ballot(1L, NodeId.of("1")), TestBytes.hashed("v"), false)));
        walRecord.flip();
        targets.add(new Target("WalRecordCodec.decodeToOffsets",
                raw -> WalRecordCodec.decodeToOffsets(ByteBuffer.wrap(raw)))
                .sample(walRecord));

        targets.add(new Target("EntryCodec.decodeSnapshotEntry",
                raw -> EntryCodec.decodeSnapshotEntry(ByteBuffer.wrap(raw)))
                .sample(EntryCodec.encodeSnapshotEntry(new Wal.SnapshotEntry(
                        TestBytes.hashed("k"), new Ballot(1L, NodeId.of("1")),
                        new Ballot(2L, NodeId.of("1")), TestBytes.hashed("v"), false))));

        return targets;
    }

    @Test
    // A decode loop whose progress depends on the corrupted bytes hangs rather than fails; without
    // this the build would stall instead of reporting.
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @DisplayName("No corruption of any encoded message makes a decoder throw an Error or hang")
    void decodersSurviveCorruption() {
        final Map<String, Integer> casesByTarget = new LinkedHashMap<>();
        final List<String> failures = new ArrayList<>();

        final List<Target> targets = targets();
        for (int t = 0; t < targets.size(); t++) {
            final Target target = targets.get(t);
            int cases = 0;
            for (int s = 0; s < target.samples.size(); s++) {
                final byte[] valid = toArray(target.samples.get(s));
                cases += fuzzOne(target, valid, failures);
            }
            casesByTarget.put(target.name, cases);
        }

        assertTrue(failures.isEmpty(),
                "Decoders must fail cleanly on corrupt input, but:\n  "
                        + String.join("\n  ", failures));
        // A silent pass would otherwise be indistinguishable from a harness that generated nothing.
        for (final Map.Entry<String, Integer> e : casesByTarget.entrySet()) {
            assertTrue(e.getValue() > 100,
                    e.getKey() + " was only fuzzed with " + e.getValue() + " cases");
        }
    }

    /** Runs every mutation of {@code valid} through {@code target}; returns the case count. */
    private static int fuzzOne(final Target target, final byte[] valid, final List<String> failures) {
        int cases = 0;
        for (int i = 0; i < valid.length; i++) {
            final int[] replacements = {valid[i] ^ 0xFF, 0x00, 0xFF, 0x7F, 0x80};
            for (int r = 0; r < replacements.length; r++) {
                final byte[] mutated = valid.clone();
                mutated[i] = (byte) replacements[r];
                cases++;
                check(target, mutated, "byte " + i + " -> 0x"
                        + Integer.toHexString(replacements[r] & 0xFF), failures);
            }
        }
        // Truncation at every length, including empty.
        for (int len = 0; len < valid.length; len++) {
            final byte[] mutated = new byte[len];
            System.arraycopy(valid, 0, mutated, 0, len);
            cases++;
            check(target, mutated, "truncated to " + len + " of " + valid.length, failures);
        }
        // Trailing garbage.
        for (int extra = 1; extra <= 4; extra++) {
            final byte[] mutated = new byte[valid.length + extra];
            System.arraycopy(valid, 0, mutated, 0, valid.length);
            for (int j = valid.length; j < mutated.length; j++) {
                mutated[j] = (byte) 0xAB;
            }
            cases++;
            check(target, mutated, "extended by " + extra + " garbage bytes", failures);
        }
        // Multi-byte corruption, seeded so a failure reproduces.
        final Random rng = new Random(SEED + valid.length);
        for (int c = 0; c < RANDOM_CASES_PER_SAMPLE; c++) {
            final byte[] mutated = valid.clone();
            final int hits = 1 + rng.nextInt(4);
            final StringBuilder how = new StringBuilder("random:");
            for (int h = 0; h < hits && mutated.length > 0; h++) {
                final int at = rng.nextInt(mutated.length);
                final byte to = (byte) rng.nextInt(256);
                mutated[at] = to;
                how.append(' ').append(at).append("=0x").append(Integer.toHexString(to & 0xFF));
            }
            cases++;
            check(target, mutated, how.toString(), failures);
        }
        return cases;
    }

    private static void check(final Target target, final byte[] input, final String how,
                              final List<String> failures) {
        try {
            target.decode.accept(input);
        } catch (final RuntimeException expected) {
            // Rejecting corrupt input is the point; which exception is the codec's business.
        } catch (final Error fatal) {
            // OutOfMemoryError here means a length field was trusted without a bound.
            failures.add(target.name + ": " + fatal.getClass().getSimpleName()
                    + " on " + how + " -- " + fatal.getMessage());
        }
    }

    private static byte[] toArray(final ByteBuffer buffer) {
        final ByteBuffer view = buffer.duplicate();
        final byte[] out = new byte[view.remaining()];
        view.get(out);
        return out;
    }
}
