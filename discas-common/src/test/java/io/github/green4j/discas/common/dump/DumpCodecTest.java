/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.dump;

import io.github.green4j.discas.common.KvLimits;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Dump format -- a file that is not whole is not a dump")
class DumpCodecTest {

    @TempDir
    Path directory;

    @Test
    @DisplayName("Pairs come back in order, with the header they were written under")
    void roundTrip() throws IOException {
        final DumpHeader header = new DumpHeader(1_700_000_000_000L,
                Arrays.asList(buffer("users/"), buffer("orders/")));
        final List<Pair> written = Arrays.asList(
                new Pair(bytes("users/1"), bytes("alice")),
                new Pair(bytes("users/2"), new byte[]{0, 1, 2, (byte) 0xFF}),
                new Pair(bytes("orders/9"), new byte[0]));

        try (DumpReader reader = DumpReader.open(write(header, written))) {
            assertEquals(1_700_000_000_000L, reader.header().createdAtEpochMs());
            assertEquals(2, reader.header().prefixes().size());
            assertArrayEquals(bytes("users/"), drain(reader.header().prefixes().get(0)));
            assertArrayEquals(bytes("orders/"), drain(reader.header().prefixes().get(1)));
            assertEquals(3, reader.entryCount());
            assertPairs(written, read(reader));
        }
    }

    @Test
    @DisplayName("A dump of nothing is still a dump")
    void emptyDump() throws IOException {
        try (DumpReader reader = DumpReader.open(write(wholeKeySpace(), Collections.emptyList()))) {
            assertEquals(0, reader.entryCount());
            assertTrue(reader.header().prefixes().isEmpty());
            assertTrue(read(reader).isEmpty());
        }
    }

    @Test
    @DisplayName("An entry larger than the reader's window reads back byte for byte")
    void entryLargerThanTheWindow() throws IOException {
        final byte[] value = new byte[512 * 1024];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        final List<Pair> written = Arrays.asList(
                new Pair(bytes("big"), value),
                new Pair(bytes("small"), bytes("x")));

        try (DumpReader reader = DumpReader.open(write(wholeKeySpace(), written))) {
            assertPairs(written, read(reader));
        }
    }

    @Test
    @DisplayName("A dump that was never committed is refused, not read short")
    void uncommittedDumpIsRefused() throws IOException {
        // The failure this whole format is arranged around: a scan that died halfway, or a
        // connection dropped mid-response, leaves a file that is byte-continuous and wrong.
        final Path file = directory.resolve("uncommitted.dump");
        try (FileChannel channel = open(file)) {
            final DumpWriter writer = new DumpWriter(channel, wholeKeySpace());
            writer.writeEntry(ByteBuffer.wrap(bytes("k")), ByteBuffer.wrap(bytes("v")));
            // no commit()
        }

        assertThrows(DumpFormatException.class, () -> DumpReader.open(file));
    }

    @Test
    @DisplayName("Truncation at any byte is refused")
    void truncationAtAnyByteIsRefused() throws IOException {
        final byte[] whole = Files.readAllBytes(write(wholeKeySpace(),
                Arrays.asList(new Pair(bytes("a"), bytes("1")), new Pair(bytes("b"), bytes("2")))));

        for (int length = 0; length < whole.length; length++) {
            final Path truncated = directory.resolve("truncated-" + length + ".dump");
            Files.write(truncated, Arrays.copyOf(whole, length));
            assertThrows(DumpFormatException.class, () -> DumpReader.open(truncated),
                    "Truncation to " + length + " bytes was accepted");
        }
    }

    @Test
    @DisplayName("A single flipped bit anywhere is refused")
    void corruptionAtAnyByteIsRefused() throws IOException {
        final byte[] whole = Files.readAllBytes(write(
                new DumpHeader(7L, Collections.singletonList(buffer("p/"))),
                Arrays.asList(new Pair(bytes("p/a"), bytes("1")), new Pair(bytes("p/b"), bytes("2")))));

        for (int offset = 0; offset < whole.length; offset++) {
            final byte[] corrupt = whole.clone();
            corrupt[offset] ^= 0x01;
            final Path file = directory.resolve("corrupt-" + offset + ".dump");
            Files.write(file, corrupt);
            assertThrows(DumpFormatException.class, () -> DumpReader.open(file),
                    "A flipped bit at offset " + offset + " was accepted");
        }
    }

    @Test
    @DisplayName("Bytes after the trailer are refused -- two dumps in one file are neither")
    void trailingBytesAreRefused() throws IOException {
        final byte[] whole = Files.readAllBytes(
                write(wholeKeySpace(), Collections.singletonList(new Pair(bytes("k"), bytes("v")))));

        final Path file = directory.resolve("extended.dump");
        Files.write(file, Arrays.copyOf(whole, whole.length + 1));

        assertThrows(DumpFormatException.class, () -> DumpReader.open(file));
    }

    @Test
    @DisplayName("A dump written under another format version is refused, not guessed at")
    void unknownFormatVersionIsRefused() throws IOException {
        final byte[] whole = Files.readAllBytes(
                write(wholeKeySpace(), Collections.singletonList(new Pair(bytes("k"), bytes("v")))));
        // The version sits right after the magic; the checksum would catch this too, so rewrite it
        // and leave the file otherwise valid.
        final byte[] future = whole.clone();
        ByteBuffer.wrap(future).order(ByteOrder.BIG_ENDIAN)
                .putInt(DumpCodec.MAGIC_BYTES, DumpCodec.FORMAT_VERSION + 1);
        reChecksum(future);

        final Path file = directory.resolve("future.dump");
        Files.write(file, future);

        assertThrows(DumpFormatException.class, () -> DumpReader.open(file));
    }

    @Test
    @DisplayName("A trailer counting entries the file does not hold is refused")
    void entryCountMismatchIsRefused() throws IOException {
        final byte[] whole = Files.readAllBytes(write(wholeKeySpace(),
                Arrays.asList(new Pair(bytes("a"), bytes("1")), new Pair(bytes("b"), bytes("2")))));
        // The count is the last field the checksum covers, so a tampered one with a recomputed
        // checksum is exactly the file a CRC alone would wave through.
        final byte[] miscounted = whole.clone();
        ByteBuffer.wrap(miscounted).order(ByteOrder.BIG_ENDIAN)
                .putLong(miscounted.length - Long.BYTES - Integer.BYTES, 3L);
        reChecksum(miscounted);

        final Path file = directory.resolve("miscounted.dump");
        Files.write(file, miscounted);

        assertThrows(DumpFormatException.class, () -> DumpReader.open(file));
    }

    @Test
    @DisplayName("The writer refuses what the store could not hold anyway")
    void writerRefusesOversizedPairs() throws IOException {
        try (FileChannel channel = open(directory.resolve("oversized.dump"))) {
            final DumpWriter writer = new DumpWriter(channel, wholeKeySpace());
            final ByteBuffer key = ByteBuffer.allocate(KvLimits.MAX_KEY_BYTES + 1);
            assertThrows(IllegalArgumentException.class,
                    () -> writer.writeEntry(key, ByteBuffer.allocate(1)));
            assertThrows(IllegalArgumentException.class,
                    () -> writer.writeEntry(ByteBuffer.allocate(1), null));
        }
    }

    @Test
    @DisplayName("An entry after the trailer is a caller's bug, not a longer file")
    void writingAfterCommitIsRejected() throws IOException {
        try (FileChannel channel = open(directory.resolve("committed.dump"))) {
            final DumpWriter writer = new DumpWriter(channel, wholeKeySpace());
            writer.commit();
            writer.commit();   // idempotent
            assertThrows(IllegalStateException.class,
                    () -> writer.writeEntry(ByteBuffer.wrap(bytes("k")), ByteBuffer.wrap(bytes("v"))));
        }
    }

    @Test
    @DisplayName("The writer leaves the caller's buffers where it found them")
    void writerDoesNotConsumeItsArguments() throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(bytes("k"));
        final ByteBuffer value = ByteBuffer.wrap(bytes("v")).asReadOnlyBuffer();

        final Path file = directory.resolve("shared.dump");
        try (FileChannel channel = open(file)) {
            final DumpWriter writer = new DumpWriter(channel, wholeKeySpace());
            writer.writeEntry(key, value);
            writer.writeEntry(key, value);
            writer.commit();
        }

        assertEquals(1, key.remaining());
        assertEquals(1, value.remaining());
        try (DumpReader reader = DumpReader.open(file)) {
            assertEquals(2, reader.entryCount());
        }
    }

    @Test
    @DisplayName("A header holds copies, so the prefixes it reports cannot be edited afterwards")
    void headerCopiesItsPrefixes() {
        final byte[] prefix = bytes("users/");
        final List<ByteBuffer> prefixes = new ArrayList<>();
        prefixes.add(ByteBuffer.wrap(prefix));

        final DumpHeader header = new DumpHeader(0L, prefixes);
        prefix[0] = 'X';
        prefixes.clear();

        assertEquals(1, header.prefixes().size());
        assertArrayEquals(bytes("users/"), drain(header.prefixes().get(0)));
    }

    @Test
    @DisplayName("More prefixes than the format bounds is a caller's bug")
    void tooManyPrefixesIsRejected() {
        final List<ByteBuffer> prefixes = new ArrayList<>();
        for (int i = 0; i <= DumpCodec.MAX_PREFIXES; i++) {
            prefixes.add(buffer("p" + i));
        }

        assertThrows(IllegalArgumentException.class, () -> new DumpHeader(0L, prefixes));
    }

    private static DumpHeader wholeKeySpace() {
        return new DumpHeader(42L, Collections.emptyList());
    }

    private Path write(final DumpHeader header, final List<Pair> pairs) throws IOException {
        final Path file = directory.resolve("dump-" + pairs.size() + "-" + System.nanoTime());
        try (FileChannel channel = open(file)) {
            final DumpWriter writer = new DumpWriter(channel, header);
            for (final Pair pair : pairs) {
                writer.writeEntry(ByteBuffer.wrap(pair.key), ByteBuffer.wrap(pair.value));
            }
            writer.commit();
            assertEquals(pairs.size(), writer.entryCount());
        }
        return file;
    }

    private static FileChannel open(final Path file) throws IOException {
        return FileChannel.open(file,
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
    }

    private static List<Pair> read(final DumpReader reader) throws IOException {
        final List<Pair> found = new ArrayList<>();
        reader.forEachEntry((key, value) -> {
            assertTrue(key.isReadOnly());
            assertTrue(value.isReadOnly());
            found.add(new Pair(drain(key), drain(value)));
        });
        return found;
    }

    /** Rewrites the trailer's checksum so a tampered file fails on what it is meant to fail on. */
    private static void reChecksum(final byte[] file) {
        final CRC32 crc = new CRC32();
        crc.update(file, 0, file.length - Integer.BYTES);
        ByteBuffer.wrap(file).order(ByteOrder.BIG_ENDIAN)
                .putInt(file.length - Integer.BYTES, (int) crc.getValue());
    }

    private static void assertPairs(final List<Pair> expected, final List<Pair> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i).key, actual.get(i).key, "key " + i);
            assertArrayEquals(expected.get(i).value, actual.get(i).value, "value " + i);
        }
    }

    private static byte[] drain(final ByteBuffer buffer) {
        final ByteBuffer view = buffer.duplicate();
        final byte[] copy = new byte[view.remaining()];
        view.get(copy);
        return copy;
    }

    private static ByteBuffer buffer(final String text) {
        return ByteBuffer.wrap(bytes(text));
    }

    private static byte[] bytes(final String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static final class Pair {
        private final byte[] key;
        private final byte[] value;

        private Pair(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}
