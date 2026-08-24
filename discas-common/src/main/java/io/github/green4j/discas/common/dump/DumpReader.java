/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.dump;

import io.github.green4j.discas.common.KvLimits;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Reads a dump file, and only a whole one. Neither pass materialises the file -- the checksum
 * streams through a fixed chunk, the structure walks a window refilled from the channel on demand
 * -- so a dump larger than the heap still reads.
 * <p>
 * The entry count and the CRC are in the trailer, so nothing about a dump is provable until its
 * last byte: {@link #open} proves the file end to end and hands out nothing, and
 * {@link #forEachEntry} then walks entries that have already been counted. A reader that streamed
 * as it went would hand a restore half a cluster before discovering the file was truncated.
 * <p>
 * Hence a {@link Path} rather than a stream: the reader has to be able to go back to the beginning.
 */
public final class DumpReader implements Closeable {

    private static final int WINDOW_BYTES = 64 * 1024;
    private static final int VERIFY_CHUNK_BYTES = 64 * 1024;

    /** magic, format version, header length -- everything before the header block. */
    private static final int PREAMBLE_BYTES = DumpCodec.MAGIC_BYTES + Integer.BYTES + Integer.BYTES;

    /** end-of-entries sentinel, entry count, checksum. */
    private static final int TRAILER_BYTES = Integer.BYTES + Long.BYTES + Integer.BYTES;

    /** Receives one pair at a time, in the order the dump was written. */
    public interface EntryVisitor {
        /**
         * @param key   read-only view over the reader's window, valid only for this call
         * @param value read-only view over the reader's window, valid only for this call
         */
        void entry(ByteBuffer key, ByteBuffer value) throws IOException;
    }

    private final FileChannel channel;
    private final long fileSize;
    private final long entriesStart;
    private final DumpHeader header;
    private final long entryCount;

    private DumpReader(final FileChannel channel,
                       final long fileSize,
                       final long entriesStart,
                       final DumpHeader header,
                       final long entryCount) {
        this.channel = channel;
        this.fileSize = fileSize;
        this.entriesStart = entriesStart;
        this.header = header;
        this.entryCount = entryCount;
    }

    /**
     * Opens {@code file} and validates it end to end: the checksum over every byte below the
     * trailer, then the magic, the format version, the header, every entry's framing, the entry
     * count, and that the walk ends exactly where the checksum begins.
     *
     * @throws DumpFormatException if the file is not a dump, or is not all of one
     */
    public static DumpReader open(final Path file) throws IOException {
        final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        boolean handedOff = false;
        try {
            final long fileSize = channel.size();
            if (fileSize < PREAMBLE_BYTES + TRAILER_BYTES) {
                throw new DumpFormatException("Not a dump file: " + fileSize
                        + " bytes cannot hold a header and a trailer");
            }
            verifyChecksum(channel, fileSize);

            final ByteBuffer preamble = ByteBuffer.allocate(PREAMBLE_BYTES).order(ByteOrder.BIG_ENDIAN);
            channel.position(0);
            readFully(channel, preamble, "preamble");
            preamble.flip();

            final byte[] magic = new byte[DumpCodec.MAGIC_BYTES];
            preamble.get(magic);
            if (!Arrays.equals(magic, DumpCodec.MAGIC)) {
                throw new DumpFormatException("Not a dump file: bad magic");
            }
            final int formatVersion = preamble.getInt();
            if (formatVersion != DumpCodec.FORMAT_VERSION) {
                throw new DumpFormatException("Dump format version " + formatVersion
                        + " is not the supported " + DumpCodec.FORMAT_VERSION);
            }
            final int headerLength = preamble.getInt();
            if (headerLength <= 0 || headerLength > DumpCodec.MAX_HEADER_BYTES
                    || PREAMBLE_BYTES + (long) headerLength > fileSize - TRAILER_BYTES) {
                // Bounded against the file before anything is allocated: a corrupt length must not
                // be able to ask for a buffer the file could not possibly fill.
                throw new DumpFormatException("Dump header length " + headerLength
                        + " does not fit a file of " + fileSize + " bytes");
            }
            final ByteBuffer headerBlock =
                    ByteBuffer.allocate(headerLength).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, headerBlock, "header");
            headerBlock.flip();
            final DumpHeader header = DumpCodec.decodeHeader(headerBlock);

            final long entriesStart = PREAMBLE_BYTES + headerLength;
            final long entries = walk(channel, fileSize, entriesStart, null);

            final DumpReader reader =
                    new DumpReader(channel, fileSize, entriesStart, header, entries);
            handedOff = true;
            return reader;
        } finally {
            if (!handedOff) {
                channel.close();
            }
        }
    }

    /** What the dump says about itself. */
    public DumpHeader header() {
        return header;
    }

    /** Pairs in the dump, as counted while opening and agreed by the trailer. */
    public long entryCount() {
        return entryCount;
    }

    /**
     * Streams every pair to {@code visitor}, in the order they were dumped. Walks the framing
     * again as it goes, so a file that changed underneath the reader is refused rather than
     * half-applied.
     */
    public void forEachEntry(final EntryVisitor visitor) throws IOException {
        if (visitor == null) {
            throw new IllegalArgumentException("visitor is required");
        }
        walk(channel, fileSize, entriesStart, visitor);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * Streams the file through one chunk, checksumming everything below the trailer's own
     * checksum field.
     */
    private static void verifyChecksum(final FileChannel channel, final long fileSize)
            throws IOException {
        channel.position(0);
        final CRC32 crc = new CRC32();
        final ByteBuffer chunk = ByteBuffer.allocate(VERIFY_CHUNK_BYTES);
        long remaining = fileSize - Integer.BYTES;
        while (remaining > 0) {
            chunk.clear();
            if (chunk.remaining() > remaining) {
                chunk.limit((int) remaining);
            }
            final int read = channel.read(chunk);
            if (read < 0) {
                throw new DumpFormatException("Dump is truncated: " + remaining
                        + " bytes short of its trailer");
            }
            chunk.flip();
            crc.update(chunk); // bulk update, not a byte at a time
            remaining -= read;
        }

        final ByteBuffer stored = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, stored, "trailer checksum");
        stored.flip();
        if (stored.getInt() != (int) crc.getValue()) {
            throw new DumpFormatException("Dump checksum mismatch");
        }
    }

    /**
     * Walks the entry stream from {@code entriesStart} to the trailer, delivering pairs to
     * {@code visitor} when there is one, and returns the number of entries it found.
     */
    private static long walk(final FileChannel channel,
                             final long fileSize,
                             final long entriesStart,
                             final EntryVisitor visitor) throws IOException {
        final Window window = new Window(channel, fileSize, entriesStart);
        long entries = 0;
        for (;;) {
            final int keyLength = window.readInt("entry key length");
            if (keyLength == DumpCodec.END_OF_ENTRIES) {
                break;
            }
            if (keyLength < 0 || keyLength > KvLimits.MAX_KEY_BYTES) {
                throw new DumpFormatException("Dump entry " + entries + " claims a key of "
                        + keyLength + " bytes, outside 0.." + KvLimits.MAX_KEY_BYTES);
            }
            final int valueLength = window.peekIntAt(keyLength, "entry value length");
            if (valueLength < 0 || valueLength > KvLimits.MAX_VALUE_BYTES) {
                throw new DumpFormatException("Dump entry " + entries + " claims a value of "
                        + valueLength + " bytes, outside 0.." + KvLimits.MAX_VALUE_BYTES);
            }

            // Both halves are held in the window at once: the visitor is handed views over it, and
            // a refill taken between them would have moved the key out from under one.
            final int pairBytes = keyLength + Integer.BYTES + valueLength;
            window.require(pairBytes, "entry");
            if (visitor != null) {
                visitor.entry(window.sliceAt(0, keyLength),
                        window.sliceAt(keyLength + Integer.BYTES, valueLength));
            }
            window.advance(pairBytes);
            entries++;
        }

        final long declaredEntries = window.readLong("entry count");
        if (declaredEntries != entries) {
            throw new DumpFormatException("Dump trailer claims " + declaredEntries
                    + " entries, but the file holds " + entries);
        }
        if (window.consumedTo() != fileSize - Integer.BYTES) {
            // Not pedantry: a second dump appended to the first, or a file the writer was still
            // growing, both land here and neither is a file to restore from.
            throw new DumpFormatException("Dump has "
                    + (fileSize - Integer.BYTES - window.consumedTo()) + " bytes around its trailer");
        }
        return entries;
    }

    private static void readFully(final FileChannel channel,
                                  final ByteBuffer buffer,
                                  final String what) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) {
                throw new DumpFormatException("Dump is truncated: "
                        + buffer.remaining() + " bytes short of its " + what);
            }
        }
    }

    /**
     * A sliding window over the entry stream: unread bytes are compacted to the front and the rest
     * refilled from the channel on demand, growing only when one entry needs more than it holds.
     */
    private static final class Window {
        private final FileChannel channel;
        private final long fileSize;
        private long readOffset;
        private ByteBuffer window;

        private Window(final FileChannel channel,
                       final long fileSize,
                       final long startOffset) throws IOException {
            this.channel = channel;
            this.fileSize = fileSize;
            this.readOffset = startOffset;
            this.window = ByteBuffer.allocate(WINDOW_BYTES).order(ByteOrder.BIG_ENDIAN);
            this.window.limit(0);
            channel.position(startOffset);
        }

        /** File offset of the next unread byte. */
        private long consumedTo() {
            return readOffset - window.remaining();
        }

        private int readInt(final String what) throws IOException {
            require(Integer.BYTES, what);
            return window.getInt();
        }

        private long readLong(final String what) throws IOException {
            require(Long.BYTES, what);
            return window.getLong();
        }

        /** Reads an int {@code offset} bytes ahead without consuming anything before it. */
        private int peekIntAt(final int offset, final String what) throws IOException {
            require(offset + Integer.BYTES, what);
            return window.getInt(window.position() + offset);
        }

        private ByteBuffer sliceAt(final int offset, final int length) {
            final ByteBuffer slice = window.duplicate();
            slice.position(window.position() + offset);
            slice.limit(window.position() + offset + length);
            return slice.slice().asReadOnlyBuffer();
        }

        private void advance(final int bytes) {
            window.position(window.position() + bytes);
        }

        private void require(final int bytes, final String what) throws IOException {
            if (consumedTo() + bytes > fileSize) {
                // Checked against the file before the window is grown, so a corrupt length cannot
                // ask for a buffer the file could not fill.
                throw new DumpFormatException("Dump is truncated inside its " + what
                        + ": wanted " + bytes + " bytes at offset " + consumedTo());
            }
            while (window.remaining() < bytes) {
                if (!refill(bytes)) {
                    throw new DumpFormatException("Dump is truncated inside its " + what);
                }
            }
        }

        private boolean refill(final int needed) throws IOException {
            window.compact();
            if (window.capacity() < needed) {
                final ByteBuffer bigger = ByteBuffer.allocate(Math.max(window.capacity() * 2, needed))
                        .order(ByteOrder.BIG_ENDIAN);
                window.flip();
                bigger.put(window);
                window = bigger;
            }
            boolean progress = false;
            while (readOffset < fileSize && window.hasRemaining()) {
                final int read = channel.read(window);
                if (read < 0) {
                    break;
                }
                readOffset += read;
                progress = true;
            }
            window.flip();
            return progress;
        }
    }
}
