/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import io.github.green4j.discas.common.Hex;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A file-backed {@link Reloadable}: watches a set of files on the shared
 * {@link FileWatchDaemon} and publishes an immutable value parsed from their contents, with
 * <b>replay-on-subscribe</b>. Everything specific to one kind of file lives in its {@link Parser}.
 *
 * <p>Change is detected in three gates, cheapest first: the {@link WatchedFile}'s mtime-and-size
 * signature, then a combined SHA-256 of the contents, so byte-identical files are not even
 * re-parsed, then {@code equals} against the current value, so a cosmetic edit that parses to the
 * same value is not re-published. Ahead of all three, a file caught mid-write is not read as a
 * revision at all -- see {@link #readAll()}.
 *
 * <p>The constructor primes the value from the first check -- the same read that sets the
 * change-detection baseline, so no later change can be missed -- and <b>fails fast</b> if nothing
 * loads or the initial parse throws. A later reload that fails keeps the last good value. Owns no
 * thread of its own.
 */
public final class WatchedFileSource<T> implements Reloadable<T>, AutoCloseable {

    /** Parses the raw contents of the watched files (same order as supplied) into a value. */
    @FunctionalInterface
    public interface Parser<T> {
        T parse(List<byte[]> contents) throws Exception;
    }

    private final List<Path> files;
    private final Parser<T> parser;
    private final long quietNanos;
    private final WatchedFile watched;
    private final CopyOnWriteArrayList<Consumer<T>> listeners = new CopyOnWriteArrayList<>();

    private final ReloadObserver observer;

    private volatile String lastFingerprint;
    private volatile T current;
    /** Whether the last read found the files empty, and when they first were; see readAll(). */
    private boolean empty;
    private long emptySinceNanos;

    public WatchedFileSource(final List<Path> files,
                             final Duration pollInterval,
                             final Parser<T> parser) {
        this(files, pollInterval, parser, ReloadObserver.NONE);
    }

    /**
     * @param observer reports a reload that failed to parse. Worth wiring: this class keeps
     *                 serving its last good value on a bad reload, so without it a file that has
     *                 been broken for hours looks exactly like one nobody has edited.
     */
    public WatchedFileSource(final List<Path> files,
                             final Duration pollInterval,
                             final Parser<T> parser,
                             final ReloadObserver observer) {
        this.observer = observer == null ? ReloadObserver.NONE : observer;
        this.files = List.copyOf(files);
        this.parser = parser;
        this.quietNanos = pollInterval.toNanos();
        this.watched = new WatchedFile(this.files, pollInterval, this::onFilesChanged, this.observer);
        // Initial load = the WatchedFile's first check (sole reader): it loads the current
        // value and sets the change-detection baseline from the same read, so no change
        // after this point can be missed. Fail fast if it can't load.
        this.watched.checkNow();
        if (current == null) {
            throw new IllegalStateException("Cannot load initial content from " + this.files);
        }
        this.watched.start();
    }

    @Override
    public T snapshot() {
        return current;
    }

    @Override
    public synchronized void addListener(final Consumer<T> listener) {
        // Replay-on-subscribe: hand the new listener the current value as its first update,
        // atomically with reloads (both take this monitor), so nothing is missed between
        // the consumer's read and its subscription.
        listener.accept(current);
        listeners.add(listener);
    }

    /** Force an immediate check-and-reload (e.g. on an ops signal or in tests). */
    public void reloadNow() {
        watched.checkNow();
    }

    @Override
    public void close() {
        watched.close();
    }

    /**
     * WatchedFile onChange: read, gate on fingerprint then value, and publish if new.
     *
     * @return false if nothing could be read yet, so the same change is offered again
     */
    private synchronized boolean onFilesChanged() {
        final List<byte[]> contents = readAll();
        if (contents == null) {
            return false; // a file was caught mid-write -- retried on the next check
        }
        final String fingerprint = fingerprint(contents);
        if (fingerprint.equals(lastFingerprint)) {
            return true; // byte-identical content: nothing new (skip the re-parse)
        }
        final T candidate;
        try {
            candidate = parser.parse(contents);
        } catch (final Exception e) {
            if (current == null) {
                // Initial load: fail fast (propagates out of the constructor's checkNow()).
                throw new RuntimeException("Failed to parse initial content from " + files, e);
            }
            observer.reloadFailed(files.toString(), e);
            return true; // read and rejected: consumed, and keep the last good value
        }
        lastFingerprint = fingerprint;
        if (candidate.equals(current)) {
            return true; // value gate: content changed but parses to the same value -- no publish
        }
        current = candidate; // atomic publish
        for (final Consumer<T> listener : listeners) {
            listener.accept(candidate);
        }
        return true;
    }

    /**
     * Read every file's bytes; {@code null} if any of them is not readable as a finished file yet.
     *
     * <p>A file being rewritten in place is readable long before it is complete: an in-progress
     * write reads short, and between the truncate and the first byte it reads as nothing at all.
     * Neither is a revision anybody wrote, and publishing one is not merely a transient wrong
     * answer -- it becomes the last good value, so an edit that ends up not parsing leaves the
     * process serving a half file forever. So each file is read between two agreeing readings of
     * its (mtime, size), and the bytes must be as long as the size says.
     *
     * <p>That leaves the truncated-to-nothing case, which reads as a perfectly stable empty file for
     * as long as the writer takes to get going. Nothing in the file can distinguish that from one an
     * operator meant to empty, so the distinction is time: an empty read is accepted only once the
     * files have been empty for a whole poll interval. A truncate-then-write is over in microseconds
     * and never survives that; emptying a file for real costs one interval to take effect.
     */
    private List<byte[]> readAll() {
        final List<byte[]> contents = new ArrayList<>(files.size());
        long total = 0;
        for (final Path file : files) {
            final byte[] bytes = readStable(file);
            if (bytes == null) {
                return null;
            }
            contents.add(bytes);
            total += bytes.length;
        }
        if (total > 0 || current == null) {
            empty = false;
            return contents;
        }
        final long now = System.nanoTime();
        if (!empty) {
            empty = true;
            emptySinceNanos = now;
        }
        return now - emptySinceNanos >= quietNanos ? contents : null;
    }

    /** A file's bytes, or {@code null} if it changed under the read (so the bytes are a fragment). */
    private static byte[] readStable(final Path file) {
        try {
            final BasicFileAttributes before = Files.readAttributes(file, BasicFileAttributes.class);
            final byte[] bytes = Files.readAllBytes(file);
            final BasicFileAttributes after = Files.readAttributes(file, BasicFileAttributes.class);
            if (before.size() != after.size()
                    || !before.lastModifiedTime().equals(after.lastModifiedTime())
                    || bytes.length != after.size()) {
                return null;
            }
            return bytes;
        } catch (final Exception e) {
            return null; // missing, or unreadable for now
        }
    }

    private static String fingerprint(final List<byte[]> contents) {
        final StringBuilder sb = new StringBuilder();
        for (final byte[] bytes : contents) {
            if (sb.length() > 0) {
                sb.append(':');
            }
            sb.append(sha256(bytes));
        }
        return sb.toString();
    }

    private static String sha256(final byte[] bytes) {
        try {
            return Hex.encode(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (final Exception e) {
            throw new RuntimeException("SHA-256 unavailable", e);
        }
    }
}
