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
 * same value is not re-published.
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
    private final WatchedFile watched;
    private final CopyOnWriteArrayList<Consumer<T>> listeners = new CopyOnWriteArrayList<>();

    private final ReloadObserver observer;

    private volatile String lastFingerprint;
    private volatile T current;

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

    /** WatchedFile onChange: read, gate on fingerprint then value, and publish if new. */
    private synchronized void onFilesChanged() {
        final List<byte[]> contents = readAll();
        if (contents == null) {
            return; // a file was transiently unreadable (mid-write) -- retried on the next check
        }
        final String fingerprint = fingerprint(contents);
        if (fingerprint.equals(lastFingerprint)) {
            return; // byte-identical content: nothing new (skip the re-parse)
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
            return; // keep the last good value
        }
        lastFingerprint = fingerprint;
        if (candidate.equals(current)) {
            return; // value gate: content changed but parses to the same value -- no publish
        }
        current = candidate; // atomic publish
        for (final Consumer<T> listener : listeners) {
            listener.accept(candidate);
        }
    }

    /** Read every file's bytes; {@code null} if any is transiently unreadable. */
    private List<byte[]> readAll() {
        final List<byte[]> contents = new ArrayList<>(files.size());
        for (final Path file : files) {
            try {
                contents.add(Files.readAllBytes(file));
            } catch (final Exception e) {
                return null;
            }
        }
        return contents;
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
