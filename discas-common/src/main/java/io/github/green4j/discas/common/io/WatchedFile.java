/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An ergonomic per-owner view over the shared {@link FileWatchDaemon}: it owns no
 * thread of its own. It holds a set of files and a cheap change-detection
 * <b>signature</b> (last-modified time + size); on each check it runs
 * {@code onChange} only when the signature changed. The <em>content gate</em> (e.g.
 * re-parse-and-compare) belongs to {@code onChange} in the owner.
 *
 * <p>The baseline starts <b>unseen</b>, so the <b>first</b> check fires and sets the
 * signature from that same read. This makes the owner's initial load and its watching
 * a single reader with no gap: the first {@code onChange} is the "first update", and
 * every change after it is detected (no update can be missed after the first load).
 *
 * <p>{@link #start()} registers the check with the single shared daemon;
 * {@link #checkNow()} runs it synchronously on the caller's thread (for the initial
 * load / {@code reloadNow()} / tests); {@link #close()} detaches it.
 */
public final class WatchedFile implements AutoCloseable {

    /** How often a hot-reloaded file is checked, unless its owner says otherwise. */
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);

    private final List<Path> files;
    private final Duration interval;
    private final Runnable onChange;
    private final ReloadObserver observer;
    private final Map<Path, long[]> signature = new HashMap<>(); // path -> [lastModifiedMs, size]

    private FileWatchDaemon.Registration registration;

    public WatchedFile(final List<Path> files,
                       final Duration interval,
                       final Runnable onChange) {
        this(files, interval, onChange, ReloadObserver.NONE);
    }

    public WatchedFile(final List<Path> files,
                       final Duration interval,
                       final Runnable onChange,
                       final ReloadObserver observer) {
        this.files = List.copyOf(files);
        this.interval = interval;
        this.onChange = onChange;
        this.observer = observer == null ? ReloadObserver.NONE : observer;
        // Baseline left unseen (empty): the first check() fires and produces the
        // initial value while setting the baseline from that same read.
    }

    /** Begin servicing this file on the shared daemon thread. A second call is a no-op. */
    public synchronized void start() {
        if (registration != null) {
            return;
        }
        registration = FileWatchDaemon.shared().register(files, interval, this::check, observer);
    }

    /** Run the signature gate (and {@code onChange} if changed) now, on this thread. */
    public synchronized void checkNow() {
        check();
    }

    @Override
    public synchronized void close() {
        if (registration != null) {
            registration.close();
            registration = null;
        }
    }

    private synchronized void check() {
        if (signatureChanged()) {
            onChange.run();
        }
    }

    /** Recompute signatures; update stored ones; return whether any file changed. */
    private boolean signatureChanged() {
        boolean changed = false;
        for (final Path f : files) {
            final long[] cur = readSignature(f);
            if (cur == null) {
                continue; // transiently missing -- keep the last known signature
            }
            final long[] prev = signature.get(f);
            if (prev == null || prev[0] != cur[0] || prev[1] != cur[1]) {
                signature.put(f, cur);
                changed = true;
            }
        }
        return changed;
    }

    private static long[] readSignature(final Path file) {
        try {
            final BasicFileAttributes a = Files.readAttributes(file, BasicFileAttributes.class);
            return new long[]{a.lastModifiedTime().toMillis(), a.size()};
        } catch (final Exception e) {
            return null;
        }
    }
}
