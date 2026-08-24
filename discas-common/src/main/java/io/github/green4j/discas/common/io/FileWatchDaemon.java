/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The one process-wide daemon behind all file watching: a single thread and a single
 * {@link WatchService} for the JVM. Callers {@link #register} a set of files, a poll interval and
 * a check, and every check runs on that thread.
 *
 * <p>The loop polls the watch service with a timeout equal to the time until the next-due
 * registration, so a filesystem event wakes it <em>early</em> and the timeout is the periodic
 * <em>safety poll</em>. Checks must be cheap and self-gating; exceptions are caught and reported.
 * On macOS the JDK's watch service is itself poll-based, so the timeout is the real detector there.
 */
public final class FileWatchDaemon {

    private static final Duration IDLE_TIMEOUT = Duration.ofMinutes(1);

    private static volatile FileWatchDaemon instance;

    /** The one shared daemon (created and started lazily on first use). */
    public static FileWatchDaemon shared() {
        FileWatchDaemon local = instance;
        if (local == null) {
            synchronized (FileWatchDaemon.class) {
                local = instance;
                if (local == null) {
                    local = new FileWatchDaemon();
                    instance = local;
                }
            }
        }
        return local;
    }

    /** A handle to remove a registration; closing it detaches its check and dir watches. */
    public interface Registration extends AutoCloseable {
        @Override
        void close();
    }

    private final Object lock = new Object();
    private final WatchService watchService;
    private final List<Reg> registrations = new ArrayList<>();
    private final Map<Path, DirWatch> dirs = new HashMap<>();
    private final Thread thread;

    private FileWatchDaemon() {
        try {
            this.watchService = FileSystems.getDefault().newWatchService();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create the shared WatchService", e);
        }
        this.thread = new Thread(this::run, "discas-file-watch");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /**
     * Register a {@code check} to run whenever any of {@code files} changes (via the
     * shared watcher) and at least every {@code interval} (safety poll). The check
     * runs on the shared thread; keep it cheap and non-blocking.
     */
    public Registration register(final List<Path> files,
                                 final Duration interval,
                                 final Runnable check) {
        return register(files, interval, check, ReloadObserver.NONE);
    }

    /**
     * Register with an observer. Each registration brings its own, because the daemon is shared by
     * unrelated owners and one of them must not receive another's faults.
     */
    public Registration register(final List<Path> files,
                                 final Duration interval,
                                 final Runnable check,
                                 final ReloadObserver observer) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        final Set<Path> watchDirs = new LinkedHashSet<>();
        for (final Path f : files) {
            final Path parent = f.toAbsolutePath().getParent();
            if (parent != null) {
                watchDirs.add(parent);
            }
        }
        final ReloadObserver regObserver = observer == null ? ReloadObserver.NONE : observer;
        final Reg reg = new Reg(check, interval.toNanos(), watchDirs, files.toString(), regObserver);
        synchronized (lock) {
            for (final Path dir : watchDirs) {
                retainDir(dir, regObserver);
            }
            registrations.add(reg);
        }
        thread.interrupt(); // wake the loop to pick up the new registration / dirs
        return () -> unregister(reg);
    }

    private void unregister(final Reg reg) {
        synchronized (lock) {
            if (!registrations.remove(reg)) {
                return;
            }
            for (final Path dir : reg.watchDirs) {
                releaseDir(dir);
            }
        }
        thread.interrupt();
    }

    private void retainDir(final Path dir, final ReloadObserver observer) {
        assert Thread.holdsLock(lock);

        final DirWatch existing = dirs.get(dir);
        if (existing != null) {
            existing.count++;
            return;
        }
        try {
            final WatchKey key = dir.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE);
            dirs.put(dir, new DirWatch(key));
        } catch (final Exception e) {
            // A missing/unwatchable dir is non-fatal: the safety poll still fires.
            observer.watchUnavailable(dir.toString(), e);
            dirs.put(dir, new DirWatch(null));
        }
    }

    private void releaseDir(final Path dir) {
        final DirWatch dw = dirs.get(dir);
        if (dw == null) {
            return;
        }
        if (--dw.count <= 0) {
            dirs.remove(dir);
            if (dw.key != null) {
                dw.key.cancel();
            }
        }
    }

    private void run() {
        while (true) {
            final long timeoutMs = computeTimeoutMs();
            boolean fsEvent = false;
            try {
                final WatchKey key = watchService.poll(timeoutMs, TimeUnit.MILLISECONDS);
                if (key != null) {
                    fsEvent = true;
                    drain(key);
                    WatchKey more;
                    while ((more = watchService.poll()) != null) {
                        drain(more);
                    }
                }
            } catch (final InterruptedException e) {
                // Woken by a register/unregister -- just recompute and re-loop.
                continue;
            } catch (final ClosedWatchServiceException e) {
                return;
            }
            runDueChecks(fsEvent);
        }
    }

    private static void drain(final WatchKey key) {
        key.pollEvents();
        key.reset();
    }

    private long computeTimeoutMs() {
        final long now = System.nanoTime();
        long soonest = IDLE_TIMEOUT.toNanos();
        synchronized (lock) {
            for (final Reg reg : registrations) {
                final long delay = reg.nextRunNanos - now;
                if (delay < soonest) {
                    soonest = delay;
                }
            }
        }
        if (soonest < 0) {
            soonest = 0;
        }
        // At least 1ms so poll(0) doesn't busy-spin when a check is momentarily not due.
        return Math.max(1L, soonest / 1_000_000L);
    }

    private void runDueChecks(final boolean fsEvent) {
        final List<Reg> due = new ArrayList<>();
        final long now = System.nanoTime();
        synchronized (lock) {
            for (final Reg reg : registrations) {
                if (fsEvent || now - reg.nextRunNanos >= 0) {
                    reg.nextRunNanos = now + reg.intervalNanos;
                    due.add(reg);
                }
            }
        }
        for (final Reg reg : due) {
            try {
                reg.check.run();
            } catch (final Exception e) {
                reg.observer.checkFailed(reg.source, e);
            }
        }
    }

    private static final class Reg {
        final Runnable check;
        final long intervalNanos;
        final Set<Path> watchDirs;
        final String source;
        final ReloadObserver observer;
        long nextRunNanos;

        Reg(final Runnable check, final long intervalNanos, final Set<Path> watchDirs,
                final String source, final ReloadObserver observer) {
            this.check = check;
            this.intervalNanos = intervalNanos;
            this.watchDirs = watchDirs;
            this.source = source;
            this.observer = observer;
            this.nextRunNanos = System.nanoTime() + intervalNanos;
        }
    }

    private static final class DirWatch {
        final WatchKey key;
        int count = 1;

        DirWatch(final WatchKey key) {
            this.key = key;
        }
    }
}
