/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * The one process-wide timer behind the checks that have to happen on their own: a single thread for
 * the JVM. Callers {@link #register} an interval and a check, and every check runs on that thread,
 * at least once per interval. Checks must be cheap and self-gating; exceptions are caught and
 * reported, and the registration lives on.
 *
 * <p>What belongs here is a check with nobody to trigger it -- a certificate drifting towards its
 * expiry, a secret manager that has to be asked before it will answer. Material read from files is
 * <b>not</b> among them: it is re-read when somebody asks for it, through {@link ReloadableFiles},
 * so a file can be edited in place and a set of files can be applied as a set.
 */
public final class PeriodicDaemon {

    /** Nothing registered: sleep in units of this rather than spinning or waking constantly. */
    private static final Duration IDLE_TIMEOUT = Duration.ofMinutes(1);

    private static volatile PeriodicDaemon instance;

    /** The one shared daemon (created and started lazily on first use). */
    public static PeriodicDaemon shared() {
        PeriodicDaemon local = instance;
        if (local == null) {
            synchronized (PeriodicDaemon.class) {
                local = instance;
                if (local == null) {
                    local = new PeriodicDaemon();
                    instance = local;
                }
            }
        }
        return local;
    }

    /** A handle to remove a registration; closing it detaches its check. */
    public interface Registration extends AutoCloseable {
        @Override
        void close();
    }

    private final Object lock = new Object();
    private final List<Reg> registrations = new ArrayList<>();
    private final Thread thread;

    private PeriodicDaemon() {
        this.thread = new Thread(this::run, "discas-periodic");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /**
     * Register a {@code check} to run every {@code interval}. The check runs on the shared thread;
     * keep it cheap and non-blocking. {@code source} names the registration for reporting.
     */
    public Registration register(final String source,
                                 final Duration interval,
                                 final Runnable check) {
        return register(source, interval, check, ReloadObserver.NONE);
    }

    /**
     * Register with an observer. Each registration brings its own, because the daemon is shared by
     * unrelated owners and one of them must not receive another's faults.
     */
    public Registration register(final String source,
                                 final Duration interval,
                                 final Runnable check,
                                 final ReloadObserver observer) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        final ReloadObserver regObserver = observer == null ? ReloadObserver.NONE : observer;
        final Reg reg = new Reg(check, interval.toNanos(), source, regObserver);
        synchronized (lock) {
            registrations.add(reg);
        }
        LockSupport.unpark(thread); // wake the loop to pick the new registration's deadline up
        return () -> unregister(reg);
    }

    private void unregister(final Reg reg) {
        synchronized (lock) {
            registrations.remove(reg);
        }
    }

    private void run() {
        while (true) {
            LockSupport.parkNanos(this, sleepNanos());
            runDueChecks();
        }
    }

    /** Time until the soonest-due registration, floored so an empty daemon does not spin. */
    private long sleepNanos() {
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
        // At least a millisecond, so a check that is momentarily not due cannot busy-spin the thread.
        return Math.max(TimeUnit.MILLISECONDS.toNanos(1), soonest);
    }

    private void runDueChecks() {
        final List<Reg> due = new ArrayList<>();
        final long now = System.nanoTime();
        synchronized (lock) {
            for (final Reg reg : registrations) {
                if (now - reg.nextRunNanos >= 0) {
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
        final String source;
        final ReloadObserver observer;
        long nextRunNanos;

        Reg(final Runnable check, final long intervalNanos, final String source,
                final ReloadObserver observer) {
            this.check = check;
            this.intervalNanos = intervalNanos;
            this.source = source;
            this.observer = observer;
            this.nextRunNanos = System.nanoTime() + intervalNanos;
        }
    }
}
