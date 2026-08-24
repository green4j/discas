/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import java.time.Duration;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * A single thread that runs queued tasks, timers and registered {@link IoDriver}s. Implements
 * {@link Executor}, so {@code CompletableFuture.*Async} completions can be kept on the loop
 * thread.
 * <p>
 * {@link #shutdown()} drains everything already queued before the thread exits, so callbacks
 * submitted before the call still run; {@link #awaitTermination(Duration)} waits for that.
 */
public final class EventLoop implements Executor {
    /**
     * I/O integration point for non-blocking transports.
     * <p>
     * <b>A driver never blocks.</b> It dispatches what is ready and says how much it found; the
     * loop owns all waiting. With several drivers on one loop (peer mesh, client port), a driver
     * that waited would delay every other driver's ready data by its own idle timeout.
     */
    public interface IoDriver {

        /**
         * Dispatch whatever I/O is ready right now, without waiting for more.
         *
         * @return how many events were handled; {@code 0} means idle, which is what tells the loop
         *         it may wait
         */
        int pollNow() throws Exception;
    }

    /** A scheduled timer, so the caller can {@link #cancel()} it before it fires. */
    public static final class TimerHandle {
        private final TimerEntry entry;

        TimerHandle(final TimerEntry entry) {
            this.entry = entry;
        }

        /** Stop the timer from firing again. Idempotent, and safe from any thread. */
        public void cancel() {
            entry.cancelled = true;
        }
    }

    private static final class TimerEntry implements Comparable<TimerEntry> {
        private final Runnable task;
        private final boolean repeating;
        private final long intervalNanos;
        private long deadlineNanos; // set on event loop thread before insertion into queue
        private volatile boolean cancelled;

        private TimerEntry(final Runnable task,
                   final boolean repeating,
                   final long intervalNanos) {
            this.task = task;
            this.repeating = repeating;
            this.intervalNanos = intervalNanos;
        }

        @Override
        public int compareTo(final TimerEntry other) {
            return Long.compare(deadlineNanos, other.deadlineNanos);
        }
    }

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final PriorityQueue<TimerEntry> timers = new PriorityQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread thread;
    private volatile boolean started = false;
    private final List<IoDriver> ioDrivers = new CopyOnWriteArrayList<>();

    /**
     * Idle backoff: spin, then yield, then park for a period that doubles while nothing happens.
     * Spinning and yielding keep work that arrives <em>now</em> cheap; the park ceiling is the most
     * latency the loop can add, and the only thing stopping an idle process from burning a core.
     * <p>
     * A blocking {@code select} cannot replace the park: its timeout has millisecond granularity,
     * too coarse for a timer due in microseconds, and the loop thread is the only one that can run
     * a queued task or fire a timer, so it cannot sleep until I/O arrives either.
     */
    private static final int IDLE_SPINS = 64;
    private static final int IDLE_YIELDS = 32;
    private static final long PARK_MIN_NANOS = 100_000L;          // 100us
    private static final long DEFAULT_PARK_MAX_NANOS = 1_000_000L; // 1ms

    private final long parkMaxNanos;
    private int idleRounds;
    private long parkNanos;

    /**
     * Cancelling a timer only flags it: it leaves the queue either when {@link #fireTimers} pops it
     * at its deadline and skips the run, or when the sweep in {@link #offerTimer} drops it, every
     * {@code CLEANUP_INTERVAL} insertions. Removing on cancel instead would be a linear scan of the
     * queue on every completed round, since each round holds a timer.
     */
    private int insertionsSinceCleanup = 0;
    private static final int CLEANUP_INTERVAL = 64;

    /**
     * Where a task, timer or drain failure is reported. The loop catches and continues -- one bad
     * task must not stop the rest -- so without a handler the failure would be invisible. Owners
     * with an observability seam of their own pass a handler that forwards to it.
     * <p>
     * Called on the loop thread, so an implementation must not block or throw.
     */
    @FunctionalInterface
    public interface ErrorHandler {

        /** Discards failures. Explicit silence, for an owner that reports them another way. */
        ErrorHandler NONE = (context, error) -> {
        };

        /**
         * Message plus stack trace, then carry on. The default, unlike the observer seams that
         * default to silence: a loop built with no handler and no owning observer would otherwise
         * swallow every task failure, and a process that quietly stops making progress is worse
         * than a noisy one.
         */
        ErrorHandler STDERR = new StderrErrorHandler();

        /**
         * @param context which part of the loop failed -- {@code "event loop"}, {@code "timer
         *                task"} or {@code "shutdown drain"}
         */
        void onError(String context, Throwable error);
    }

    private final ErrorHandler errorHandler;

    public EventLoop(final String name) {
        this(name, ErrorHandler.STDERR);
    }

    public EventLoop(final String name, final ErrorHandler errorHandler) {
        this(name, errorHandler, DEFAULT_PARK_MAX_NANOS);
    }

    /**
     * @param parkMaxNanos ceiling on the idle park, and therefore the most latency this loop can
     *                     add to work that arrives while it is idle. A server-side loop wants it
     *                     short; a loop embedded in someone else's application may prefer a longer
     *                     one, trading a little latency for not holding a core awake.
     */
    public EventLoop(final String name, final ErrorHandler errorHandler, final long parkMaxNanos) {
        this.errorHandler = errorHandler == null ? ErrorHandler.STDERR : errorHandler;
        if (parkMaxNanos < PARK_MIN_NANOS) {
            throw new IllegalArgumentException(
                    "parkMaxNanos must be at least " + PARK_MIN_NANOS + ", got " + parkMaxNanos);
        }
        this.parkMaxNanos = parkMaxNanos;
        thread = new Thread(this::loop, name);
        thread.setDaemon(true);
    }

    /** Start the loop thread. A second call is a no-op, so re-entrant wiring cannot crash. */
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        thread.start();
    }

    /**
     * Register a non-blocking I/O driver, such as an NIO selector. Every registered driver is
     * polled each iteration, so one loop thread can serve several sockets. Driver callbacks run on
     * the loop thread.
     */
    public void registerIoDriver(final IoDriver driver) {
        if (!ioDrivers.contains(driver)) {
            ioDrivers.add(driver);
        }
    }

    /**
     * An {@link Executor} that throws {@link RejectedExecutionException} once the loop has stopped,
     * where {@link #execute} drops the task. Use it for {@code CompletableFuture.*Async} so a
     * chained future fails during shutdown instead of hanging.
     */
    public Executor rejectingExecutor() {
        return task -> {
            if (!running.get()) {
                throw new RejectedExecutionException("EventLoop is shut down");
            }
            taskQueue.offer(task);
            LockSupport.unpark(thread);
            if (!running.get() && taskQueue.remove(task)) {
                throw new RejectedExecutionException("EventLoop is shut down");
            }
        };
    }

    /** Run {@code task} on the loop thread. Dropped if the loop has stopped. */
    @Override
    public void execute(final Runnable task) {
        if (!running.get()) {
            return;
        }
        taskQueue.offer(task);
        // The loop thread is the only one that can run this, so it is the one that has to be woken.
        LockSupport.unpark(thread);
        // Best-effort revoke. If shutdown() set running=false between
        // the check above and the offer, the loop thread may have already
        // finished drainOnShutdown. Remove the task so it doesn't sit in an
        // unconsumed queue with captured futures that would hang forever
        if (!running.get()) {
            taskQueue.remove(task);
        }
    }

    /**
     * Schedule a one-shot timer. Safe to call from any thread; the deadline is taken on the loop
     * thread, so a delay in getting there does not shorten {@code delay}.
     */
    public TimerHandle schedule(final Duration delay, final Runnable task) {
        final long delayNanos = delay.toNanos();
        final TimerEntry entry = new TimerEntry(task, false, 0L);
        execute(() -> {
            entry.deadlineNanos = System.nanoTime() + delayNanos;
            offerTimer(entry);
        });
        return new TimerHandle(entry);
    }

    /** Schedule a repeating timer. Safe to call from any thread. */
    public TimerHandle scheduleRepeat(final Duration interval, final Runnable task) {
        final long intervalNanos = interval.toNanos();
        final TimerEntry entry = new TimerEntry(task, true, intervalNanos);
        execute(() -> {
            entry.deadlineNanos = System.nanoTime() + intervalNanos;
            offerTimer(entry);
        });
        return new TimerHandle(entry);
    }

    /**
     * Stop the loop. Timers stop firing, but every task already queued is run on the loop thread
     * before it exits, so cleanup handlers submitted before this call still get to run.
     */
    public void shutdown() {
        running.set(false);
        LockSupport.unpark(thread);
        thread.interrupt();
    }

    /**
     * Block until the loop thread has terminated, drain included, or the timeout expires.
     *
     * @return true if the thread terminated within the timeout
     */
    public boolean awaitTermination(final Duration timeout) {
        try {
            thread.join(timeout.toMillis());
            return !thread.isAlive();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Whether the calling thread is the loop thread. */
    public boolean inLoop() {
        return Thread.currentThread() == thread;
    }

    /** Whether the loop is still accepting tasks. */
    public boolean isRunning() {
        return running.get();
    }

    private void offerTimer(final TimerEntry entry) {
        insertionsSinceCleanup++;
        if (insertionsSinceCleanup >= CLEANUP_INTERVAL) {
            timers.removeIf(timerEntry -> timerEntry.cancelled);
            insertionsSinceCleanup = 0;
        }
        timers.offer(entry);
    }

    private void loop() {
        while (running.get()) {
            try {
                int work = 0;
                for (int i = 0; i < ioDrivers.size(); i++) {
                    work += ioDrivers.get(i).pollNow();
                }

                Runnable task;
                int batch = 0;
                while (batch < 64 && (task = taskQueue.poll()) != null) {
                    task.run();
                    batch++;
                }
                work += batch;

                work += fireTimers();

                if (work > 0) {
                    idleRounds = 0;
                    parkNanos = 0L;
                } else {
                    idle();
                }

            } catch (final InterruptedException e) {
                // Only a shutdown-initiated interrupt breaks the loop. Re-setting the flag on a
                // spurious one would make the next poll() throw at once, spinning on a core.
                if (!running.get()) {
                    break;
                }
            } catch (final Exception e) {
                errorHandler.onError("event loop", e);
            }
        }

        // Drain remaining tasks after main loop exits so cleanup handlers run before exit
        drainOnShutdown();
    }

    /**
     * The drain is unbounded because it cannot cascade: once {@link #running} is false a task
     * running here cannot queue more work. Timers are not fired -- they are future work. The
     * interrupt flag is cleared first, since {@link #shutdown()} set it to wake the loop.
     */
    private void drainOnShutdown() {
        Thread.interrupted(); // reset interrupted flag if any
        Runnable task;
        while ((task = taskQueue.poll()) != null) {
            try {
                task.run();
            } catch (final Exception e) {
                errorHandler.onError("shutdown drain", e);
            }
        }
    }

    /**
     * The park is never longer than the wait to the next timer, so backing off cannot make one
     * late -- the loop is the only thread that can fire it.
     */
    private void idle() {
        idleRounds++;
        if (idleRounds <= IDLE_SPINS) {
            Thread.onSpinWait();
            return;
        }
        if (idleRounds <= IDLE_SPINS + IDLE_YIELDS) {
            Thread.yield();
            return;
        }
        parkNanos = parkNanos == 0L ? PARK_MIN_NANOS : Math.min(parkMaxNanos, parkNanos * 2L);
        LockSupport.parkNanos(Math.min(parkNanos, nanosToNextTimer()));
    }

    /** How long until the next timer is due; a long time when there is none. */
    private long nanosToNextTimer() {
        final TimerEntry next = timers.peek();
        if (next == null) {
            return Long.MAX_VALUE;
        }
        final long remainingNanos = next.deadlineNanos - System.nanoTime();
        return Math.max(0L, remainingNanos);
    }

    /** @return how many timers fired, so the loop can tell a busy round from an idle one */
    private int fireTimers() {
        final long now = System.nanoTime();
        int fired = 0;
        while (!timers.isEmpty() && (timers.peek().deadlineNanos - now) <= 0) {
            final TimerEntry entry = timers.poll();
            if (entry.cancelled) {
                continue;
            }
            fired++;
            try {
                entry.task.run();
            } catch (final Exception e) {
                errorHandler.onError("timer task", e);
            }
            if (entry.repeating && !entry.cancelled) {
                entry.deadlineNanos = now + entry.intervalNanos;
                timers.offer(entry);
            }
        }
        return fired;
    }
}
