/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.operator;

import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.logging.LogThrottle;
import io.github.green4j.discas.common.metrics.MetricRegistry;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * What this process currently needs an operator to do, and the single place all three surfaces come
 * from: the log line, the metric sample, and -- through {@link OperatorState#action()} -- the row in
 * the operating documentation.
 * <p>
 * The register holds the {@link OperatorState}s that are <b>raised</b>. A raise is scoped: a peer
 * id, a file, a component, or empty for a process-wide condition, so the same state can be raised
 * about two peers at once and cleared about one of them.
 * <p>
 * <b>Nothing is reported until it is due.</b> A raise starts a clock; the state becomes due once it
 * has been raised for {@link OperatorState#normalFor()}, and only then is a line written and a
 * sample exposed. A condition that clears inside its window was an ordinary moment in a healthy
 * cluster and says nothing at all -- which is what makes a rolling restart quiet and a member that
 * is genuinely gone loud, without needing a log level between "act" and "do not act".
 * <p>
 * <b>Some states never clear.</b> A degraded WAL or a dropped unaccounted key cannot be un-happened,
 * and no event in this process can say it has been dealt with; those simply stay raised, and a
 * restart is the acknowledgement. That is honest rather than sticky: the alternative is a surface
 * that reports a fault has gone away because nothing mentioned it again.
 * <p>
 * <b>Threading.</b> Raises arrive from an event loop, from a file-watch thread, and from a client's
 * transport; scrapes arrive from the observability server's thread. Everything here is therefore on
 * a concurrent map and no lock is taken. {@link #checkDue()} is expected on one thread -- a repeating
 * timer on the owning loop -- but is safe anywhere.
 *
 * @see OperatorState
 */
public final class OperatorAttention {

    /** How often the owner is expected to call {@link #checkDue()}; also the resolution of a window. */
    public static final Duration CHECK_INTERVAL = Duration.ofSeconds(1);

    /** The one family, so an alert is one rule rather than one per condition. */
    private static final String METRIC = "discas_operator_attention";
    private static final String METRIC_HELP =
            "States that need an operator to act, one sample each while raised past its window. "
                    + "Alert on this being present at all: discas_operator_attention > 0.";

    private final Log log;
    private final LongSupplier nanoClock;
    private final Map<Key, Raised> raised = new ConcurrentHashMap<>();

    /**
     * A raised state is reported once, so the storm shape is not repetition but a condition that
     * <em>flaps</em>: a peer reconnecting around the clock-skew bound produces a raise and a clear
     * per cycle. Two throttles rather than one, so a flapping condition cannot bury the arrival of
     * a different, steady state.
     */
    private final LogThrottle raiseThrottle = new LogThrottle();
    private final LogThrottle clearThrottle = new LogThrottle();

    public OperatorAttention(final Log log) {
        this(log, System::nanoTime);
    }

    /** The clock is injectable so a test can drive a window without waiting one out. */
    OperatorAttention(final Log log, final LongSupplier nanoClock) {
        this.log = log;
        this.nanoClock = nanoClock;
    }

    /**
     * Raises {@code state} about {@code scope}, or does nothing if it is already raised there.
     * <p>
     * Re-raising neither restarts the window nor replaces the detail: the window is about how long
     * the condition has been true, and the first occurrence is the one worth a trace. How often it
     * recurs is a rate, and rates belong in the counters.
     *
     * @param scope  which peer, file or component, or {@code null}/empty for the whole process
     * @param detail one short line of what happened, in the past tense; the action comes from the
     *               state
     */
    public void raise(final OperatorState state, final String scope, final String detail) {
        raise(state, scope, detail, null);
    }

    /** As {@link #raise(OperatorState, String, String)}, with a throwable to log when it is due. */
    public void raise(final OperatorState state, final String scope, final String detail,
                      final Throwable error) {
        raised.computeIfAbsent(new Key(state, normalise(scope)),
                key -> new Raised(nanoClock.getAsLong(), detail, error));
        // Zero-window states are due the moment they are raised, and waiting up to a second to say
        // so would make the log a worse record of when something happened than the metric is.
        if (state.normalFor().isZero()) {
            checkDue();
        }
    }

    /**
     * Clears {@code state} for {@code scope}. Writes a line only if the state had become due --
     * nothing announces the end of something that was never announced.
     */
    public void clear(final OperatorState state, final String scope) {
        remove(new Key(state, normalise(scope)));
    }

    /**
     * Clears every state raised about {@code scope}: what a peer completing a handshake does to
     * whatever kept it out. Process-wide states (empty scope) are never touched by this.
     */
    public void clearScope(final String scope) {
        final String normalised = normalise(scope);
        if (normalised.isEmpty()) {
            return;
        }
        for (final Key key : raised.keySet()) {
            if (key.scope.equals(normalised)) {
                remove(key);
            }
        }
    }

    /**
     * Writes the line for every state that has now been raised for its window. Called from a
     * repeating timer at {@link #CHECK_INTERVAL}; a state with a zero window does not wait for it.
     */
    public void checkDue() {
        final long now = nanoClock.getAsLong();
        for (final Map.Entry<Key, Raised> entry : raised.entrySet()) {
            final Key key = entry.getKey();
            final Raised state = entry.getValue();
            if (state.reported || !isDue(key.state, state, now)) {
                continue;
            }
            // The flag is what stops a second line on the next tick; a lost race here would repeat
            // one line, which is cheaper than the lock that would prevent it.
            state.reported = true;
            final String line = prefix(key) + state.detail
                    + elapsedSuffix(key.state, state, now)
                    + " Action: " + key.state.action();
            if (state.error != null) {
                raiseThrottle.error(log, line, state.error);
            } else {
                raiseThrottle.error(log, line);
            }
        }
    }

    /** Whether {@code state} is raised and due for {@code scope} -- what the metric family exposes. */
    public boolean isDue(final OperatorState state, final String scope) {
        final Raised entry = raised.get(new Key(state, normalise(scope)));
        return entry != null && isDue(state, entry, nanoClock.getAsLong());
    }

    /**
     * Registers the one family, enumerated at scrape time rather than as a gauge per state: the
     * set of raised states varies, and a gauge each would mean writing to the registry from an
     * event loop every time a peer went away. Cardinality is bounded by a frozen {@code N} times a
     * fixed enum.
     */
    public void registerMetrics(final MetricRegistry registry) {
        registry.register(sink -> {
            final long now = nanoClock.getAsLong();
            for (final Map.Entry<Key, Raised> entry : raised.entrySet()) {
                final Key key = entry.getKey();
                if (!isDue(key.state, entry.getValue(), now)) {
                    continue;
                }
                sink.gauge(METRIC, METRIC_HELP, 1L,
                        "group", key.state.group().name(),
                        "state", key.state.name(),
                        "scope", key.scope);
            }
        });
    }

    private void remove(final Key key) {
        final Raised entry = raised.remove(key);
        if (entry != null && entry.reported) {
            clearThrottle.info(log, prefix(key) + "cleared after "
                    + elapsed(nanoClock.getAsLong() - entry.raisedAtNanos) + ".");
        }
    }

    private boolean isDue(final OperatorState state, final Raised entry, final long now) {
        return now - entry.raisedAtNanos >= state.normalFor().toNanos();
    }

    private static String prefix(final Key key) {
        return "[" + key.state.group().name() + "/" + key.state.name()
                + (key.scope.isEmpty() ? "" : " " + key.scope) + "] ";
    }

    /**
     * A windowed state says how long it has been true, because that is the whole reason it is being
     * reported now and was not a minute ago. A zero-window state says nothing: it is being reported
     * because it just happened.
     */
    private static String elapsedSuffix(final OperatorState state, final Raised entry,
                                        final long now) {
        if (state.normalFor().isZero()) {
            return ".";
        }
        return ", for " + elapsed(now - entry.raisedAtNanos) + ".";
    }

    private static String elapsed(final long nanos) {
        final long seconds = TimeUnit.NANOSECONDS.toSeconds(nanos);
        if (seconds < 90) {
            return seconds + "s";
        }
        final long minutes = seconds / 60;
        return minutes < 90 ? minutes + "m" : (minutes / 60) + "h";
    }

    private static String normalise(final String scope) {
        return scope == null ? "" : scope;
    }

    private static final class Key {
        private final OperatorState state;
        private final String scope;

        private Key(final OperatorState state, final String scope) {
            this.state = state;
            this.scope = scope;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Key)) {
                return false;
            }
            final Key that = (Key) other;
            return state == that.state && scope.equals(that.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, scope);
        }
    }

    private static final class Raised {
        private final long raisedAtNanos;
        private final String detail;
        private final Throwable error;
        /** Written once the line has been logged, so a repeating check does not repeat it. */
        private volatile boolean reported;

        private Raised(final long raisedAtNanos, final String detail, final Throwable error) {
            this.raisedAtNanos = raisedAtNanos;
            this.detail = detail;
            this.error = error;
        }
    }
}
