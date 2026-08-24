/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A task that throws on the event loop must not take the loop down with it.
 * <p>
 * One bad task cannot be allowed to stop consensus, and neither can one bad timer stop the timer
 * wheel -- an uncaught throw on the run path would end the loop thread and strand every future
 * task and every scheduled repair with no further symptom. The loop therefore catches, hands the
 * throwable to its {@code ErrorHandler}, and carries on; what is asserted here is the carrying on.
 */
@DisplayName("EventLoop -- a throwing task or timer does not stop the loop")
class EventLoopErrorReportingTest {

    /** Swallows failures so the test asserts survival, not the reporting seam. */
    private static final EventLoop.ErrorHandler IGNORE = (context, error) -> { };

    @Test
    @DisplayName("The loop keeps running after a task throws")
    void throwingTaskDoesNotStopTheLoop() throws Exception {
        final EventLoop loop = new EventLoop("errors-task", IGNORE);
        loop.start();
        try {
            loop.execute(() -> {
                throw new IllegalStateException("Boom");
            });

            // Tasks run in submission order, so this one is reached only after the throw above
            // has been caught.
            final CountDownLatch after = new CountDownLatch(1);
            loop.execute(after::countDown);
            assertTrue(after.await(5, TimeUnit.SECONDS),
                    "The loop must keep running after a task throws");
        } finally {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("Later timers still fire after one throws")
    void throwingTimerDoesNotStopTheTimerWheel() throws Exception {
        final EventLoop loop = new EventLoop("errors-timer", IGNORE);
        loop.start();
        try {
            loop.schedule(Duration.ofMillis(10), () -> {
                throw new IllegalStateException("Timer boom");
            });

            final CountDownLatch later = new CountDownLatch(1);
            loop.schedule(Duration.ofMillis(10), later::countDown);
            assertTrue(later.await(5, TimeUnit.SECONDS),
                    "One failed timer must not stop the timer wheel");
        } finally {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(2));
        }
    }
}
