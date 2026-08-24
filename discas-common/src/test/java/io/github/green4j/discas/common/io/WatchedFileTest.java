/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("WatchedFile -- signature gate over the shared FileWatchDaemon")
class WatchedFileTest {

    @Test
    @DisplayName("First check fires (initial load); then only on change (checkNow)")
    void firstCheckLoadsThenFiresOnChange(@TempDir final Path dir) throws Exception {
        final Path f = Files.writeString(dir.resolve("f"), "a");
        final AtomicInteger count = new AtomicInteger();
        final WatchedFile wf = new WatchedFile(List.of(f), Duration.ofSeconds(1), count::incrementAndGet);

        wf.checkNow();
        assertEquals(1, count.get(), "First check fires (the initial load)");

        wf.checkNow();
        assertEquals(1, count.get(), "No fire without a change");

        Thread.sleep(10L);
        Files.writeString(f, "bb");
        wf.checkNow();
        assertEquals(2, count.get(), "A change fires");

        wf.checkNow();
        assertEquals(2, count.get(), "No further fire without a change");
    }

    @Test
    @DisplayName("A change to either of two watched files fires")
    void anyOfTwoFilesFires(@TempDir final Path dir) throws Exception {
        final Path a = Files.writeString(dir.resolve("a"), "1");
        final Path b = Files.writeString(dir.resolve("b"), "2");
        final AtomicInteger count = new AtomicInteger();
        final WatchedFile wf = new WatchedFile(List.of(a, b), Duration.ofSeconds(1), count::incrementAndGet);

        wf.checkNow();
        assertEquals(1, count.get(), "First check fires (initial)");

        Thread.sleep(10L);
        Files.writeString(b, "22");
        wf.checkNow();
        assertEquals(2, count.get(), "Change to the second file fires");
    }

    @Test
    @DisplayName("A transiently-missing file is tolerated; appearance fires")
    void missingFileTolerated(@TempDir final Path dir) throws Exception {
        final Path f = dir.resolve("later");           // does not exist yet
        final AtomicInteger count = new AtomicInteger();
        final WatchedFile wf = new WatchedFile(List.of(f), Duration.ofSeconds(1), count::incrementAndGet);

        wf.checkNow();
        assertEquals(0, count.get(), "Missing file does not fire");

        Files.writeString(f, "now here");
        wf.checkNow();
        assertEquals(1, count.get(), "The file appearing fires");
    }

    @Test
    @DisplayName("The shared daemon services a registration (watcher/poll path)")
    void daemonServicesRegistration(@TempDir final Path dir) throws Exception {
        final Path f = Files.writeString(dir.resolve("f"), "a");
        final AtomicInteger count = new AtomicInteger();
        try (WatchedFile wf = new WatchedFile(List.of(f), Duration.ofMillis(50), count::incrementAndGet)) {
            wf.checkNow();                 // consume the initial fire synchronously
            assertEquals(1, count.get(), "Initial load fired");
            wf.start();
            Thread.sleep(200L);
            assertEquals(1, count.get(), "No further fire while unchanged");

            Thread.sleep(10L);
            Files.writeString(f, "changed");

            final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deadline && count.get() < 2) {
                Thread.sleep(20L);
            }
            assertTrue(count.get() >= 2, "The shared daemon applied the change without checkNow()");
        }
    }

    @Test
    @DisplayName("All registrations share exactly one daemon thread")
    void singleDaemonThread(@TempDir final Path dir) throws Exception {
        final Path a = Files.writeString(dir.resolve("a"), "1");
        final Path b = Files.writeString(dir.resolve("b"), "2");
        try (WatchedFile wa = new WatchedFile(List.of(a), Duration.ofSeconds(1), () -> { });
                WatchedFile wb = new WatchedFile(List.of(b), Duration.ofSeconds(1), () -> { })) {
            wa.start();
            wb.start();
            final long threads = Thread.getAllStackTraces().keySet().stream()
                    .filter(t -> "discas-file-watch".equals(t.getName()))
                    .count();
            assertEquals(1L, threads, "Exactly one shared file-watch thread in the process");
        }
    }
}
