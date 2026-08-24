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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What a watched file publishes, and -- the part with teeth -- what it refuses to publish.
 *
 * <p>A config file rewritten in place is readable throughout the rewrite: between the truncate and
 * the first byte it reads as nothing, and during the write it reads short. Neither is a revision
 * anybody wrote. Publishing one would be forgivable if it were transient, but it is not: it becomes
 * the last good value, so an edit that then fails to parse leaves the process serving the fragment
 * rather than the configuration it was serving before.
 */
@DisplayName("WatchedFileSource -- what a rewrite in progress must not publish")
class WatchedFileSourceTest {

    @Test
    @DisplayName("A file truncated to nothing is not published while a rewrite could still be under way")
    void truncationIsNotARevision(@TempDir final Path dir) throws Exception {
        final Path f = Files.writeString(dir.resolve("f"), "good");
        try (WatchedFileSource<String> source = source(f)) {
            assertEquals("good", source.snapshot(), "The initial load");

            Files.writeString(f, "");
            source.reloadNow();
            assertEquals("good", source.snapshot(),
                    "Zero bytes is what a rewrite looks like before it has written anything");
        }
    }

    @Test
    @DisplayName("A file that stays empty is published: it is emptied, not mid-write")
    void aFileThatStaysEmptyIsPublished(@TempDir final Path dir) throws Exception {
        // The other half of the rule above, and the reason it is a wait rather than a refusal: an
        // operator who empties a file must not be left running on the old one indefinitely with
        // nothing to say so. A truncate-then-write never stays empty for a whole interval.
        final Path f = Files.writeString(dir.resolve("f"), "good");
        try (WatchedFileSource<String> source = source(f, Duration.ofMillis(100))) {
            Files.writeString(f, "");
            source.reloadNow();
            assertEquals("good", source.snapshot(), "Not while it could still be a rewrite starting");

            Thread.sleep(150L);
            source.reloadNow();
            assertEquals("", source.snapshot(), "Empty a whole interval later is empty on purpose");
        }
    }

    @Test
    @DisplayName("An edit that fails to parse falls back to the last revision, not to a fragment of it")
    void aFailedParseFallsBackToTheLastRealRevision(@TempDir final Path dir) throws Exception {
        // The one that matters. The watcher can wake on the truncate that starts an edit, so the
        // state the edit is measured against must still be the configuration in force -- otherwise a
        // typo does not just fail to apply, it takes the running configuration with it.
        final Path f = Files.writeString(dir.resolve("f"), "good");
        try (WatchedFileSource<String> source = source(f)) {
            Files.writeString(f, "");     // the truncate half of a rewrite
            source.reloadNow();
            Files.writeString(f, "!");    // ... which the parser refuses
            source.reloadNow();

            assertEquals("good", source.snapshot(),
                    "A broken edit keeps the last good value, and the truncation was never one");
        }
    }

    private static WatchedFileSource<String> source(final Path f) {
        // An interval no test waits out, so an empty file is never taken for an intentional one.
        return source(f, Duration.ofHours(1));
    }

    /** Parses anything but {@code !}, which stands in for the edit an operator got wrong. */
    private static WatchedFileSource<String> source(final Path f, final Duration pollInterval) {
        return new WatchedFileSource<>(List.of(f), pollInterval, contents -> {
            final String text = new String(contents.get(0), StandardCharsets.UTF_8);
            if (text.equals("!")) {
                throw new IllegalArgumentException("unparseable");
            }
            return text;
        });
    }
}
