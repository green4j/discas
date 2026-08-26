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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a reload publishes, and -- the part with teeth -- what it refuses to publish.
 *
 * <p>A config file rewritten in place is readable throughout the rewrite: between the truncate and
 * the first byte it reads as nothing, and during the write it reads short. Neither is a revision
 * anybody wrote. Publishing one would be forgivable if it were transient, but it is not: it becomes
 * the last good value, so an edit that then fails to parse leaves the process serving the fragment
 * rather than the configuration it was serving before.
 */
@DisplayName("ReloadableFileSource -- what a reload applies, and what a rewrite must not")
class ReloadableFileSourceTest {

    @Test
    @DisplayName("A rewrite still in progress does not become the value; the finished file does")
    void aRewriteInProgressIsNotPublished(@TempDir final Path dir) throws Exception {
        // The truncate that starts an in-place rewrite leaves a complete, stable, empty file, and
        // nothing in it says whether a writer is coming. So the reload looks again before deciding,
        // and what the writer finally wrote is what it applies.
        final Path f = Files.writeString(dir.resolve("f"), "good");
        try (ReloadableFileSource<String> source = source(f)) {
            Files.writeString(f, "");
            final Thread writer = new Thread(() -> {
                try {
                    Thread.sleep(20L);
                    Files.writeString(f, "better");
                } catch (final Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            writer.start();
            final ReloadReport.Entry entry = source.reloadNow();
            writer.join();

            assertEquals(ReloadReport.Outcome.APPLIED, entry.outcome());
            assertEquals("better", source.snapshot(),
                    "The finished rewrite, never the empty file it started as");
        }
    }

    @Test
    @DisplayName("A file that stays empty is applied: it was emptied, not caught mid-write")
    void aFileEmptiedOnPurposeIsApplied(@TempDir final Path dir) throws Exception {
        // The other half of the rule above, and the reason it is a second look rather than a
        // refusal: an operator who empties a file must not be left running on the old one with
        // nothing to say so.
        final Path f = Files.writeString(dir.resolve("f"), "good");
        try (ReloadableFileSource<String> source = source(f)) {
            Files.writeString(f, "");

            assertEquals(ReloadReport.Outcome.APPLIED, source.reloadNow().outcome());
            assertEquals("", source.snapshot(), "Empty and still empty is empty on purpose");
        }
    }

    @Test
    @DisplayName("An edit that fails to parse leaves the value in force untouched")
    void aFailedParseKeepsTheLastGoodValue(@TempDir final Path dir) throws Exception {
        final Path f = Files.writeString(dir.resolve("f"), "good");
        final List<String> events = new ArrayList<>();
        try (ReloadableFileSource<String> source = strict(f, recording(events))) {
            events.clear();
            Files.writeString(f, "!");    // ... which the parser refuses

            final ReloadReport.Entry entry = source.reloadNow();
            assertEquals(ReloadReport.Outcome.FAILED, entry.outcome());
            assertEquals(List.of("failed"), events, "and it is reported, not swallowed");
            assertEquals("good", source.snapshot(), "A broken edit costs nothing that was working");
        }
    }

    @Test
    @DisplayName("A file that cannot be loaded at all fails the constructor")
    void anUnloadableFileFailsFast(@TempDir final Path dir) {
        // There is no last good value to keep serving at startup, so the only honest thing a
        // process can do with a file it cannot read is refuse to start.
        final Path missing = dir.resolve("absent");
        assertThrows(IllegalStateException.class, () -> source(missing).close(),
                "A file that is not there is not a configuration");
    }

    @Test
    @DisplayName("Every applied revision is reported, including the first")
    void everyAppliedRevisionIsReported(@TempDir final Path dir) throws Exception {
        // A value in force that was never reported is one nobody can check, and the load at startup
        // is the one an operator most wants to see: it is the whole configuration, not a delta.
        final Path f = Files.writeString(dir.resolve("f"), "one");
        final List<String> events = new ArrayList<>();
        try (ReloadableFileSource<String> source = trimming(f, recording(events))) {
            assertEquals(List.of("reloaded"), events, "The initial load is an application too");

            Files.writeString(f, "two");
            source.reloadNow();
            assertEquals(List.of("reloaded", "reloaded"), events, "And so is each one after it");
        }
    }

    @Test
    @DisplayName("A file that changed but means the same is reported as applying nothing")
    void aCosmeticEditIsReportedAsUnchanged(@TempDir final Path dir) throws Exception {
        // The question an operator is asking by calling reload is "did that take effect", and
        // "your edit changed nothing" is a different answer from both "applied" and "did not
        // parse". Silence here reads as the edit having landed.
        final Path f = Files.writeString(dir.resolve("f"), "  one  ");
        final List<String> events = new ArrayList<>();
        try (ReloadableFileSource<String> source = trimming(f, recording(events))) {
            events.clear(); // the initial load, already covered above

            Files.writeString(f, "one");         // the parser trims, so this is the same value
            assertEquals(ReloadReport.Outcome.UNCHANGED, source.reloadNow().outcome(),
                    "Different bytes, same meaning");
            assertEquals(List.of("unchanged"), events);
            assertEquals("one", source.snapshot(), "and nothing was applied");

            assertEquals(ReloadReport.Outcome.UNCHANGED, source.reloadNow().outcome(),
                    "And the same bytes are answered too, without even re-parsing");
            assertEquals(List.of("unchanged", "unchanged"), events);
        }
    }

    @Test
    @DisplayName("A summary that throws costs the report, not the reload")
    void aBrokenSummaryDoesNotCostTheReload(@TempDir final Path dir) throws Exception {
        final Path f = Files.writeString(dir.resolve("f"), "one");
        final List<String> events = new ArrayList<>();
        try (ReloadableFileSource<String> source = new ReloadableFileSource<>(List.of(f),
                contents -> new String(contents.get(0), StandardCharsets.UTF_8),
                value -> {
                    throw new IllegalStateException("no summary for you");
                }, recording(events))) {
            Files.writeString(f, "two");
            source.reloadNow();

            assertEquals("two", source.snapshot(), "The revision is in force either way");
            assertEquals(List.of("reloaded", "reloaded"), events,
                    "and it is still reported as applied: a report is not worth a configuration");
        }
    }

    @Test
    @DisplayName("One refused file holds back the whole set, and the set applies together")
    void aSetOfFilesAppliesTogetherOrNotAtAll(@TempDir final Path dir) throws Exception {
        // The reason a reload is one call rather than one per file. Two files that reference each
        // other must never be half-applied, and the way to promise that is to parse everything
        // before publishing anything.
        final ReloadableFiles registry = new ReloadableFiles();
        final Path a = Files.writeString(dir.resolve("a"), "a1");
        final Path b = Files.writeString(dir.resolve("b"), "b1");
        try (ReloadableFileSource<String> first = strict(registry, a);
                ReloadableFileSource<String> second = strict(registry, b)) {
            Files.writeString(a, "a2");
            Files.writeString(b, "!");   // the one the parser refuses

            final ReloadReport rejected = registry.reloadAll();
            assertFalse(rejected.applied(), "One refusal is the whole reload's refusal");
            assertEquals("a1", first.snapshot(), "so the file that did parse was not applied");
            assertEquals("b1", second.snapshot());
            assertEquals(List.of(ReloadReport.Outcome.NOT_APPLIED, ReloadReport.Outcome.FAILED),
                    outcomes(rejected),
                    "and the report distinguishes 'held back' from 'refused'");

            Files.writeString(b, "b2");
            final ReloadReport applied = registry.reloadAll();
            assertTrue(applied.applied(), "With nothing refused, the set goes in together");
            assertEquals("a2", first.snapshot());
            assertEquals("b2", second.snapshot());
        }
    }

    @Test
    @DisplayName("A source that is closed is no longer part of a reload")
    void aClosedSourceLeavesTheSet(@TempDir final Path dir) throws Exception {
        final ReloadableFiles registry = new ReloadableFiles();
        final Path f = Files.writeString(dir.resolve("f"), "one");
        final ReloadableFileSource<String> source = strict(registry, f);
        assertEquals(1, registry.reloadAll().entries().size());

        source.close();
        assertEquals(List.of(), registry.reloadAll().entries(),
                "What a reload covers is what the process is still serving");
    }

    private static List<ReloadReport.Outcome> outcomes(final ReloadReport report) {
        final List<ReloadReport.Outcome> outcomes = new ArrayList<>();
        for (final ReloadReport.Entry entry : report.entries()) {
            outcomes.add(entry.outcome());
        }
        return outcomes;
    }

    /**
     * Records which event fired, and not a word of what it said. The wording is for an operator to
     * read, not for a test to match: pinning it here would make every improvement to a message a
     * test failure, and would not check anything this class promises.
     */
    private static ReloadObserver recording(final List<String> events) {
        return new ReloadObserver() {
            @Override
            public void reloaded(final String source, final String detail) {
                events.add("reloaded");
            }

            @Override
            public void reloadUnchanged(final String source, final String detail) {
                events.add("unchanged");
            }

            @Override
            public void reloadFailed(final String source, final Throwable error) {
                events.add("failed");
            }
        };
    }

    /** Takes the file verbatim, so an empty file is a value like any other. */
    private static ReloadableFileSource<String> source(final Path f) {
        return new ReloadableFileSource<>(new ReloadableFiles(), List.of(f),
                contents -> new String(contents.get(0), StandardCharsets.UTF_8), null,
                ReloadObserver.NONE);
    }

    /** Trims, so whitespace is a cosmetic edit, and wraps, so a summary is visibly at work. */
    private static ReloadableFileSource<String> trimming(final Path f,
                                                         final ReloadObserver observer) {
        return new ReloadableFileSource<>(new ReloadableFiles(), List.of(f),
                contents -> new String(contents.get(0), StandardCharsets.UTF_8).trim(),
                value -> "held<" + value + ">", observer);
    }

    private static ReloadableFileSource<String> strict(final Path f,
                                                      final ReloadObserver observer) {
        return strict(new ReloadableFiles(), f, observer);
    }

    private static ReloadableFileSource<String> strict(final ReloadableFiles registry,
                                                      final Path f) {
        return strict(registry, f, ReloadObserver.NONE);
    }

    /** Parses anything but {@code !}, which stands in for the edit an operator got wrong. */
    private static ReloadableFileSource<String> strict(final ReloadableFiles registry,
                                                      final Path f,
                                                      final ReloadObserver observer) {
        return new ReloadableFileSource<>(registry, List.of(f), contents -> {
            final String text = new String(contents.get(0), StandardCharsets.UTF_8);
            if (text.equals("!")) {
                throw new IllegalArgumentException("unparseable");
            }
            return text;
        }, null, observer);
    }
}
