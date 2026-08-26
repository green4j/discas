/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What one reload did, source by source: the answer to the only question an operator asks after
 * editing a file, which is whether what they wrote is now in force.
 *
 * <p>Every source the process reads appears here, including the ones that had nothing to say -- a
 * source missing from the report would be indistinguishable from one nobody looked at. Since a
 * reload applies the whole set or none of it (see {@link ReloadableFiles}), a report where
 * {@link #applied()} is false says that <b>nothing</b> changed, and the entries say which source
 * refused and why.
 */
public final class ReloadReport {

    /** What became of one source in a reload. */
    public enum Outcome {

        /** Read, parsed, differed from the value in force, and is now the value in force. */
        APPLIED("applied"),

        /**
         * Read and parsed, and there was nothing to apply: either byte-identical to what is in
         * force, or different bytes that say the same thing (whitespace, comments, reordered
         * lines). Not an error and not a change -- the answer "your edit changed nothing", which
         * usually means the wrong copy was edited.
         */
        UNCHANGED("unchanged"),

        /** Read but refused: it did not parse, and the value in force was left alone. */
        FAILED("failed"),

        /**
         * Could not be read as a finished file -- caught between the truncate and the first byte of
         * a rewrite, or changing under the read. Nothing is known about its contents, so nothing
         * was decided about them; reloading again once the writer has finished is the whole remedy.
         */
        UNREADABLE("unreadable"),

        /**
         * Read, parsed and ready to be applied -- and not applied, because another source in the
         * same reload was refused. Its own file is fine; it is being held back rather than rejected.
         */
        NOT_APPLIED("not-applied");

        private final String label;

        Outcome(final String label) {
            this.label = label;
        }

        /** The lower-case name this outcome is reported under. */
        public String label() {
            return label;
        }

        /** Whether the source raised no objection: it applied, or had nothing to apply. */
        public boolean accepted() {
            return this == APPLIED || this == UNCHANGED;
        }
    }

    /** One source's line in the report. */
    public static final class Entry {

        private final String source;
        private final Outcome outcome;
        private final String detail;

        public Entry(final String source, final Outcome outcome, final String detail) {
            this.source = source;
            this.outcome = outcome;
            this.detail = detail;
        }

        /** Which files this line is about. */
        public String source() {
            return source;
        }

        public Outcome outcome() {
            return outcome;
        }

        /**
         * One line an operator reads to recognise their own edit -- which clients, which nodes, how
         * many records, or why it was refused. Never the material that makes a file worth
         * protecting: no token, no hash or salt, no key or password.
         */
        public String detail() {
            return detail;
        }

        @Override
        public String toString() {
            return source + ": " + outcome.label() + " -- " + detail;
        }
    }

    private final List<Entry> entries;

    /** Copies {@code entries}: a report is a record of something that already happened. */
    public ReloadReport(final List<Entry> entries) {
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /** One line per source read by this process, in the order the sources were registered. */
    public List<Entry> entries() {
        return entries;
    }

    /**
     * Whether the reload was applied. False when any source was refused, in which case no source
     * was applied at all. A process with nothing to reload reports true: there was nothing to
     * refuse.
     */
    public boolean applied() {
        for (final Entry entry : entries) {
            if (!entry.outcome().accepted()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(applied() ? "applied" : "rejected");
        for (final Entry entry : entries) {
            sb.append("; ").append(entry);
        }
        return sb.toString();
    }
}
