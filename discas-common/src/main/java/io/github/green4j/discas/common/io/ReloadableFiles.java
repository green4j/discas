/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Every file-backed value the process is serving, and the one call that re-reads all of them.
 *
 * <p>A file is read when somebody asks for it to be read, and at no other time. That is the whole
 * contract, and both of its halves are the point:
 * <ul>
 *   <li>A file can be edited in place. Nothing looks at it between the moment an editor truncates
 *       it and the moment it is saved, so an editor that saves on a timer -- or a half-finished
 *       ACL -- cannot be read as a revision, because nobody is reading.</li>
 *   <li>A set of files applies as a set. {@link #reloadAll()} reads and parses <b>everything</b>
 *       before it publishes <b>anything</b>: if one source refuses, none is applied, and the value
 *       in force is exactly what it was. A certificate and its key, or a members list and the ACL
 *       that references it, therefore change together or not at all.</li>
 * </ul>
 *
 * <p>The registry is process-wide, like the values it holds: "reload the files this process is
 * using" is one question with one answer, and a node's ACL and its TLS material are no more
 * separable to an operator than two halves of one config directory. Sources register themselves
 * when they are constructed and unregister when they are closed, so what a reload covers is
 * whatever the process is actually serving at that moment -- never a stale list.
 *
 * <p>Reached through {@code DisCasNode.reloadFiles()} and {@code DisCasClient.reloadFiles()} rather
 * than directly, so a caller asks the thing it already holds.
 */
public final class ReloadableFiles {

    /**
     * One file-backed value, from the registry's side: a two-phase reload, so that a set of files
     * can be applied atomically. {@link #prepare()} does all the work that can fail -- read, hash,
     * parse, compare -- and publishes nothing; exactly one of {@link #commit()} or
     * {@link #discard()} follows.
     */
    public interface Source {

        /** Which files this source reads, for the report. */
        String source();

        /**
         * Read, validate and hold the result, publishing nothing. Must not throw: a source that
         * cannot read or parse its files says so in the returned entry.
         */
        ReloadReport.Entry prepare();

        /** Publish what {@link #prepare()} held, if anything. Must not throw. */
        void commit();

        /** Drop what {@link #prepare()} held, publishing nothing. Must not throw. */
        void discard();
    }

    /** A handle that detaches its source from the registry when closed. */
    public interface Registration extends AutoCloseable {
        @Override
        void close();
    }

    private static final ReloadableFiles SHARED = new ReloadableFiles();

    /** The registry every source in this process registers with. */
    public static ReloadableFiles shared() {
        return SHARED;
    }

    // Copy-on-write: registration happens a handful of times at startup, iteration happens on
    // every reload, and a reload must not be able to see the list half-built.
    private final CopyOnWriteArrayList<Source> sources = new CopyOnWriteArrayList<>();

    ReloadableFiles() {
    }

    /** Add {@code source} to what a reload covers. Close the returned handle to remove it. */
    public Registration register(final Source source) {
        sources.add(source);
        return () -> sources.remove(source);
    }

    /**
     * Re-read every registered source and apply the result, all of it or none of it.
     *
     * <p>Synchronized, so two callers cannot interleave their phases: with one reload preparing
     * while another commits, "all or none" would hold for neither. Runs on the calling thread and
     * publishes on it too -- consumers that touch single-threaded state already marshal onto their
     * own thread, as {@link Reloadable} requires of them.
     */
    public synchronized ReloadReport reloadAll() {
        final List<Source> current = new ArrayList<>(sources);
        final List<ReloadReport.Entry> entries = new ArrayList<>(current.size());
        boolean refused = false;
        for (final Source source : current) {
            final ReloadReport.Entry entry = prepare(source);
            refused |= !entry.outcome().accepted();
            entries.add(entry);
        }
        for (int i = 0; i < current.size(); i++) {
            final Source source = current.get(i);
            if (refused) {
                source.discard();
                final ReloadReport.Entry entry = entries.get(i);
                if (entry.outcome() == ReloadReport.Outcome.APPLIED) {
                    // It parsed and it was ready; saying "applied" would be a plain lie, since
                    // this reload applied nothing at all.
                    entries.set(i, new ReloadReport.Entry(entry.source(),
                            ReloadReport.Outcome.NOT_APPLIED,
                            "ready, and held back because another source was refused: "
                                    + entry.detail()));
                }
            } else {
                source.commit();
            }
        }
        return new ReloadReport(entries);
    }

    /**
     * A {@code Source} is documented not to throw, and this is what happens if one does anyway: it
     * becomes that source's refusal rather than the whole reload's exception, so a defect in one
     * source cannot cost every other source its reload -- nor leave the set half-prepared with
     * nobody to discard it.
     */
    private static ReloadReport.Entry prepare(final Source source) {
        try {
            return source.prepare();
        } catch (final Exception e) {
            return new ReloadReport.Entry(source.source(), ReloadReport.Outcome.FAILED,
                    "reading it threw: " + e);
        }
    }
}
