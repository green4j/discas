/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.io;

import io.github.green4j.discas.common.Hex;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A file-backed {@link Reloadable}: publishes an immutable value parsed from a set of files, with
 * <b>replay-on-subscribe</b>, and re-reads them when it is asked to. Everything specific to one
 * kind of file lives in its {@link Parser}.
 *
 * <p>Registers itself with {@link ReloadableFiles}, so one call re-reads every file the process is
 * serving and the whole set applies atomically. A reload is two phases -- everything that can fail
 * happens before anything is published -- which is what lets a certificate and its key, or a
 * members list and the ACL that references it, change together.
 *
 * <p>What reaches the value is gated three times, cheapest first: a combined SHA-256 of the
 * contents, so byte-identical files are not even re-parsed; then the parser, which refuses what it
 * cannot make sense of; then {@code equals} against the value in force, so an edit that parses to
 * the same value is not re-published. Ahead of all three, a file caught mid-write is not read as a
 * revision at all -- see {@link #readContents()}. Each of those is reported, since "your edit
 * changed nothing" and "your edit did not parse" are different answers and both are answers.
 *
 * <p>The constructor loads the value and <b>fails fast</b> if nothing loads or the initial parse
 * throws. A later reload that fails keeps the last good value. Owns no thread.
 */
public final class ReloadableFileSource<T> implements Reloadable<T>, AutoCloseable {

    /** Parses the raw contents of the files (same order as supplied) into a value. */
    @FunctionalInterface
    public interface Parser<T> {
        T parse(List<byte[]> contents) throws Exception;
    }

    /**
     * Says, in one line, what a value holds -- for the reload report an operator reads to confirm
     * that what reached the process is what they wrote.
     *
     * <p>Every source supplies its own, because only the source knows which half of its file is
     * safe to name: a members list is topology and an ACL is a table of who may touch what, but a
     * token file and a key store are the secret itself. The default says nothing about contents at
     * all, so a source that has not thought about it cannot leak by omission.
     *
     * <p>Distinct from a config {@code describe()}, which tabulates a whole process's effective
     * configuration once at startup and <em>masks</em> the secrets among it. This runs on every
     * revision of one value, fits on a log line, and does not mask anything -- it omits, naming
     * what a secret file has (which clients, how many records, until when) rather than what it is.
     */
    @FunctionalInterface
    public interface Summary<T> {
        String of(T value);
    }

    /** Names no contents: the safe answer for a source that has not supplied a summary. */
    private static final Summary<Object> NO_SUMMARY = value -> "applied";

    private static final String BYTE_IDENTICAL = "byte-identical to what is in force; not applied";
    private static final String SAME_MEANING =
            "the file changed but says what is already in force; not applied";
    private static final String MID_WRITE = "a file is being written; nothing was read";

    /**
     * How long a file that has just been emptied is given to prove that it means it.
     *
     * <p>A rewrite in place reads as a complete, stable, empty file between the truncate and the
     * first byte, and nothing in the file distinguishes that from one an operator meant to empty.
     * So the distinction is time: an empty read is accepted only if it is still empty this much
     * later. A truncate-then-write is over in microseconds and never survives it; emptying a file
     * for real costs this once, on the reload that first sees it.
     *
     * <p>Paid only when the bytes changed and read as nothing -- a file that is already empty and
     * in force is byte-identical to itself, and answered before this arises.
     */
    private static final long EMPTY_CONFIRM_MILLIS = 100L;

    private final List<Path> files;
    private final Parser<T> parser;
    private final Summary<? super T> summary;
    private final String source;
    private final CopyOnWriteArrayList<Consumer<T>> listeners = new CopyOnWriteArrayList<>();
    private final ReloadObserver observer;
    private final ReloadableFiles.Registration registration;

    private volatile String lastFingerprint;
    private volatile T current;

    // Held between prepare() and commit()/discard(). A fingerprint with no value is the
    // "changed bytes, same meaning" case: there is nothing to publish, but the hash still moved
    // and banking it is what keeps the next reload from re-parsing the same bytes.
    private String stagedFingerprint;
    private T stagedValue;

    public ReloadableFileSource(final List<Path> files,
                                final Parser<T> parser) {
        this(files, parser, ReloadObserver.NONE);
    }

    /**
     * @param observer reports what each reload did. Worth wiring: this class keeps serving its last
     *                 good value when a reload is refused, so without it a file that has been
     *                 broken for hours looks exactly like one nobody has edited.
     */
    public ReloadableFileSource(final List<Path> files,
                                final Parser<T> parser,
                                final ReloadObserver observer) {
        this(files, parser, NO_SUMMARY, observer);
    }

    /**
     * @param summary what goes in the reload report for each applied value. See {@link Summary}:
     *                it is read by whoever reads the log, so it names what an operator needs to
     *                recognise their edit and nothing that is secret.
     */
    public ReloadableFileSource(final List<Path> files,
                                final Parser<T> parser,
                                final Summary<? super T> summary,
                                final ReloadObserver observer) {
        this(ReloadableFiles.shared(), files, parser, summary, observer);
    }

    /**
     * Registers with {@code registry} rather than the process-wide one. For tests, which need a set
     * of sources they can reload without reaching every other source the JVM happens to hold.
     */
    ReloadableFileSource(final ReloadableFiles registry,
                         final List<Path> files,
                         final Parser<T> parser,
                         final Summary<? super T> summary,
                         final ReloadObserver observer) {
        this.observer = observer == null ? ReloadObserver.NONE : observer;
        this.files = List.copyOf(files);
        this.parser = parser;
        this.summary = summary == null ? NO_SUMMARY : summary;
        this.source = label(this.files);
        // Fail fast: a process must not start on a file it could not read, and there is no last
        // good value to fall back on yet. A parse failure throws from prepare() for the same reason.
        final ReloadReport.Entry initial = prepare();
        if (!initial.outcome().accepted()) {
            throw new IllegalStateException("Cannot load initial content from " + this.source
                    + ": " + initial.detail());
        }
        commit();
        this.registration = registry.register(asSource());
    }

    @Override
    public T snapshot() {
        return current;
    }

    @Override
    public synchronized void addListener(final Consumer<T> listener) {
        // Replay-on-subscribe: hand the new listener the current value as its first update,
        // atomically with reloads (both take this monitor), so nothing is missed between
        // the consumer's read and its subscription.
        listener.accept(current);
        listeners.add(listener);
    }

    /**
     * Re-read this source alone and apply the result. What {@link ReloadableFiles#reloadAll()} does
     * for every source at once; use that where a set of files must move together.
     */
    public synchronized ReloadReport.Entry reloadNow() {
        final ReloadReport.Entry entry = prepare();
        if (entry.outcome().accepted()) {
            commit();
        } else {
            discard();
        }
        return entry;
    }

    @Override
    public void close() {
        registration.close();
    }

    /** This source's view for the registry, kept off the public surface. */
    private ReloadableFiles.Source asSource() {
        return new ReloadableFiles.Source() {
            @Override
            public String source() {
                return source;
            }

            @Override
            public ReloadReport.Entry prepare() {
                return ReloadableFileSource.this.prepare();
            }

            @Override
            public void commit() {
                ReloadableFileSource.this.commit();
            }

            @Override
            public void discard() {
                ReloadableFileSource.this.discard();
            }
        };
    }

    /**
     * Read, gate, parse and hold -- publishing nothing. The initial load is the exception: with no
     * value in force there is nothing to keep serving, so a file that does not parse throws from
     * here and out of the constructor.
     */
    private synchronized ReloadReport.Entry prepare() {
        stagedFingerprint = null;
        stagedValue = null;
        List<byte[]> contents = readContents();
        if (contents == null) {
            return entry(ReloadReport.Outcome.UNREADABLE, MID_WRITE);
        }
        String fingerprint = fingerprint(contents);
        if (fingerprint.equals(lastFingerprint)) {
            // Byte-identical: nothing new, and not even worth re-parsing.
            return unchanged(BYTE_IDENTICAL);
        }
        if (current != null && isEmpty(contents)) {
            contents = confirmEmpty();
            if (contents == null) {
                return entry(ReloadReport.Outcome.UNREADABLE, MID_WRITE);
            }
            fingerprint = fingerprint(contents);
            if (fingerprint.equals(lastFingerprint)) {
                return unchanged(BYTE_IDENTICAL); // the rewrite put back what was already there
            }
        }
        final T candidate;
        try {
            candidate = parser.parse(contents);
        } catch (final Exception e) {
            if (current == null) {
                throw new IllegalStateException(
                        "Failed to parse initial content from " + source, e);
            }
            observer.reloadFailed(source, e);
            return entry(ReloadReport.Outcome.FAILED, reason(e));
        }
        stagedFingerprint = fingerprint;
        if (candidate.equals(current)) {
            // The bytes moved and the meaning did not: whitespace, a comment, reordered lines.
            return unchanged(SAME_MEANING);
        }
        stagedValue = candidate;
        return entry(ReloadReport.Outcome.APPLIED, summaryOf(candidate));
    }

    /** Publish what {@link #prepare()} held. Never throws: a consumer's fault is not the file's. */
    private synchronized void commit() {
        if (stagedFingerprint == null) {
            return;
        }
        lastFingerprint = stagedFingerprint;
        stagedFingerprint = null;
        final T candidate = stagedValue;
        stagedValue = null;
        if (candidate == null) {
            return; // the hash moved, the value did not
        }
        final boolean initial = current == null;
        current = candidate; // atomic publish
        for (final Consumer<T> listener : listeners) {
            try {
                listener.accept(candidate);
            } catch (final Exception e) {
                // The value is in force whatever a consumer makes of it, and the consumers after
                // this one are owed their update. A listener that throws is a defect in the
                // listener, which is what this channel says.
                observer.checkFailed(source, e);
            }
        }
        observer.reloaded(source, (initial ? "loaded -- " : "applied -- ") + summaryOf(candidate));
    }

    /** Drop what {@link #prepare()} held: the value in force, and its hash, stay as they were. */
    private synchronized void discard() {
        stagedFingerprint = null;
        stagedValue = null;
    }

    private ReloadReport.Entry entry(final ReloadReport.Outcome outcome, final String detail) {
        return new ReloadReport.Entry(source, outcome, detail);
    }

    /**
     * Nothing to apply, and said out loud. An operator who has just saved a file is asking whether
     * it took effect, and "your edit changed nothing" is a different answer from both "applied" and
     * "did not parse" -- it usually means they edited a copy, or that the change was cosmetic.
     */
    private ReloadReport.Entry unchanged(final String detail) {
        observer.reloadUnchanged(source, detail);
        return entry(ReloadReport.Outcome.UNCHANGED, detail);
    }

    /** Never let a summary's own failure cost the reload that succeeded. */
    private String summaryOf(final T value) {
        try {
            return summary.of(value);
        } catch (final Exception e) {
            return "contents could not be described: " + e;
        }
    }

    /** Why a parse was refused, in one line and without a stack trace: the log has that. */
    private static String reason(final Exception e) {
        final String message = e.getMessage();
        return message == null ? e.getClass().getSimpleName() : message;
    }

    /**
     * Read every file's bytes; {@code null} if any of them is not readable as a finished file yet.
     *
     * <p>A file being rewritten in place is readable long before it is complete: an in-progress
     * write reads short. That is not a revision anybody wrote, and publishing one is not merely a
     * transient wrong answer -- it becomes the last good value, so an edit that ends up not parsing
     * leaves the process serving a half file. So each file is read between two agreeing readings of
     * its {@code (mtime, size)}, and the bytes must be as long as the size says.
     */
    private List<byte[]> readContents() {
        final List<byte[]> contents = new ArrayList<>(files.size());
        for (final Path file : files) {
            final byte[] bytes = readStable(file);
            if (bytes == null) {
                return null;
            }
            contents.add(bytes);
        }
        return contents;
    }

    /**
     * The other half of the rewrite guard, for the case the size cannot catch: a file truncated to
     * nothing reads as complete and stable. Wait, look again, and let what is there then decide --
     * see {@link #EMPTY_CONFIRM_MILLIS}.
     */
    private List<byte[]> confirmEmpty() {
        try {
            Thread.sleep(EMPTY_CONFIRM_MILLIS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null; // shutting down: decide nothing
        }
        return readContents();
    }

    private static boolean isEmpty(final List<byte[]> contents) {
        for (final byte[] bytes : contents) {
            if (bytes.length > 0) {
                return false;
            }
        }
        return true;
    }

    /** A file's bytes, or {@code null} if it changed under the read (so the bytes are a fragment). */
    private static byte[] readStable(final Path file) {
        try {
            final BasicFileAttributes before = Files.readAttributes(file, BasicFileAttributes.class);
            final byte[] bytes = Files.readAllBytes(file);
            final BasicFileAttributes after = Files.readAttributes(file, BasicFileAttributes.class);
            if (before.size() != after.size()
                    || !before.lastModifiedTime().equals(after.lastModifiedTime())
                    || bytes.length != after.size()) {
                return null;
            }
            return bytes;
        } catch (final Exception e) {
            return null; // missing, or unreadable for now
        }
    }

    /** How this source is named wherever it is reported: the paths, and nothing else. */
    private static String label(final List<Path> files) {
        final StringBuilder sb = new StringBuilder();
        for (final Path file : files) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(file);
        }
        return sb.toString();
    }

    private static String fingerprint(final List<byte[]> contents) {
        final StringBuilder sb = new StringBuilder();
        for (final byte[] bytes : contents) {
            if (sb.length() > 0) {
                sb.append(':');
            }
            sb.append(sha256(bytes));
        }
        return sb.toString();
    }

    private static String sha256(final byte[] bytes) {
        try {
            return Hex.encode(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (final Exception e) {
            throw new RuntimeException("SHA-256 unavailable", e);
        }
    }
}
