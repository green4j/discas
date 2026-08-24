/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class WalFileName {

    private static final Pattern OPEN_TMP_PATTERN =
            Pattern.compile("(\\d{3})-wal-(\\d{12})\\.open\\.tmp");
    private static final Pattern OPEN_PATTERN =
            Pattern.compile("(\\d{3})-wal-(\\d{12})\\.open");
    private static final Pattern SEALED_PATTERN =
            Pattern.compile("(\\d{3})-wal-(\\d{12})-(\\d{12})-(\\d{12})\\.wal");
    private static final Pattern GC_PATTERN =
            Pattern.compile("(\\d{3})-wal-(\\d{12})-(\\d{12})-(\\d{12})\\.gc");

    public static final Comparator<WalFileName> BY_SEQUENCE =
            Comparator.comparingLong(WalFileName::sequence);

    public enum Type { OPEN_TMP, OPEN, SEALED, GC }

    private final int layoutVersion;
    private final long sequence;
    private final long firstLsn;
    private final long lastLsn;
    private final Type type;

    public WalFileName(final int layoutVersion,
                       final long sequence,
                       final long firstLsn,
                       final long lastLsn,
                       final Type type) {
        this.layoutVersion = layoutVersion;
        this.sequence = sequence;
        this.firstLsn = firstLsn;
        this.lastLsn = lastLsn;
        this.type = type;
    }

    public int layoutVersion() {
        return layoutVersion;
    }

    public long sequence() {
        return sequence;
    }

    public long firstLsn() {
        return firstLsn;
    }

    public long lastLsn() {
        return lastLsn;
    }

    public Type type() {
        return type;
    }

    public String toFileName() {
        final String prefix = String.format("%03d-", layoutVersion);
        switch (type) {
            case OPEN_TMP:
                return String.format("%swal-%012d.open.tmp", prefix, sequence);
            case OPEN:
                return String.format("%swal-%012d.open", prefix, sequence);
            case SEALED:
                return String.format("%swal-%012d-%012d-%012d.wal", prefix, sequence, firstLsn, lastLsn);
            case GC:
                return String.format("%swal-%012d-%012d-%012d.gc", prefix, sequence, firstLsn, lastLsn);
            default:
                throw new IllegalStateException("Unknown WAL file type: " + type);
        }
    }

    public Path toPath(final Path walDirectory) {
        return walDirectory.resolve(toFileName());
    }

    static WalFileName openTmp(final int layoutVersion, final long sequence) {
        return new WalFileName(layoutVersion, sequence, -1, -1, Type.OPEN_TMP);
    }

    public static WalFileName open(final int layoutVersion, final long sequence) {
        return new WalFileName(layoutVersion, sequence, -1, -1, Type.OPEN);
    }

    public static WalFileName sealed(final int layoutVersion,
                                     final long sequence,
                                     final long firstLsn,
                                     final long lastLsn) {
        return new WalFileName(layoutVersion, sequence, firstLsn, lastLsn, Type.SEALED);
    }

    public static WalFileName gc(final int layoutVersion,
                                 final long sequence,
                                 final long firstLsn,
                                 final long lastLsn) {
        return new WalFileName(layoutVersion, sequence, firstLsn, lastLsn, Type.GC);
    }

    /**
     * Parses a WAL file name, validating its layout-version prefix against
     * {@code expectedLayoutVersion}.
     *
     * @return the parsed name, or {@code null} if {@code name} is not a WAL file at all
     * @throws IllegalStateException if {@code name} is a WAL file written under a different
     *                               layout version (fail-closed: never silently skipped)
     */
    public static WalFileName parse(final int expectedLayoutVersion, final String name) {
        Matcher matcher = OPEN_TMP_PATTERN.matcher(name);
        if (matcher.matches()) {
            checkLayoutVersion(expectedLayoutVersion, matcher.group(1), name);
            return new WalFileName(expectedLayoutVersion,
                    Long.parseLong(matcher.group(2)), -1, -1, Type.OPEN_TMP);
        }

        matcher = OPEN_PATTERN.matcher(name);
        if (matcher.matches()) {
            checkLayoutVersion(expectedLayoutVersion, matcher.group(1), name);
            return new WalFileName(expectedLayoutVersion,
                    Long.parseLong(matcher.group(2)), -1, -1, Type.OPEN);
        }

        matcher = SEALED_PATTERN.matcher(name);
        if (matcher.matches()) {
            checkLayoutVersion(expectedLayoutVersion, matcher.group(1), name);
            return new WalFileName(
                    expectedLayoutVersion,
                    Long.parseLong(matcher.group(2)),
                    Long.parseLong(matcher.group(3)),
                    Long.parseLong(matcher.group(4)),
                    Type.SEALED);
        }

        matcher = GC_PATTERN.matcher(name);
        if (matcher.matches()) {
            checkLayoutVersion(expectedLayoutVersion, matcher.group(1), name);
            return new WalFileName(
                    expectedLayoutVersion,
                    Long.parseLong(matcher.group(2)),
                    Long.parseLong(matcher.group(3)),
                    Long.parseLong(matcher.group(4)),
                    Type.GC);
        }

        return null;
    }

    private static void checkLayoutVersion(final int expected,
                                           final String parsedGroup,
                                           final String name) {
        final int actual = Integer.parseInt(parsedGroup);
        if (actual != expected) {
            throw new IllegalStateException("Incompatible WAL layout version " + actual
                    + ", expected " + expected + ": " + name);
        }
    }

    @Override
    public String toString() {
        return toFileName();
    }
}
