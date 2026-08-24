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

final class SnapshotFileName {

    private static final Pattern FINAL_PATTERN =
            Pattern.compile("(\\d{3})-snap-lsn-(\\d{12})\\.snap");
    private static final Pattern TMP_PATTERN =
            Pattern.compile("(\\d{3})-snap-lsn-(\\d{12})\\.snap\\.tmp");
    private static final Pattern GC_PATTERN =
            Pattern.compile("(\\d{3})-snap-lsn-(\\d{12})\\.gc");

    private static final Comparator<SnapshotFileName> BY_LSN_ASCENDING =
            Comparator.comparingLong(SnapshotFileName::lsn);
    static final Comparator<SnapshotFileName> BY_LSN_DESCENDING =
            BY_LSN_ASCENDING.reversed();

    public enum Type { TMP, FINAL, GC }

    private final int layoutVersion;
    private final long lsn;
    private final Type type;

    SnapshotFileName(final int layoutVersion, final long lsn, final Type type) {
        this.layoutVersion = layoutVersion;
        this.lsn = lsn;
        this.type = type;
    }

    int layoutVersion() {
        return layoutVersion;
    }

    long lsn() {
        return lsn;
    }

    public Type type() {
        return type;
    }

    String toFileName() {
        final String prefix = String.format("%03d-", layoutVersion);
        switch (type) {
            case TMP:
                return String.format("%ssnap-lsn-%012d.snap.tmp", prefix, lsn);
            case FINAL:
                return String.format("%ssnap-lsn-%012d.snap", prefix, lsn);
            case GC:
                return String.format("%ssnap-lsn-%012d.gc", prefix, lsn);
            default:
                throw new IllegalStateException("Unknown snapshot file type: " + type);
        }
    }

    Path toPath(final Path snapshotDirectory) {
        return snapshotDirectory.resolve(toFileName());
    }

    static SnapshotFileName tmp(final int layoutVersion, final long lsn) {
        return new SnapshotFileName(layoutVersion, lsn, Type.TMP);
    }

    static SnapshotFileName finalSnapshot(final int layoutVersion, final long lsn) {
        return new SnapshotFileName(layoutVersion, lsn, Type.FINAL);
    }

    static SnapshotFileName gc(final int layoutVersion, final long lsn) {
        return new SnapshotFileName(layoutVersion, lsn, Type.GC);
    }

    /**
     * Parses a snapshot file name, validating its layout-version prefix against
     * {@code expectedLayoutVersion}.
     *
     * @return the parsed name, or {@code null} if {@code name} is not a snapshot file at all
     * @throws IllegalStateException if {@code name} is a snapshot file written under a different
     *                               layout version (fail-closed: never silently skipped)
     */
    public static SnapshotFileName parse(final int expectedLayoutVersion, final String name) {
        Matcher matcher = TMP_PATTERN.matcher(name);
        if (matcher.matches()) {
            checkLayoutVersion(expectedLayoutVersion, matcher.group(1), name);
            return new SnapshotFileName(expectedLayoutVersion,
                    Long.parseLong(matcher.group(2)), Type.TMP);
        }

        matcher = FINAL_PATTERN.matcher(name);
        if (matcher.matches()) {
            checkLayoutVersion(expectedLayoutVersion, matcher.group(1), name);
            return new SnapshotFileName(expectedLayoutVersion,
                    Long.parseLong(matcher.group(2)), Type.FINAL);
        }

        matcher = GC_PATTERN.matcher(name);
        if (matcher.matches()) {
            checkLayoutVersion(expectedLayoutVersion, matcher.group(1), name);
            return new SnapshotFileName(expectedLayoutVersion,
                    Long.parseLong(matcher.group(2)), Type.GC);
        }

        return null;
    }

    private static void checkLayoutVersion(final int expected,
                                           final String parsedGroup,
                                           final String name) {
        final int actual = Integer.parseInt(parsedGroup);
        if (actual != expected) {
            throw new IllegalStateException("Incompatible snapshot layout version " + actual
                    + ", expected " + expected + ": " + name);
        }
    }

    @Override
    public String toString() {
        return toFileName();
    }
}
