/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.identity.IncarnationId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.List;

/**
 * The {@code incarnation} file at the root of a node's data directory: proof that this storage has
 * been initialized before, and the identity of the run that initialized it.
 *
 * <h2>What it is for</h2>
 * Nothing in an empty directory distinguishes "this node is new" from "someone deleted this node's
 * state". The difference matters because the second case is an amnesiac acceptor -- it has forgotten
 * promises no peer ever held, so no quorum can repair it -- and it must not vote while claiming to be
 * its former self. The only way to tell them apart is to have recorded something while the state
 * still existed, which is this file.
 *
 * <h2>Format</h2>
 * Plain text, one {@code key=value} per line. An operator staring at a data directory
 * during an incident should be able to read it with {@code cat}, and it is small enough that a
 * binary framing would buy nothing.
 *
 * <pre>
 *   incarnation=550e8400-e29b-41d4-a716-446655440000
 *   created=2026-08-18T09:41:03.221Z
 * </pre>
 *
 * <p>Written through a temp file, fsynced, then atomically renamed, with the directory fsynced
 * afterwards: a half-written marker would be worse than none, since it would make a healthy node
 * look wiped.
 */
public final class IncarnationMarker {

    private static final String FILE_NAME = "incarnation";
    private static final String TMP_NAME = "incarnation.tmp";
    private static final String KEY_INCARNATION = "incarnation=";
    private static final String KEY_CREATED = "created=";

    private IncarnationMarker() {
    }

    /** Whether {@code baseDirectory} has been initialized as a discas data directory before. */
    public static boolean exists(final Path baseDirectory) {
        return Files.isRegularFile(baseDirectory.resolve(FILE_NAME));
    }

    /**
     * Read the incarnation this storage was created under.
     *
     * @throws IOException if the marker is missing or unreadable
     * @throws IllegalStateException if it is present but malformed -- a marker that cannot be parsed
     *         is not treated as absent, because "absent" is the answer that lets a node start
     */
    public static IncarnationId read(final Path baseDirectory) throws IOException {
        final Path file = baseDirectory.resolve(FILE_NAME);
        final List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (final String line : lines) {
            if (line.startsWith(KEY_INCARNATION)) {
                return IncarnationId.of(line.substring(KEY_INCARNATION.length()).trim());
            }
        }
        throw new IllegalStateException("Incarnation marker at " + file + " has no incarnation= line");
    }

    /**
     * Create the marker for a storage directory being initialized for the first time.
     *
     * @throws IllegalStateException if one already exists -- overwriting it would erase the very
     *         evidence it exists to preserve
     */
    public static IncarnationId create(final Path baseDirectory) throws IOException {
        if (exists(baseDirectory)) {
            throw new IllegalStateException(
                    "Incarnation marker already exists at " + baseDirectory.resolve(FILE_NAME));
        }
        final IncarnationId incarnation = IncarnationId.generate();
        final String body = KEY_INCARNATION + incarnation.value() + System.lineSeparator()
                + KEY_CREATED + Instant.now() + System.lineSeparator();

        final Path tmp = baseDirectory.resolve(TMP_NAME);
        Files.write(tmp, body.getBytes(StandardCharsets.UTF_8));
        Fsync.file(tmp);
        try {
            Files.move(tmp, baseDirectory.resolve(FILE_NAME),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final AtomicMoveNotSupportedException e) {
            // Some network filesystems refuse ATOMIC_MOVE. A non-atomic rename is weaker but still
            // better than writing in place, and this file is written exactly once in a directory's
            // life -- there is no concurrent reader to tear.
            Files.move(tmp, baseDirectory.resolve(FILE_NAME), StandardCopyOption.REPLACE_EXISTING);
        }
        Fsync.dir(baseDirectory);
        return incarnation;
    }
}
