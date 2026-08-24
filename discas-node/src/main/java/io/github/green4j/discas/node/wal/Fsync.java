/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * fsync helpers. Directory fsync is required after rename/create so the
 * dirent itself is durable on POSIX filesystems.
 */
final class Fsync {

    private Fsync() {
    }

    /** Force a regular file's contents to disk. Pair with {@link #dir} after a rename. */
    static void file(final Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    static void dir(final Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (final UnsupportedOperationException ignored) {
            // Windows does not support fsync on a directory handle; dirent
            // durability is provided by NTFS metadata journaling
        } catch (final IOException e) {
            // Some platforms (older macOS, some FUSE mounts) reject force()
            // on a directory channel with EINVAL. Treat as best-effort.
            if (!"Invalid argument".equals(e.getMessage())) {
                throw e;
            }
        }
    }
}
