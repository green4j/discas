/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import java.nio.file.Path;

/**
 * Storage tunables for {@link FileWal}: where the segments and snapshots live, how large a
 * segment grows before it rolls, and how many snapshots are kept.
 * <p>
 * Every default is declared inline on its field -- that line is the single source of truth for
 * both the value and its documentation. Format invariants (header sizes, format and layout
 * versions) are on {@link StorageFormat} instead: they are properties of the binary, not knobs.
 */
public final class StorageConfig {

    // Required; there is no sensible default for where a node keeps its data.
    private Path baseDirectory;

    // Size a WAL segment reaches before it is rolled. Only a roll threshold -- the write
    // buffer is sized from the maximum record instead (see WalWriter).
    private int walMaxFileBytes = 64 * 1024 * 1024;

    // Snapshots retained after compaction. Two, so a reader working through the previous
    // snapshot is never left without a file.
    private int snapshotRetentionCount = 2;

    private StorageConfig() {
    }

    /** Copy constructor: the built config shares no state with the builder that produced it. */
    private StorageConfig(final StorageConfig other) {
        this.baseDirectory = other.baseDirectory;
        this.walMaxFileBytes = other.walMaxFileBytes;
        this.snapshotRetentionCount = other.snapshotRetentionCount;
    }

    /** A builder pre-loaded with the defaults; {@code baseDirectory} still has to be set. */
    public static Builder builder() {
        return new Builder();
    }

    /** The node's data directory. Everything below is derived from it. */
    public Path baseDirectory() {
        return baseDirectory;
    }

    /** Size a WAL segment reaches before it rolls (default 64 MiB). */
    public int walMaxFileBytes() {
        return walMaxFileBytes;
    }

    /** Snapshots retained after compaction (default 2). */
    public int snapshotRetentionCount() {
        return snapshotRetentionCount;
    }

    /** Where WAL segments live: {@code <baseDirectory>/wal}. */
    public Path walDirectory() {
        return baseDirectory.resolve("wal");
    }

    /** Where snapshots live: {@code <baseDirectory>/snap}. */
    public Path snapshotDirectory() {
        return baseDirectory.resolve("snap");
    }

    /** Fluent builder. Each setter validates its own argument; {@link #build()} checks the rest. */
    public static final class Builder {

        private final StorageConfig cfg = new StorageConfig();

        private Builder() {
        }

        /** Required: the node's data directory. @see StorageConfig#baseDirectory() */
        public Builder baseDirectory(final Path value) {
            if (value == null) {
                throw new IllegalArgumentException("baseDirectory is required");
            }
            cfg.baseDirectory = value;
            return this;
        }

        /** @see StorageConfig#walMaxFileBytes() */
        public Builder walMaxFileBytes(final int value) {
            if (value <= 0) {
                throw new IllegalArgumentException("walMaxFileBytes must be positive: " + value);
            }
            cfg.walMaxFileBytes = value;
            return this;
        }

        /** @see StorageConfig#snapshotRetentionCount() */
        public Builder snapshotRetentionCount(final int value) {
            if (value < 1) {
                throw new IllegalArgumentException(
                        "snapshotRetentionCount must be at least 1: " + value);
            }
            cfg.snapshotRetentionCount = value;
            return this;
        }

        /**
          * @throws IllegalArgumentException if no base directory was set.
          */
        public StorageConfig build() {
            if (cfg.baseDirectory == null) {
                throw new IllegalArgumentException("baseDirectory is required");
            }
            return new StorageConfig(cfg);
        }
    }
}
