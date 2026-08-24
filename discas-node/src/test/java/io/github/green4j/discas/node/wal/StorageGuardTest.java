/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What the storage says about <em>itself</em>: which run of it this is, and when the question of a
 * provable ceiling may be asked at all.
 * <p>
 * Which directory shapes are provable is enumerated in {@link StorageRecoveryMatrixTest} rather
 * than sampled here.
 */
@DisplayName("FileWal -- identity, and when the ceiling question may be asked")
class StorageGuardTest {

    @TempDir
    Path dir;

    /**
     * A second managed directory, because the "different storage, different incarnation" case needs
     * two. It is a field rather than {@code dir.resolveSibling(...)}: a sibling of a {@code @TempDir}
     * is a fixed path in the system temp directory that JUnit neither makes unique nor deletes, so
     * the test would inherit whatever a previous run -- or another developer's run -- left there.
     */
    @TempDir
    Path otherDir;

    private static StorageConfig config(final Path dir) {
        return StorageConfig.builder().baseDirectory(dir).build();
    }


    /** Bootstrap a directory and leave a forced record in it. */
    private static void bootstrap(final Path dir) throws IOException {
        final FileWal wal = new FileWal(config(dir));
        wal.initialize();
        wal.replayTail(e -> { });
        wal.append(new Wal.Entry.PromiseCeiling(5000L));
        wal.force();
        wal.close();
    }


    @Test
    @DisplayName("Asking before the replay that answers it is a bug, not a safe default")
    void askingBeforeReplayThrows() throws IOException {
        final FileWal wal = new FileWal(config(dir));
        wal.initialize();
        assertThrows(IllegalStateException.class, wal::ceilingIsProven,
                "A silent 'cannot prove it' would send the node to the cluster and hide the missing"
                        + " replay");
        wal.close();
    }

    @Test
    @DisplayName("The incarnation is stable across restarts and new for a new directory")
    void incarnationIsPerStorageNotPerStart() throws IOException {
        bootstrap(dir);
        final FileWal first = new FileWal(config(dir));
        first.initialize();
        final String a = first.incarnation().value();
        first.close();

        final FileWal second = new FileWal(config(dir));
        second.initialize();
        assertEquals(a, second.incarnation().value(),
                "A restart is the same incarnation -- it is the storage that is identified");
        second.close();

        bootstrap(otherDir);
        final FileWal elsewhere = new FileWal(config(otherDir));
        elsewhere.initialize();
        assertNotEquals(a, elsewhere.incarnation().value(),
                "A different storage directory is a different incarnation");
        elsewhere.close();
    }

}
