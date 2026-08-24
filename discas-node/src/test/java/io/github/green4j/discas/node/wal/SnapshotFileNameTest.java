/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("Snapshot file name parsing / formatting")
class SnapshotFileNameTest {

    @Test
    void finalRoundtrip() {
        final SnapshotFileName original = SnapshotFileName.finalSnapshot(1, 4812);
        assertEquals("001-snap-lsn-000000004812.snap", original.toFileName());
        final SnapshotFileName parsed = SnapshotFileName.parse(1, original.toFileName());
        assertNotNull(parsed);
        assertEquals(1, parsed.layoutVersion());
        assertEquals(4812, parsed.lsn());
        assertEquals(SnapshotFileName.Type.FINAL, parsed.type());
    }

    @Test
    void parseRejectsGarbage() {
        assertNull(SnapshotFileName.parse(1, "not-a-snapshot.dat"));
        assertNull(SnapshotFileName.parse(1, ""));
    }

    @Test
    void parseRejectsForeignLayoutVersion() {
        final String foreign = SnapshotFileName.finalSnapshot(2, 4812).toFileName();
        assertThrows(IllegalStateException.class, () -> SnapshotFileName.parse(1, foreign));
    }
}
