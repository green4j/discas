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

@DisplayName("WAL file name parsing / formatting")
class WalFileNameTest {

    @Test
    void sealedRoundtrip() {
        final WalFileName original = WalFileName.sealed(1, 42, 100, 200);
        final String fileName = original.toFileName();
        assertEquals("001-wal-000000000042-000000000100-000000000200.wal", fileName);
        final WalFileName parsed = WalFileName.parse(1, fileName);
        assertNotNull(parsed);
        assertEquals(1, parsed.layoutVersion());
        assertEquals(42, parsed.sequence());
        assertEquals(100, parsed.firstLsn());
        assertEquals(200, parsed.lastLsn());
        assertEquals(WalFileName.Type.SEALED, parsed.type());
    }

    @Test
    void parseRejectsGarbage() {
        assertNull(WalFileName.parse(1, "random.txt"));
        assertNull(WalFileName.parse(1, "wal-abc.open"));
        assertNull(WalFileName.parse(1, "001-wal-abc.open"));
        assertNull(WalFileName.parse(1, ""));
    }

    @Test
    void parseRejectsForeignLayoutVersion() {
        final String foreign = WalFileName.sealed(2, 42, 100, 200).toFileName();
        assertThrows(IllegalStateException.class, () -> WalFileName.parse(1, foreign));
    }
}
