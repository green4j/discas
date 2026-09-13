/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("AuditDrain -- hands records over, and says what a full buffer cost")
class AuditDrainTest {

    private static byte[] record(final AuditEventKind kind) {
        final byte[] record = new byte[AuditRecordLayout.SECTIONS
                + AuditRecordLayout.SECTION_COUNT * Integer.BYTES];
        record[AuditRecordLayout.KIND] = kind.code();
        return record;
    }

    private static void awaitSize(final List<?> list, final int size) throws Exception {
        for (int i = 0; i < 200 && list.size() < size; i++) {
            Thread.sleep(10L);
        }
        assertEquals(size, list.size(), list.toString());
    }

    @Test
    void handsEveryRecordToTheSink() throws Exception {
        final AuditRing ring = new AuditRing(4096);
        final List<AuditEventKind> seen = new CopyOnWriteArrayList<>();
        final AuditDrain drain = new AuditDrain("audit-drain-test", ring,
                event -> seen.add(event.kind()), count -> { });
        drain.start();
        try {
            ring.offer(record(AuditEventKind.SESSION_OPENED),
                    record(AuditEventKind.SESSION_OPENED).length);
            ring.offer(record(AuditEventKind.RESULT), record(AuditEventKind.RESULT).length);
            awaitSize(seen, 2);
        } finally {
            drain.close();
        }
        assertEquals(AuditEventKind.SESSION_OPENED, seen.get(0));
        assertEquals(AuditEventKind.RESULT, seen.get(1));
    }

    @Test
    @DisplayName("A loss is reported to the node and written into the trail")
    void reportsWhatWasLost() throws Exception {
        final AuditRing ring = new AuditRing(1024);
        final List<AuditEventKind> seen = new CopyOnWriteArrayList<>();
        final AtomicLong reported = new AtomicLong();
        final List<Long> losses = new CopyOnWriteArrayList<>();

        // Fill it with the drain stopped, so the records that follow have nowhere to go.
        final byte[] record = record(AuditEventKind.REQUEST);
        while (ring.offer(record, record.length)) {
            continue;
        }
        final long dropped = ring.droppedRecords();
        assertTrue(dropped > 0);

        final AuditDrain drain = new AuditDrain("audit-drain-loss", ring, event -> {
            seen.add(event.kind());
            if (event.kind() == AuditEventKind.RECORDS_LOST) {
                losses.add(event.lostRecords());
            }
        }, count -> reported.addAndGet(count));
        drain.start();
        try {
            for (int i = 0; i < 200 && losses.isEmpty(); i++) {
                Thread.sleep(10L);
            }
        } finally {
            drain.close();
        }

        assertEquals(dropped, reported.get());
        assertEquals(List.of(dropped), losses);
        assertTrue(seen.contains(AuditEventKind.REQUEST));
    }

    @Test
    @DisplayName("Close hands over what is still buffered")
    void closeDrainsTheRemainder() throws Exception {
        final AuditRing ring = new AuditRing(4096);
        final List<AuditEventKind> seen = new CopyOnWriteArrayList<>();
        final AuditDrain drain = new AuditDrain("audit-drain-close", ring,
                event -> seen.add(event.kind()), count -> { });
        final byte[] record = record(AuditEventKind.RESULT);
        for (int i = 0; i < 5; i++) {
            ring.offer(record, record.length);
        }

        drain.start();
        drain.close();

        assertEquals(5, seen.size());
        assertTrue(ring.isEmpty());
    }

    @Test
    void drainStopsWithinItsJoinWindow() throws Exception {
        final AuditRing ring = new AuditRing(1024);
        final AuditDrain drain = new AuditDrain("audit-drain-stop", ring, event -> { },
                count -> { });
        drain.start();
        final long startedAt = System.nanoTime();
        drain.close();
        assertTrue(System.nanoTime() - startedAt < TimeUnit.SECONDS.toNanos(5));
    }
}
