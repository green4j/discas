/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("AuditRing -- one producer, one consumer, records never split")
class AuditRingTest {

    private static byte[] record(final int length, final byte fill) {
        final byte[] record = new byte[length];
        java.util.Arrays.fill(record, fill);
        // Byte 0 is the kind: anything but the padding marker, which is zero.
        record[0] = fill == 0 ? 1 : fill;
        return record;
    }

    private static List<String> drainAll(final AuditRing ring) {
        final List<String> seen = new ArrayList<>();
        ring.drain((buffer, offset, length) -> {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                sb.append(buffer[offset + i]).append(',');
            }
            seen.add(sb.toString());
        }, 1024);
        return seen;
    }

    @Test
    void handsBackWhatWasPublished() {
        final AuditRing ring = new AuditRing(1024);
        assertTrue(ring.offer(record(8, (byte) 7), 8));
        assertTrue(ring.offer(record(9, (byte) 9), 9));

        final List<String> seen = drainAll(ring);
        assertEquals(2, seen.size());
        assertTrue(seen.get(0).startsWith("7,"));
        assertTrue(seen.get(1).startsWith("9,"));
        assertTrue(ring.isEmpty());
    }

    @Test
    @DisplayName("A record that would straddle the end is preceded by padding, not split")
    void wrapsWithPadding() {
        final AuditRing ring = new AuditRing(1024);
        // 100 bytes of payload take 104 with the length prefix and alignment; ten of them leave
        // fewer than that before the seam.
        for (int i = 0; i < 40; i++) {
            assertTrue(ring.offer(record(100, (byte) (i % 100 + 1)), 100));
            assertEquals(1, drainAll(ring).size());
        }
        assertEquals(0, ring.droppedRecords());
    }

    @Test
    void dropsAndCountsWhenFull() {
        final AuditRing ring = new AuditRing(1024);
        int accepted = 0;
        while (ring.offer(record(100, (byte) 3), 100)) {
            accepted++;
        }
        assertTrue(accepted > 0);
        assertEquals(1, ring.droppedRecords());
        assertFalse(ring.offer(record(100, (byte) 3), 100));
        assertEquals(2, ring.droppedRecords());
    }

    @Test
    @DisplayName("A record larger than the buffer is dropped rather than waited on")
    void oversizedRecordIsNotWaitedFor() {
        final AuditRing ring = new AuditRing(1024);
        assertFalse(ring.offerOrWait(record(2048, (byte) 4), 2048));
        assertEquals(1, ring.droppedRecords());
    }

    @Test
    void waitingProducerProceedsOnceTheConsumerDrains() throws Exception {
        final AuditRing ring = new AuditRing(1024);
        while (ring.offer(record(100, (byte) 5), 100)) {
            continue;
        }
        final CountDownLatch published = new CountDownLatch(1);
        final Thread producer = new Thread(() -> {
            ring.offerOrWait(record(100, (byte) 6), 100);
            published.countDown();
        });
        producer.start();

        assertFalse(published.await(100, TimeUnit.MILLISECONDS));
        drainAll(ring);
        assertTrue(published.await(5, TimeUnit.SECONDS));
        producer.join();
    }

    @Test
    @DisplayName("Everything a producer publishes reaches the consumer, in order")
    void singleProducerSingleConsumerUnderLoad() throws Exception {
        final AuditRing ring = new AuditRing(4096);
        final int records = 20_000;
        final AtomicInteger consumed = new AtomicInteger();
        final AtomicInteger outOfOrder = new AtomicInteger();
        final Thread consumer = new Thread(() -> {
            int expected = 0;
            while (consumed.get() < records) {
                final int[] next = {expected};
                ring.drain((buffer, offset, length) -> {
                    if ((buffer[offset + 1] & 0xff) != (next[0] & 0xff)) {
                        outOfOrder.incrementAndGet();
                    }
                    next[0]++;
                    consumed.incrementAndGet();
                }, 64);
                expected = next[0];
            }
        });
        consumer.start();
        for (int i = 0; i < records; i++) {
            final byte[] record = record(24, (byte) 2);
            record[1] = (byte) i;
            ring.offerOrWait(record, record.length);
        }
        consumer.join(10_000);
        assertEquals(records, consumed.get());
        assertEquals(0, outOfOrder.get());
        assertEquals(0, ring.droppedRecords());
    }
}
