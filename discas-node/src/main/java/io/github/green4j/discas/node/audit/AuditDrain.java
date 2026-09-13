/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;

/**
 * Takes records out of the {@link AuditRing} and hands them to an {@link AuditLog}, so the sink's
 * cost is not the event loop's.
 * <p>
 * Also where a full buffer is reported: the loop that lost a record cannot say so without the room
 * it just failed to get, so the drain notices the count move, tells the node's observer, and
 * writes the loss into the trail itself.
 */
public final class AuditDrain implements AutoCloseable, AuditRing.RecordHandler {

    private static final int BATCH = 256;
    private static final long IDLE_PARK_NANOS = 1_000_000L;

    private final AuditRing ring;
    private final AuditLog log;
    private final LongConsumer droppedReporter;
    private final AuditEvent event = new AuditEvent();
    private final byte[] lossRecord = new byte[AuditRecordLayout.SECTIONS
            + AuditRecordLayout.SECTION_COUNT * Integer.BYTES];
    private final Thread thread;

    private volatile boolean running = true;
    private long reportedDrops;

    /**
     * @param droppedReporter told how many records were lost since last time, on the drain thread.
     *                        The node routes it into its {@code NodeObserver}.
     */
    public AuditDrain(final String name, final AuditRing ring, final AuditLog log,
                      final LongConsumer droppedReporter) {
        this.ring = ring;
        this.log = log;
        this.droppedReporter = droppedReporter;
        this.thread = new Thread(this::run, name);
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    /** Stops the thread, hands over what is still buffered, and closes the sink. */
    @Override
    public void close() {
        running = false;
        LockSupport.unpark(thread);
        try {
            thread.join(5_000L);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        drainAll();
        reportDrops();
        try {
            log.close();
        } catch (final Exception ignored) {
            // Nothing left to report it to: the node is already down to its last closeables.
        }
    }

    private void run() {
        while (running) {
            final int handled = drainAll();
            reportDrops();
            if (handled == 0) {
                LockSupport.parkNanos(IDLE_PARK_NANOS);
            }
        }
    }

    private int drainAll() {
        int total = 0;
        int handled;
        do {
            handled = ring.drain(this, BATCH);
            total += handled;
        } while (handled == BATCH);
        return total;
    }

    @Override
    public void onRecord(final byte[] buffer, final int offset, final int length) {
        event.wrap(buffer, offset, length);
        try {
            log.record(event);
        } catch (final RuntimeException ignored) {
            // One bad record must not stop the trail; the sink's own failures are its to report.
        }
    }

    private void reportDrops() {
        final long dropped = ring.droppedRecords();
        final long since = dropped - reportedDrops;
        if (since <= 0) {
            return;
        }
        reportedDrops = dropped;
        if (droppedReporter != null) {
            droppedReporter.accept(since);
        }
        recordLoss(since);
    }

    /**
     * Writes the loss into the trail where the lost records would have been. Built here: the ring
     * is what had no room, and the recorder's scratch belongs to the event loop.
     */
    private void recordLoss(final long since) {
        for (int i = 0; i < lossRecord.length; i++) {
            lossRecord[i] = 0;
        }
        lossRecord[AuditRecordLayout.KIND] = AuditEventKind.RECORDS_LOST.code();
        AuditRecordLayout.putLong(lossRecord, AuditRecordLayout.TIMESTAMP_MILLIS,
                System.currentTimeMillis());
        AuditRecordLayout.putLong(lossRecord, AuditRecordLayout.LOST_RECORDS, since);
        onRecord(lossRecord, 0, lossRecord.length);
    }
}
