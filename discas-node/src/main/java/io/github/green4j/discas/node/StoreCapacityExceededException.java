/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

/**
 * Replay found this node's own log holds more than the heap it was given: recovering it in full
 * would take the store past {@code NodeConfig.storeHeapFraction} of max heap.
 * <p>
 * <b>Thrown so the node fails at startup rather than while serving.</b> The alternative is a member
 * that replays as far as it can and is killed by the JVM somewhere in the middle -- a death with no
 * diagnosis, at a moment nobody chose, on a node its peers were counting as a member. Refusing to
 * start is the same trade the WAL makes when it degrades: stop being a member outright rather than
 * become an unreliable one.
 * <p>
 * The peak is real, not an artefact of replaying history: each entry is applied in order, so a key
 * overwritten or purged later is genuinely held until that record arrives. A log whose live set ever
 * exceeded the budget did once occupy that much, and replaying it occupies it again.
 * <p>
 * Not a storage fault -- the log is intact and readable. What is wrong is the size of the heap
 * against the size of the data, which is why it reports as {@code CAPACITY} rather than
 * {@code STORAGE}: the action is to give the node more room, never to delete anything under
 * {@code --wal-dir}.
 */
public final class StoreCapacityExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long storedBytes;
    private final long capacityBytes;

    StoreCapacityExceededException(final long storedBytes, final long capacityBytes) {
        super("replay reached an estimated " + storedBytes + " bytes of store against a budget of "
                + capacityBytes + "; this node cannot hold its own log");
        this.storedBytes = storedBytes;
        this.capacityBytes = capacityBytes;
    }

    /** What the store was estimated at when replay stopped. */
    public long storedBytes() {
        return storedBytes;
    }

    /** The budget it passed. */
    public long capacityBytes() {
        return capacityBytes;
    }
}
