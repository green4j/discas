/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.dump;

/**
 * Told how far a dump has got, once per key it has dealt with.
 *
 * <p><b>Absolute, never relative</b>, and that is not a shortcut: a scan pages through the key
 * space with no count in front of it, so at no point during a dump does anyone know how many keys
 * are left. A percentage would have to be invented. (The other direction has it: a restore reads
 * the entry count out of the dump's trailer before it writes anything, and reports a fraction.)
 *
 * <p>Called from the dump's own worker threads, once per key, which can be tens of thousands of
 * times a second -- so an implementation that renders anything is expected to throttle itself.
 * Deciding how often to draw is the caller's business; counting is this one's.
 */
@FunctionalInterface
public interface DumpProgress {

    /** Ignores it. The default when a caller asks for a dump without asking to watch it. */
    DumpProgress NONE = (entries, bytes) -> { };

    /**
     * @param entriesWritten pairs written so far
     * @param bytesWritten   bytes the channel has accepted so far, framing included
     */
    void advanced(long entriesWritten, long bytesWritten);
}
