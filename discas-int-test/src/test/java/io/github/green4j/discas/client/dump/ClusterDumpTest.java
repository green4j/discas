/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.dump;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestCluster;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.dump.ClusterDump.DumpSummary;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.common.dump.DumpReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Dumping a cluster -- live pairs, and nothing that only meant something here")
class ClusterDumpTest {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    @TempDir
    Path directory;

    private TestCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @DisplayName("Carries the live pairs; a deleted key is not one of them")
    void dumpsLivePairsOnly() throws Exception {
        final DisCasClient client = startCluster();
        put(client, "users/1", "alice");
        put(client, "users/2", "bob");
        put(client, "orders/9", "widget");

        // Deleted after being written: what is left is a tombstone, and a tombstone is not a pair.
        put(client, "users/3", "was-here");
        await(client.delete("users/3"));

        final DumpSummary summary = dump(client, Collections.emptyList(), "all.dump");

        assertEquals(3, summary.entries());
        final Map<String, String> dumped = read("all.dump");
        assertEquals(3, dumped.size());
        assertEquals("alice", dumped.get("users/1"));
        assertEquals("bob", dumped.get("users/2"));
        assertEquals("widget", dumped.get("orders/9"));
        assertFalse(dumped.containsKey("users/3"), "A deleted key travelled");
    }

    @Test
    @DisplayName("A lock is a value: inside an asked prefix it travels, outside one it does not")
    void locksAreOrdinaryPairs() throws Exception {
        // Deliberately not filtered out by recognising the value. Only the application knows what
        // lives where, which is why the dump space is named by prefixes -- and the prefix set is
        // the whole of the operator's control over whether locks are carried.
        final DisCasClient client = startCluster();
        put(client, "users/1", "alice");
        final LockAcquireResult lock = await(client.tryLock("locks/nightly", Duration.ofMinutes(5), "dump-test"));
        assertTrue(lock.acquired());

        final DumpSummary everything = dump(client, Collections.emptyList(), "all.dump");
        assertEquals(2, everything.entries());
        assertTrue(read("all.dump").containsKey("locks/nightly"),
                "A lock inside the dumped space was dropped by something that read its value");

        final DumpSummary dataOnly =
                dump(client, Collections.singletonList(TestBytes.utf8("users/")), "data.dump");
        assertEquals(1, dataOnly.entries());
        assertFalse(read("data.dump").containsKey("locks/nightly"));
    }

    @Test
    @DisplayName("Carries the prefixes asked for and nothing else, and records what was asked")
    void dumpsOnlyTheAskedPrefixes() throws Exception {
        final DisCasClient client = startCluster();
        put(client, "users/1", "alice");
        put(client, "orders/9", "widget");
        put(client, "cache/tmp", "junk");

        final DumpSummary summary = dump(client,
                Arrays.asList(TestBytes.utf8("users/"), TestBytes.utf8("orders/")), "some.dump");

        assertEquals(2, summary.entries());
        final Map<String, String> dumped = read("some.dump");
        assertEquals(2, dumped.size());
        assertTrue(dumped.containsKey("users/1"));
        assertTrue(dumped.containsKey("orders/9"));

        try (DumpReader reader = DumpReader.open(directory.resolve("some.dump"))) {
            final List<ByteBuffer> asked = reader.header().prefixes();
            assertEquals(2, asked.size());
            assertEquals("users/", TestBytes.string(asked.get(0)));
            assertEquals("orders/", TestBytes.string(asked.get(1)));
        }
    }

    @Test
    @DisplayName("A key under two of the asked prefixes is carried once")
    void overlappingPrefixesDoNotDuplicateAKey() throws Exception {
        final DisCasClient client = startCluster();
        put(client, "users/1", "alice");
        put(client, "users/2", "bob");

        final DumpSummary summary = dump(client,
                Arrays.asList(TestBytes.utf8("users/"), TestBytes.utf8("users/1")), "overlap.dump");

        assertEquals(2, summary.entries());
        assertEquals(2, read("overlap.dump").size());
    }

    private DisCasClient startCluster() throws Exception {
        cluster = new TestCluster(3);
        cluster.start();
        cluster.awaitReady();
        return cluster.client(0);
    }

    private DumpSummary dump(final DisCasClient client,
                             final List<ByteBuffer> prefixes,
                             final String fileName) throws Exception {
        try (FileChannel channel = FileChannel.open(directory.resolve(fileName),
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            return ClusterDump.dump(client, prefixes, channel)
                    .get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** The dump's pairs, in the order it carries them. */
    private Map<String, String> read(final String fileName) throws IOException {
        final Map<String, String> pairs = new LinkedHashMap<>();
        try (DumpReader reader = DumpReader.open(directory.resolve(fileName))) {
            reader.forEachEntry((key, value) ->
                    pairs.put(TestBytes.string(key), TestBytes.string(value)));
            assertEquals(reader.entryCount(), pairs.size(), "The dump repeated a key");
        }
        return pairs;
    }

    private static void put(final DisCasClient client, final String key, final String value)
            throws Exception {
        await(client.put(key, TestBytes.utf8(value)));
    }

    private static <T> T await(final CompletableFuture<T> future)
            throws Exception {
        return future.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
