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
import io.github.green4j.discas.client.dump.ClusterLoad.LoadSummary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("Loading a dump into a running cluster -- a merge, unless cleanup makes it a replacement")
class ClusterLoadTest {

    private static final long TIMEOUT_MS = 10_000L;

    @TempDir
    Path directory;

    private TestCluster cluster;
    private DisCasClient client;

    @BeforeEach
    void startCluster() throws Exception {
        cluster = new TestCluster(3);
        cluster.start();
        cluster.awaitReady();
        client = cluster.client(0);
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @DisplayName("Overwrites the dump's keys and leaves every other key alone")
    void loadIsAMerge() throws Exception {
        put("users/1", "alice");
        put("users/2", "bob");
        final Path dump = dump("merge.dump");

        put("users/1", "changed-since");
        put("users/9", "written-since");

        final LoadSummary summary = ClusterLoad.load(client, dump, null);

        assertEquals(2, summary.written());
        assertEquals(0, summary.deleted());
        assertEquals("alice", get("users/1"), "The dump's value did not overwrite the newer one");
        assertEquals("bob", get("users/2"));
        assertEquals("written-since", get("users/9"), "A key outside the dump was touched");
    }

    @Test
    @DisplayName("Cleanup deletes what the dump did not carry, and only under the given prefixes")
    void cleanupIsBoundedByItsPrefixes() throws Exception {
        put("users/1", "alice");
        final Path dump = dump("scoped.dump");

        put("users/9", "stale");
        put("orders/9", "not-in-scope");

        final LoadSummary summary = ClusterLoad.loadAndCleanup(client, dump,
                Collections.singletonList(TestBytes.utf8("users/")), null);

        assertEquals(1, summary.written());
        assertEquals(1, summary.deleted());
        assertEquals("alice", get("users/1"));
        assertNull(get("users/9"), "A stale key under the cleaned prefix survived");
        assertEquals("not-in-scope", get("orders/9"), "Cleanup reached outside its prefixes");
    }

    @Test
    @DisplayName("Cleanup with no prefixes makes the whole key space the dump's")
    void cleanupWithoutPrefixesTakesTheWholeKeySpace() throws Exception {
        put("users/1", "alice");
        final Path dump = dump("whole.dump");

        put("orders/9", "stale");
        put("cache/tmp", "stale");

        final LoadSummary summary =
                ClusterLoad.loadAndCleanup(client, dump, Collections.emptyList(), null);

        assertEquals(1, summary.written());
        assertEquals(2, summary.deleted());
        assertEquals("alice", get("users/1"));
        assertNull(get("orders/9"));
        assertNull(get("cache/tmp"));
    }

    @Test
    @DisplayName("Progress counts every pair, and the total is known before the first write")
    void progressIsRelative() throws Exception {
        put("users/1", "alice");
        put("users/2", "bob");
        put("users/3", "carol");
        final Path dump = dump("progress.dump");

        final long[] lastWritten = {0};
        final long[] lastTotal = {0};
        ClusterLoad.load(client, dump, (written, total) -> {
            lastWritten[0] = written;
            lastTotal[0] = total;
        });

        assertEquals(3, lastWritten[0]);
        assertEquals(3, lastTotal[0]);
    }

    private Path dump(final String fileName) throws Exception {
        return dump(fileName, Collections.emptyList());
    }

    private Path dump(final String fileName, final List<ByteBuffer> prefixes) throws Exception {
        final Path file = directory.resolve(fileName);
        try (FileChannel channel = FileChannel.open(file,
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            ClusterDump.dump(client, prefixes, channel).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        return file;
    }

    private void put(final String key, final String value) throws Exception {
        await(client.put(key, TestBytes.utf8(value)));
    }

    private String get(final String key) throws Exception {
        return TestBytes.string(await(client.get(key)).value());
    }

    private static <T> T await(final CompletableFuture<T> future) throws Exception {
        return future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
