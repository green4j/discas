/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.tls.CertRotationManager;
import io.github.green4j.discas.common.transport.tls.FileTlsMaterialSource;
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.RenewalPolicy;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("FileTlsMaterialSource -- push rotated material from disk (replay-on-subscribe + fingerprint gate)")
class FileTlsMaterialSourceTest {

    private static final ClusterId CLUSTER = ClusterId.of("file-src");
    private static final NodeId N1 = NodeId.of("1");
    private static final char[] PW = "changeit".toCharArray();

    @Test
    @DisplayName("Subscribe replays the current material, then a changed key store pushes fresh material once")
    void pushesNewMaterialWhenFileChanges(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final Path keyFile = dir.resolve("key.p12");
        final Path trustFile = dir.resolve("trust.p12");
        writeMaterial(ca.material(N1), keyFile, trustFile);

        try (FileTlsMaterialSource source = new FileTlsMaterialSource(keyFile, PW, trustFile, PW)) {
            final TlsMaterial first = source.snapshot();
            assertNotNull(first);
            final BigInteger firstSerial = first.leafCertificate().getSerialNumber();

            final List<TlsMaterial> pushed = new CopyOnWriteArrayList<>();
            source.addListener(pushed::add); // replay-on-subscribe delivers the current material
            assertEquals(1, pushed.size(), "Replay-on-subscribe delivers the current material");

            source.reloadNow();
            assertEquals(1, pushed.size(), "Unchanged files => nothing new");

            // Renew: overwrite the key store with a fresh leaf (new serial).
            renewKeyStore(ca.material(N1), keyFile);
            source.reloadNow();
            assertEquals(2, pushed.size(), "Changed key store => fresh material pushed");
            assertNotEquals(firstSerial, pushed.get(1).leafCertificate().getSerialNumber());

            source.reloadNow();
            assertEquals(2, pushed.size(), "No further change => nothing new");
        }
    }

    @Test
    @DisplayName("CertRotationManager applies the material on reload; unchanged files apply nothing")
    void rotationManagerAppliesOnReload(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final Path keyFile = dir.resolve("key.p12");
        final Path trustFile = dir.resolve("trust.p12");
        writeMaterial(ca.material(N1), keyFile, trustFile);

        // Counting executor: the manager must route every apply through the executor it was given
        // rather than applying inline, and this counter is what says so -- an inline apply leaves
        // it at zero.
        final AtomicInteger applies = new AtomicInteger();
        final Executor counting = task -> {
            applies.incrementAndGet();
            task.run();
        };

        try (FileTlsMaterialSource source = new FileTlsMaterialSource(keyFile, PW, trustFile, PW)) {
            final TlsMaterial initial = source.snapshot();
            final ReloadableTlsContext ctx = ReloadableTlsContext.create(initial);
            try (CertRotationManager mgr =
                         new CertRotationManager(ctx, source, initial, RenewalPolicy.defaults(), counting)) {
                mgr.start(); // subscribes -> replay-on-subscribe applies the initial material
                assertEquals(1, applies.get(), "Initial material applied on start");

                source.reloadNow();
                assertEquals(1, applies.get(), "No file change => no rotation");

                renewKeyStore(ca.material(N1), keyFile);
                source.reloadNow();
                assertEquals(2, applies.get(), "File changed => rotation applied");

                source.reloadNow();
                assertEquals(2, applies.get(), "Already applied => no rotation");
            }
        }
    }

    private static void writeMaterial(final TlsMaterial material, final Path keyFile,
                                      final Path trustFile) throws Exception {
        writeKeyStore(material, keyFile);
        try (OutputStream os = Files.newOutputStream(trustFile)) {
            material.trustStore().store(os, PW);
        }
    }

    private static void writeKeyStore(final TlsMaterial material, final Path keyFile) throws Exception {
        try (OutputStream os = Files.newOutputStream(keyFile)) {
            material.keyStore().store(os, PW);
        }
    }

    /**
     * Rewrite the key store and advance its last-modified time so the watch signature
     * (mtime + size) changes deterministically even where a same-second rewrite yields an
     * identical size -- the content fingerprint then gates the actual push.
     */
    private static void renewKeyStore(final TlsMaterial material, final Path keyFile) throws Exception {
        final FileTime before = Files.getLastModifiedTime(keyFile);
        writeKeyStore(material, keyFile);
        Files.setLastModifiedTime(keyFile, FileTime.from(before.toInstant().plusSeconds(2)));
    }
}
