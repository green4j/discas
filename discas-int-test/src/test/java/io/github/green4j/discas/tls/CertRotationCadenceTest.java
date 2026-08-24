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
import io.github.green4j.discas.TestAwait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CertRotationManager -- periodic hot-reload cadence")
class CertRotationCadenceTest {

    private static final ClusterId CLUSTER = ClusterId.of("cadence");
    private static final NodeId N1 = NodeId.of("1");
    private static final char[] PW = "changeit".toCharArray();

    @Test
    @DisplayName("A key store rewritten by an external agent is hot-swapped by the source's safety poll (no reloadNow)")
    void periodicPollPicksUpRewrittenFile(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final Path keyFile = dir.resolve("key.p12");
        final Path trustFile = dir.resolve("trust.p12");
        writeMaterial(ca.material(N1), keyFile, trustFile);

        // Fast safety poll so the source auto-detects a rewrite without a filesystem event.
        final FileTlsMaterialSource fileSource =
                new FileTlsMaterialSource(keyFile, PW, trustFile, PW, Duration.ofMillis(50));
        final TlsMaterial initial = fileSource.snapshot();
        final ReloadableTlsContext ctx = ReloadableTlsContext.create(initial);

        // Count how many times the manager applied material (initial replay = 1).
        final AtomicInteger applies = new AtomicInteger();
        final Executor counting = task -> {
            applies.incrementAndGet();
            task.run();
        };

        try (CertRotationManager mgr = new CertRotationManager(ctx, fileSource, initial,
                RenewalPolicy.defaults(), counting)) {
            mgr.start(); // replay-on-subscribe applies the initial material once
            assertEquals(1, applies.get(), "Initial material applied on start");

            Thread.sleep(300L); // several safety polls with no change -> no further apply
            assertEquals(1, applies.get(), "No reload while the file is unchanged");

            // External agent renews the leaf on disk (mtime advanced so the signature gate opens).
            renewKeyStore(ca.material(N1), keyFile);

            // The source's periodic safety poll must apply it promptly, with no reloadNow().
            TestAwait.until("the safety poll to apply the renewed key store", Duration.ofSeconds(10),
                    () -> {
                        if (applies.get() < 2) {
                            throw new IllegalStateException("applies=" + applies.get());
                        }
                    });
            assertTrue(applies.get() >= 2,
                    "Manager hot-swapped the rewritten key store via the source's periodic poll");
        } finally {
            fileSource.close();
        }
    }

    @Test
    @DisplayName("pastWarnDeadline warns warnBeforeExpiry before notAfter, clamped to the half-life")
    void warnDeadlineArithmetic() {
        // lifetime [1000, 101000]; lead 20s => deadline notAfter - 20s = 81000.
        assertFalse(CertRotationManager.pastWarnDeadline(1000, 101000, Duration.ofMillis(20000), 80999));
        assertTrue(CertRotationManager.pastWarnDeadline(1000, 101000, Duration.ofMillis(20000), 81000));
        assertTrue(CertRotationManager.pastWarnDeadline(1000, 101000, Duration.ofMillis(20000), 100999));
        // lead 80s exceeds half the 100s lifetime => clamped to 50s => deadline 51000.
        assertFalse(CertRotationManager.pastWarnDeadline(1000, 101000, Duration.ofMillis(80000), 50999));
        assertTrue(CertRotationManager.pastWarnDeadline(1000, 101000, Duration.ofMillis(80000), 51000));
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

    private static void renewKeyStore(final TlsMaterial material, final Path keyFile) throws Exception {
        final FileTime before = Files.getLastModifiedTime(keyFile);
        writeKeyStore(material, keyFile);
        Files.setLastModifiedTime(keyFile, FileTime.from(before.toInstant().plusSeconds(2)));
    }
}
