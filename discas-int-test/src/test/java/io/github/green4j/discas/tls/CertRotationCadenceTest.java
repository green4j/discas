/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.io.ReloadReport;
import io.github.green4j.discas.common.transport.tls.CertRotationManager;
import io.github.green4j.discas.common.transport.tls.FileTlsMaterialSource;
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.RenewalPolicy;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CertRotationManager -- what a reload of the material costs and reaches")
class CertRotationCadenceTest {

    private static final ClusterId CLUSTER = ClusterId.of("cadence");
    private static final NodeId N1 = NodeId.of("1");
    private static final char[] PW = "changeit".toCharArray();

    @Test
    @DisplayName("A key store renewed on disk reaches the TLS context on the next reload, and only then")
    void aReloadHotSwapsTheRenewedKeyStore(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final Path keyFile = dir.resolve("key.p12");
        final Path trustFile = dir.resolve("trust.p12");
        writeMaterial(ca.material(N1), keyFile, trustFile);

        final FileTlsMaterialSource fileSource =
                new FileTlsMaterialSource(keyFile, PW, trustFile, PW);
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

            assertEquals(ReloadReport.Outcome.UNCHANGED, fileSource.reloadNow().outcome());
            assertEquals(1, applies.get(), "Nothing was rewritten, so nothing was applied");

            // The external agent (cert-manager, step, a mounted secret) renews the leaf on disk.
            renewKeyStore(ca.material(N1), keyFile);
            assertEquals(1, applies.get(),
                    "A file on disk is not material: until a reload is asked for, nothing reads it");

            assertEquals(ReloadReport.Outcome.APPLIED, fileSource.reloadNow().outcome());
            assertEquals(2, applies.get(),
                    "and the reload hot-swaps the renewed key store into the live context");
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
        writeKeyStore(material, keyFile);
    }
}
