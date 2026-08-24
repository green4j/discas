/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls.stepca;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.tls.CertIdentities;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A dockerized <a href="https://smallstep.com/docs/step-ca">smallstep step-ca</a>
 * used by the CA-rotation integration test to issue real, short-lived certs with
 * the discas identity SAN. Certs are bundled into PKCS12 inside the container
 * (via the {@code step} CLI) and loaded directly as Java {@link KeyStore}s.
 */
public final class StepCa implements AutoCloseable {

    private static final String IMAGE = "smallstep/step-ca:0.25.2";
    private static final String PASSWORD = "rotation-test-pw";
    private static final String PW_FILE = "/tmp/pw";
    private static final String ROOT = "/home/step/certs/root_ca.crt";
    private static final String INTER = "/home/step/certs/intermediate_ca.crt";

    private final GenericContainer<?> container;
    private final ClusterId clusterId;
    private final AtomicInteger seq = new AtomicInteger();
    private volatile KeyStore trustStore;
    private volatile boolean pwFileReady;

    public StepCa(final ClusterId clusterId) {
        this.clusterId = clusterId;
        this.container = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withEnv("DOCKER_STEPCA_INIT_NAME", "discas-ca")
                .withEnv("DOCKER_STEPCA_INIT_DNS_NAMES", "localhost,step-ca")
                .withEnv("DOCKER_STEPCA_INIT_PROVISIONER_NAME", "admin")
                .withEnv("DOCKER_STEPCA_INIT_PASSWORD", PASSWORD)
                .withEnv("DOCKER_STEPCA_INIT_ACME", "false")
                .withExposedPorts(9000)
                .waitingFor(Wait.forLogMessage(".*Serving HTTPS.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    public void start() {
        container.start();
    }

    /** Write the CA/provisioner password into a file inside the container (once). */
    private synchronized void ensurePwFile() throws Exception {
        if (!pwFileReady) {
            exec("sh", "-c", "printf '%s' '" + PASSWORD + "' > " + PW_FILE);
            pwFileReady = true;
        }
    }

    /**
     * Issue a fresh leaf for {@code nodeId} (new key/serial each call) with SAN
     * {@code discas://<cluster>/<node>}, and return it bundled with the CA trust
     * store as {@link TlsMaterial}. Calling again renews (rotation).
     */
    public TlsMaterial issue(final NodeId nodeId) throws Exception {
        ensurePwFile();
        final String tag = nodeId.value() + "-" + seq.incrementAndGet();
        final String leafCrt = "/tmp/leaf-" + tag + ".crt";
        final String leafKey = "/tmp/leaf-" + tag + ".key";
        final String p12 = "/tmp/node-" + tag + ".p12";
        final String sanUri = CertIdentities.sanUri(clusterId, nodeId);

        exec("step", "ca", "certificate", "node-" + nodeId.value(), leafCrt, leafKey,
                "--ca-url", "https://localhost:9000", "--root", ROOT,
                "--san", sanUri,
                "--provisioner", "admin", "--provisioner-password-file", PW_FILE,
                "--kty", "RSA", "--size", "2048",
                "--not-after", "2h", "--force"); // under step-ca's 24h default max

        exec("step", "certificate", "p12", p12, leafCrt, leafKey,
                "--ca", INTER, "--ca", ROOT,
                "--password-file", PW_FILE, "--force");

        final KeyStore keyStore = load(copyOut(p12));
        return new TlsMaterial(keyStore, PASSWORD.toCharArray(), trustStore());
    }

    /** CA trust store (root), built once and reused. */
    public synchronized KeyStore trustStore() throws Exception {
        if (trustStore == null) {
            ensurePwFile();
            final String trustP12 = "/tmp/trust.p12";
            exec("step", "certificate", "p12", trustP12, "--ca", ROOT,
                    "--password-file", PW_FILE, "--force");
            trustStore = load(copyOut(trustP12));
        }
        return trustStore;
    }

    private void exec(final String... cmd) throws Exception {
        final Container.ExecResult result = container.execInContainer(cmd);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Step command failed (" + result.getExitCode() + "): "
                    + String.join(" ", cmd) + "\nstdout: " + result.getStdout()
                    + "\nstderr: " + result.getStderr());
        }
    }

    private byte[] copyOut(final String path) throws Exception {
        return container.copyFileFromContainer(path, InputStream::readAllBytes);
    }

    private static KeyStore load(final byte[] p12Bytes) throws Exception {
        final KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(new ByteArrayInputStream(p12Bytes), PASSWORD.toCharArray());
        return ks;
    }

    @Override
    public void close() {
        container.stop();
    }
}
