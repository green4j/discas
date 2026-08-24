/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.tls.CertIdentities;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A throwaway cluster CA for the {@link SecureClusterFileMembersExample}, built by
 * shelling out to the JDK's {@code keytool}. In production these keystores/trust
 * stores are provisioned by an external cert agent (cert-manager, step-ca, ...) --
 * this just stands in so the example is self-contained.
 */
final class ExampleCa {

    private static final String PASSWORD = "changeit";

    private final Path dir;
    private final ClusterId clusterId;
    private final Path caStore;
    private final Path caCert;

    ExampleCa(final Path dir, final ClusterId clusterId) throws Exception {
        this.dir = dir;
        this.clusterId = clusterId;
        this.caStore = dir.resolve("ca.p12");
        this.caCert = dir.resolve("ca.cer");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-keystore", caStore, "-storetype", "PKCS12", "-storepass", PASSWORD,
                "-dname", "CN=" + clusterId.value() + "-ca", "-ext", "bc:c", "-validity", "3650");
        keytool("-exportcert", "-rfc", "-alias", "ca", "-keystore", caStore,
                "-storepass", PASSWORD, "-file", caCert);
    }

    char[] password() {
        return PASSWORD.toCharArray();
    }

    /** Trust store holding the cluster CA (written once) */
    Path trustStore() throws Exception {
        final Path trust = dir.resolve("trust.p12");
        if (!Files.exists(trust)) {
            keytool("-importcert", "-noprompt", "-alias", "ca", "-keystore", trust,
                    "-storetype", "PKCS12", "-storepass", PASSWORD, "-file", caCert);
        }
        return trust;
    }

    /**
     * (Re)issue {@code node-<id>.p12} with a fresh CA-signed leaf carrying SAN
     * {@code discas://<cluster>/<node>}. Called again to renew (rotation)
     */
    Path issueNode(final NodeId nodeId) throws Exception {
        final Path store = dir.resolve("node-" + nodeId.value() + ".p12");
        final Path csr = dir.resolve("node-" + nodeId.value() + ".csr");
        final Path cert = dir.resolve("node-" + nodeId.value() + ".cer");
        Files.deleteIfExists(store); // fresh keystore each issuance
        keytool("-genkeypair", "-alias", "node", "-keyalg", "EC", "-groupname", "secp256r1",
                "-keystore", store, "-storetype", "PKCS12", "-storepass", PASSWORD,
                "-dname", "CN=" + nodeId.value(), "-validity", "3650");
        keytool("-certreq", "-alias", "node", "-keystore", store, "-storepass", PASSWORD, "-file", csr);
        keytool("-gencert", "-alias", "ca", "-keystore", caStore, "-storepass", PASSWORD,
                "-infile", csr, "-outfile", cert,
                "-ext", "san=uri:" + CertIdentities.sanUri(clusterId, nodeId), "-validity", "3650");
        keytool("-importcert", "-noprompt", "-alias", "ca", "-keystore", store,
                "-storepass", PASSWORD, "-file", caCert);
        keytool("-importcert", "-noprompt", "-alias", "node", "-keystore", store,
                "-storepass", PASSWORD, "-file", cert);
        return store;
    }

    private static void keytool(final Object... args) throws Exception {
        final List<String> cmd = new ArrayList<>();
        cmd.add(Paths.get(System.getProperty("java.home"), "bin", "keytool").toString());
        for (final Object a : args) {
            cmd.add(a.toString());
        }
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        final String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(60, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("keytool timed out: " + cmd);
        }
        if (p.exitValue() != 0) {
            throw new IllegalStateException("keytool failed (" + p.exitValue() + "): " + cmd + "\n" + out);
        }
    }
}
