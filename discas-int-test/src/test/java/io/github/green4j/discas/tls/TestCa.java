/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;
import io.github.green4j.discas.common.transport.tls.CertIdentities;
import io.github.green4j.discas.common.transport.tls.TlsClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsMaterial;
import io.github.green4j.discas.common.transport.tls.TlsContexts;
import io.github.green4j.discas.common.transport.tls.TlsPeerSecurityProvider;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A throwaway cluster Certificate Authority for mTLS integration tests, built
 * in a temp directory by shelling out to the JDK's {@code keytool} (always
 * available, no third-party dependency). Issues per-node keystores whose leaf
 * cert carries the discas identity SAN ({@code discas://<cluster>/<node>}) and a
 * matching truststore.
 */
public final class TestCa {

    private static final String PASS = "changeit";

    private final Path dir;
    private final ClusterId clusterId;
    private final Path caStore;
    private final Path caCert;

    public TestCa(final Path dir, final ClusterId clusterId) throws Exception {
        this.dir = dir;
        this.clusterId = clusterId;
        this.caStore = dir.resolve("ca.p12");
        this.caCert = dir.resolve("ca.cer");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-keystore", caStore, "-storetype", "PKCS12", "-storepass", PASS,
                "-dname", "CN=" + clusterId.value() + "-ca", "-ext", "bc:c", "-validity", "3650");
        keytool("-exportcert", "-rfc", "-alias", "ca", "-keystore", caStore,
                "-storepass", PASS, "-file", caCert);
    }

    public char[] password() {
        return PASS.toCharArray();
    }

    /** Truststore containing this CA's certificate. */
    public Path trustStore() throws Exception {
        final Path trust = dir.resolve("trust.p12");
        if (!Files.exists(trust)) {
            keytool("-importcert", "-noprompt", "-alias", "ca", "-keystore", trust,
                    "-storetype", "PKCS12", "-storepass", PASS, "-file", caCert);
        }
        return trust;
    }

    /**
     * A keystore for {@code nodeId} holding a CA-signed leaf cert (SAN
     * {@code discas://<cluster>/<nodeId>}) plus the CA cert, so the KeyManager
     * presents a complete chain.
     */
    public Path nodeStore(final NodeId nodeId) throws Exception {
        return nodeStore(nodeId, "");
    }

    private int generation = 0;

    /**
     * A fresh, CA-signed {@link TlsMaterial} for {@code nodeId} -- a newly issued
     * leaf (new key/serial) with the same identity SAN, plus this CA's trust
     * store. Each call renews (distinct keystore file), so callers can rotate.
     */
    public TlsMaterial material(final NodeId nodeId) throws Exception {
        final Path store = nodeStore(nodeId, "-g" + (generation++));
        return new TlsMaterial(load(store), password(), load(trustStore()));
    }

    private Path nodeStore(final NodeId nodeId, final String suffix) throws Exception {
        final Path store = dir.resolve("node-" + nodeId.value() + suffix + ".p12");
        final Path csr = dir.resolve("node-" + nodeId.value() + suffix + ".csr");
        final Path cert = dir.resolve("node-" + nodeId.value() + suffix + ".cer");
        keytool("-genkeypair", "-alias", "node", "-keyalg", "EC", "-groupname", "secp256r1",
                "-keystore", store, "-storetype", "PKCS12", "-storepass", PASS,
                "-dname", "CN=" + nodeId.value(), "-validity", "3650");
        keytool("-certreq", "-alias", "node", "-keystore", store, "-storepass", PASS, "-file", csr);
        keytool("-gencert", "-alias", "ca", "-keystore", caStore, "-storepass", PASS,
                "-infile", csr, "-outfile", cert,
                "-ext", "san=uri:" + CertIdentities.sanUri(clusterId, nodeId), "-validity", "3650");
        keytool("-importcert", "-noprompt", "-alias", "ca", "-keystore", store,
                "-storepass", PASS, "-file", caCert);
        keytool("-importcert", "-noprompt", "-alias", "node", "-keystore", store,
                "-storepass", PASS, "-file", cert);
        return store;
    }

    /** A mTLS {@link PeerSecurityProvider} for {@code nodeId}, trusting this CA. */
    public PeerSecurityProvider provider(final NodeId nodeId) throws Exception {
        final KeyStore key = load(nodeStore(nodeId));
        final KeyStore trust = load(trustStore());
        final SSLContext context = TlsContexts.build(key, password(), trust);
        return new TlsPeerSecurityProvider(TlsConfig.of(context));
    }

    /**
     * A mTLS {@link ClientSecurityProvider} whose leaf certificate carries {@code CN=<cn>},
     * trusting this CA. Serves both the node's client-server (inbound) and the client
     * (outbound) roles from the same SSLContext.
     */
    public ClientSecurityProvider clientProvider(final String cn) throws Exception {
        final KeyStore key = load(nodeStore(NodeId.of(cn)));
        final KeyStore trust = load(trustStore());
        final SSLContext context = TlsContexts.build(key, password(), trust);
        return new TlsClientSecurityProvider(TlsConfig.of(context));
    }

    /** The leaf certificate issued for {@code cn} (subject {@code CN=<cn>}). */
    public X509Certificate leafCert(final String cn) throws Exception {
        return (X509Certificate) load(nodeStore(NodeId.of(cn))).getCertificate("node");
    }

    /**
     * An {@link SSLContext} presenting a leaf cert with {@code CN=<cn>} (key present, for the
     * server role) and trusting this CA. Pair with
     * {@code TlsClientSecurityProvider.serverAuthOnly(...)} for server-authenticated-TLS.
     */
    public SSLContext serverContext(final String cn) throws Exception {
        return TlsContexts.build(load(nodeStore(NodeId.of(cn))), password(), load(trustStore()));
    }

    public KeyStore load(final Path p12) throws Exception {
        final KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(p12)) {
            ks.load(in, password());
        }
        return ks;
    }

    private static void keytool(final Object... args) throws Exception {
        final List<String> cmd = new ArrayList<>();
        cmd.add(Paths.get(System.getProperty("java.home"), "bin", "keytool").toString());
        for (final Object a : args) {
            cmd.add(a.toString());
        }
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        final String out = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(60, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("Keytool timed out: " + cmd);
        }
        if (p.exitValue() != 0) {
            throw new IllegalStateException("Keytool failed (" + p.exitValue() + "): " + cmd + "\n" + out);
        }
    }
}
