/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls;

import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.transport.tls.ClientCertIdentities;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("ClientCertIdentities -- CN -> ClientId")
class ClientCertIdentitiesTest {

    @Test
    @DisplayName("Parses the ClientId from a certificate subject CN")
    void parsesCn(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(dir, ClusterId.of("acl-cluster"));
        final X509Certificate cert = ca.leafCert("web-1");

        assertEquals(ClientId.of("web-1"), ClientCertIdentities.parse(cert));
    }
}
