/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * An {@link X509ExtendedTrustManager} that delegates to an atomically-swappable
 * inner manager. Swapping the delegate (see {@link #setDelegate}) rotates the set
 * of trusted CAs for new handshakes without rebuilding the {@code SSLContext}.
 * During a CA rotation the new delegate should trust <em>both</em> the old and
 * new CA (overlapping validity) so no in-flight handshake is rejected.
 */
public final class ReloadableTrustManager extends X509ExtendedTrustManager {

    private volatile X509ExtendedTrustManager delegate;

    public ReloadableTrustManager(final X509ExtendedTrustManager delegate) {
        this.delegate = delegate;
    }

    public void setDelegate(final X509ExtendedTrustManager next) {
        this.delegate = next;
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType)
            throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType)
            throws CertificateException {
        delegate.checkServerTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType, final Socket socket)
            throws CertificateException {
        delegate.checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType, final Socket socket)
            throws CertificateException {
        delegate.checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine)
            throws CertificateException {
        delegate.checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine)
            throws CertificateException {
        delegate.checkServerTrusted(chain, authType, engine);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }
}
