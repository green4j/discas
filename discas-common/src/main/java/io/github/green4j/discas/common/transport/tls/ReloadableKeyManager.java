/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * An {@link X509ExtendedKeyManager} that delegates to an atomically-swappable
 * inner manager. Rotating the node's cert/key replaces the delegate (see
 * {@link #setDelegate}); handshakes in progress keep the delegate they read, and
 * new handshakes pick up the rotated material -- the {@code SSLContext} itself is
 * never rebuilt, so live connections are undisturbed.
 */
public final class ReloadableKeyManager extends X509ExtendedKeyManager {

    private volatile X509ExtendedKeyManager delegate;

    public ReloadableKeyManager(final X509ExtendedKeyManager delegate) {
        this.delegate = delegate;
    }

    public void setDelegate(final X509ExtendedKeyManager next) {
        this.delegate = next;
    }

    @Override
    public String[] getClientAliases(final String keyType, final Principal[] issuers) {
        return delegate.getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(final String[] keyType, final Principal[] issuers, final Socket socket) {
        return delegate.chooseClientAlias(keyType, issuers, socket);
    }

    @Override
    public String[] getServerAliases(final String keyType, final Principal[] issuers) {
        return delegate.getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(final String keyType, final Principal[] issuers, final Socket socket) {
        return delegate.chooseServerAlias(keyType, issuers, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(final String alias) {
        return delegate.getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(final String alias) {
        return delegate.getPrivateKey(alias);
    }

    @Override
    public String chooseEngineClientAlias(final String[] keyType, final Principal[] issuers, final SSLEngine engine) {
        return delegate.chooseEngineClientAlias(keyType, issuers, engine);
    }

    @Override
    public String chooseEngineServerAlias(final String keyType, final Principal[] issuers, final SSLEngine engine) {
        return delegate.chooseEngineServerAlias(keyType, issuers, engine);
    }
}
