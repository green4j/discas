/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.io.Reloadable;
import io.github.green4j.discas.common.io.WatchedFileSource;

import java.io.ByteArrayInputStream;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * A {@link Reloadable} source of this node's TLS material, loaded from a key store and a
 * trust store on disk. This is the operational renewal path: an external agent
 * (cert-manager, {@code step ca renew}, a SPIFFE helper, a mounted secret, ...) writes
 * newly-issued material to the same paths and it is picked up here -- always carrying the
 * <b>same</b> {@code node_id} SAN and trusting the current cluster CA set (old and new during a
 * CA rotation). A change in either store covers a CA rotation, trust first and key later.
 * Certificate lifetime is the operator's choice, long-lived leaves being the default expectation;
 * in short-lived deployments letting a cert lapse doubles as revocation, but the primary
 * revocation here is member eviction.
 * <p>
 * The watching, change-gating, replay-on-subscribe and fail-fast-on-initial-load all live in the
 * shared {@link WatchedFileSource}; this class supplies only the key-store/trust-store parser. Use
 * {@link #snapshot()} for the initial material when building the {@link ReloadableTlsContext} and
 * {@link CertRotationManager}.
 */
public final class FileTlsMaterialSource implements Reloadable<TlsMaterial>, AutoCloseable {

    /**
     * Deliberately slower than {@link io.github.green4j.discas.common.io.WatchedFile#DEFAULT_POLL_INTERVAL},
     * which the hot-reloaded configuration files use: a key or trust store rotates on the order
     * of days, and re-reading one is far more expensive than re-reading a text file.
     */
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(60);

    private final WatchedFileSource<TlsMaterial> source;

    public FileTlsMaterialSource(final Path keyStorePath, final char[] keyStorePassword,
                                 final Path trustStorePath, final char[] trustStorePassword) {
        this(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                DEFAULT_POLL_INTERVAL, ReloadObserver.NONE);
    }

    /** Default poll interval, with diagnostics routed to {@code observer}. */
    public FileTlsMaterialSource(final Path keyStorePath, final char[] keyStorePassword,
                                 final Path trustStorePath, final char[] trustStorePassword,
                                 final ReloadObserver observer) {
        this(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                DEFAULT_POLL_INTERVAL, observer);
    }

    /** An explicit safety-poll interval (the shared file watch hosts the check). */
    public FileTlsMaterialSource(final Path keyStorePath, final char[] keyStorePassword,
                                 final Path trustStorePath, final char[] trustStorePassword,
                                 final Duration pollInterval) {
        this(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                pollInterval, ReloadObserver.NONE);
    }

    public FileTlsMaterialSource(final Path keyStorePath, final char[] keyStorePassword,
                                 final Path trustStorePath, final char[] trustStorePassword,
                                 final Duration pollInterval,
                                 final ReloadObserver observer) {
        // PKCS12 with the key password equal to the store password -- the only combination any
        // caller has ever asked for.
        this(keyStorePath, keyStorePassword, keyStorePassword, trustStorePath,
                trustStorePassword, "PKCS12", pollInterval, observer);
    }

    private FileTlsMaterialSource(final Path keyStorePath, final char[] keyStorePassword,
                                  final char[] keyPassword, final Path trustStorePath,
                                  final char[] trustStorePassword, final String storeType,
                                  final Duration pollInterval,
                                  final ReloadObserver observer) {
        final char[] keyStorePw = keyStorePassword.clone();
        final char[] keyPw = keyPassword.clone();
        final char[] trustStorePw = trustStorePassword.clone();
        this.source = new WatchedFileSource<>(
                List.of(keyStorePath, trustStorePath), pollInterval,
                contents -> parse(contents, keyStorePw, keyPw, trustStorePw, storeType), observer);
    }

    @Override
    public TlsMaterial snapshot() {
        return source.snapshot();
    }

    @Override
    public void addListener(final Consumer<TlsMaterial> listener) {
        source.addListener(listener);
    }

    /** Force an immediate check-and-push (e.g. on an ops signal or in tests). */
    public void reloadNow() {
        source.reloadNow();
    }

    @Override
    public void close() {
        source.close();
    }

    private static TlsMaterial parse(final List<byte[]> contents,
                                     final char[] keyStorePassword, final char[] keyPassword,
                                     final char[] trustStorePassword, final String storeType)
            throws Exception {
        final KeyStore keyStore = load(contents.get(0), keyStorePassword, storeType);
        final KeyStore trustStore = load(contents.get(1), trustStorePassword, storeType);
        return new TlsMaterial(keyStore, keyPassword, trustStore);
    }

    private static KeyStore load(final byte[] bytes, final char[] password,
                                 final String storeType) throws Exception {
        final KeyStore ks = KeyStore.getInstance(storeType);
        ks.load(new ByteArrayInputStream(bytes), password);
        return ks;
    }
}
