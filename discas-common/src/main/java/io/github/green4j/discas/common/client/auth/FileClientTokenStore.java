/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.io.WatchedFile;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.io.WatchedFileSource;

import java.io.ByteArrayInputStream;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * A {@link ClientTokenStore} backed by a config file, watched and hot-reloaded so tokens
 * can be rotated on disk with no restart. Format (java properties, one line per client;
 * multiple tokens for overlap rotation are separated by {@code ;}):
 * <pre>
 *   client.web-1 = pbkdf2$210000$&lt;saltB64&gt;$&lt;hashB64&gt;$&lt;notAfterEpochMs&gt;
 *   client.web-2 = pbkdf2$210000$&lt;saltB64&gt;$&lt;hashB64&gt;$&lt;notAfter&gt; ; pbkdf2$...$&lt;notAfter&gt;
 * </pre>
 *
 * <p>All watching, change-gating, replay-on-subscribe and fail-fast-on-initial-load lives in the
 * shared {@link WatchedFileSource}; this class supplies only the parser.
 */
public final class FileClientTokenStore implements ClientTokenStore, AutoCloseable {


    private final WatchedFileSource<ClientTokens> source;

    public FileClientTokenStore(final Path file) {
        this(file, WatchedFile.DEFAULT_POLL_INTERVAL);
    }

    public FileClientTokenStore(final Path file, final ReloadObserver observer) {
        this(file, WatchedFile.DEFAULT_POLL_INTERVAL, observer);
    }

    public FileClientTokenStore(final Path file, final Duration pollInterval) {
        this(file, pollInterval, ReloadObserver.NONE);
    }

    public FileClientTokenStore(final Path file, final Duration pollInterval,
                                final ReloadObserver observer) {
        final Path abs = file.toAbsolutePath();
        this.source = new WatchedFileSource<>(
                List.of(abs), pollInterval, contents -> parse(abs, contents.get(0)), observer);
    }

    @Override
    public ClientTokens snapshot() {
        return source.snapshot();
    }

    @Override
    public void addListener(final Consumer<ClientTokens> listener) {
        source.addListener(listener);
    }

    /** Force an immediate check-and-reload (e.g. on an ops signal or in tests). */
    public void reloadNow() {
        source.reloadNow();
    }

    @Override
    public void close() {
        source.close();
    }

    private static ClientTokens parse(final Path file, final byte[] bytes) {
        final Properties props = new Properties();
        try {
            props.load(new ByteArrayInputStream(bytes));
        } catch (final Exception e) {
            throw new RuntimeException("Cannot read client token file " + file, e);
        }
        final Map<ClientId, List<TokenRecord>> byClient = new LinkedHashMap<>();
        for (final String name : props.stringPropertyNames()) {
            if (!name.startsWith("client.")) {
                continue;
            }
            final ClientId clientId = ClientId.of(name.substring("client.".length()));
            final List<TokenRecord> records = TokenSpecs.parseLine(props.getProperty(name));
            if (!records.isEmpty()) {
                byClient.put(clientId, records);
            }
        }
        return new ClientTokens(byClient);
    }
}
