/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.io.WatchedFile;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.io.FileWatchDaemon;

import java.nio.file.Files;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A {@link ClientTokenStore} backed by a <b>directory</b>, one file per client: a file named
 * {@code <clientId>.token} holds that client's token record(s) (the same
 * {@code pbkdf2$...} grammar as {@link FileClientTokenStore}, one record per line or
 * {@code ;}-separated; multiple records = overlap rotation). Adding, rotating, or revoking a
 * client is dropping, editing, or deleting one file -- no shared file to rewrite.
 * <p>
 * The directory is watched on the shared {@link FileWatchDaemon} (a sentinel path inside it
 * makes the daemon watch the directory itself) plus a periodic safety rescan; a change
 * re-lists the directory and republishes on {@code equals}-change, with replay-on-subscribe.
 */
public final class DirectoryClientTokenStore implements ClientTokenStore, AutoCloseable {

    private static final String SUFFIX = ".token";

    private final Path dir;
    private final CopyOnWriteArrayList<Consumer<ClientTokens>> listeners = new CopyOnWriteArrayList<>();
    private final FileWatchDaemon.Registration registration;

    private final ReloadObserver observer;

    private volatile ClientTokens current;

    public DirectoryClientTokenStore(final Path dir) {
        this(dir, WatchedFile.DEFAULT_POLL_INTERVAL);
    }

    public DirectoryClientTokenStore(final Path dir, final ReloadObserver observer) {
        this(dir, WatchedFile.DEFAULT_POLL_INTERVAL, observer);
    }

    public DirectoryClientTokenStore(final Path dir, final Duration pollInterval) {
        this(dir, pollInterval, ReloadObserver.NONE);
    }

    public DirectoryClientTokenStore(final Path dir, final Duration pollInterval,
                                     final ReloadObserver observer) {
        this.observer = observer == null ? ReloadObserver.NONE : observer;
        this.dir = dir.toAbsolutePath();
        this.current = scan();
        // Register a sentinel path inside the directory so the daemon watches the directory
        // for created/modified/deleted client files, with the interval as a safety rescan.
        this.registration = FileWatchDaemon.shared().register(
                List.of(this.dir.resolve("__scan__")), pollInterval, this::refresh, this.observer);
    }

    @Override
    public ClientTokens snapshot() {
        return current;
    }

    @Override
    public synchronized void addListener(final Consumer<ClientTokens> listener) {
        listener.accept(current); // replay-on-subscribe
        listeners.add(listener);
    }

    /** Force an immediate rescan (e.g. in tests). */
    public void reloadNow() {
        refresh();
    }

    @Override
    public void close() {
        registration.close();
    }

    private synchronized void refresh() {
        final ClientTokens candidate;
        try {
            candidate = scan();
        } catch (final Exception e) {
            observer.reloadFailed("client-token directory " + dir, e);
            return; // keep the last good value
        }
        if (candidate.equals(current)) {
            return;
        }
        current = candidate;
        for (final Consumer<ClientTokens> listener : listeners) {
            listener.accept(candidate);
        }
    }

    private ClientTokens scan() {
        // Sorted by client id for a stable, deterministic snapshot.
        final Map<ClientId, List<TokenRecord>> byClient = new TreeMap<>();
        if (!Files.isDirectory(dir)) {
            return new ClientTokens(byClient);
        }
        try (Stream<Path> entries = Files.list(dir)) {
            for (final Path file : (Iterable<Path>) entries::iterator) {
                final String name = file.getFileName().toString();
                if (!name.endsWith(SUFFIX) || !Files.isRegularFile(file)) {
                    continue;
                }
                final ClientId clientId = ClientId.of(name.substring(0, name.length() - SUFFIX.length()));
                final List<TokenRecord> records = new ArrayList<>();
                for (final String line : Files.readAllLines(file)) {
                    records.addAll(TokenSpecs.parseLine(line));
                }
                if (!records.isEmpty()) {
                    byClient.put(clientId, records);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Cannot list client token directory " + dir, e);
        }
        return new ClientTokens(new LinkedHashMap<>(byClient));
    }
}
