/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.io.ReloadableFiles;
import io.github.green4j.discas.common.io.ReloadReport;

import java.nio.file.Files;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.nio.file.Path;
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
 * A directory rather than a fixed set of paths, so this registers with {@link ReloadableFiles} on
 * its own rather than through {@code ReloadableFileSource}: the whole directory is rescanned, which
 * is how a client file appearing, changing or being deleted is noticed. What it shares with every
 * other source is what matters -- the scan is complete before anything is published, and it
 * republishes only on an {@code equals}-change, with replay-on-subscribe.
 */
public final class DirectoryClientTokenStore implements ClientTokenStore, AutoCloseable {

    private static final String SUFFIX = ".token";

    private final Path dir;
    private final CopyOnWriteArrayList<Consumer<ClientTokens>> listeners = new CopyOnWriteArrayList<>();
    private final ReloadableFiles.Registration registration;

    private final ReloadObserver observer;

    private volatile ClientTokens current;
    private ClientTokens staged;

    public DirectoryClientTokenStore(final Path dir) {
        this(dir, ReloadObserver.NONE);
    }

    public DirectoryClientTokenStore(final Path dir, final ReloadObserver observer) {
        this.observer = observer == null ? ReloadObserver.NONE : observer;
        this.dir = dir.toAbsolutePath();
        this.current = scan();
        this.registration = ReloadableFiles.shared().register(asSource());
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

    /** Rescan this directory alone and apply what it holds. */
    public ReloadReport.Entry reloadNow() {
        final ReloadReport.Entry entry = prepare();
        if (entry.outcome().accepted()) {
            commit();
        } else {
            discard();
        }
        return entry;
    }

    @Override
    public void close() {
        registration.close();
    }

    private ReloadableFiles.Source asSource() {
        return new ReloadableFiles.Source() {
            @Override
            public String source() {
                return DirectoryClientTokenStore.this.source();
            }

            @Override
            public ReloadReport.Entry prepare() {
                return DirectoryClientTokenStore.this.prepare();
            }

            @Override
            public void commit() {
                DirectoryClientTokenStore.this.commit();
            }

            @Override
            public void discard() {
                DirectoryClientTokenStore.this.discard();
            }
        };
    }

    private synchronized ReloadReport.Entry prepare() {
        staged = null;
        final ClientTokens candidate;
        try {
            candidate = scan();
        } catch (final Exception e) {
            observer.reloadFailed(source(), e);
            return new ReloadReport.Entry(source(), ReloadReport.Outcome.FAILED,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
        if (candidate.equals(current)) {
            final String detail = "the directory holds what is already in force; not applied";
            observer.reloadUnchanged(source(), detail);
            return new ReloadReport.Entry(source(), ReloadReport.Outcome.UNCHANGED, detail);
        }
        staged = candidate;
        return new ReloadReport.Entry(source(), ReloadReport.Outcome.APPLIED, candidate.summary());
    }

    private synchronized void commit() {
        final ClientTokens candidate = staged;
        staged = null;
        if (candidate == null) {
            return;
        }
        current = candidate;
        for (final Consumer<ClientTokens> listener : listeners) {
            try {
                listener.accept(candidate);
            } catch (final Exception e) {
                observer.checkFailed(source(), e);
            }
        }
        observer.reloaded(source(), "applied -- " + candidate.summary());
    }

    private synchronized void discard() {
        staged = null;
    }

    private String source() {
        return "client-token directory " + dir;
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
