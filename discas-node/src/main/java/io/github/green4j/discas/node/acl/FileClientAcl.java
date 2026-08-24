/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.io.WatchedFile;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.io.WatchedFileSource;

import java.io.ByteArrayInputStream;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * A {@link ClientAcl} backed by a config file, watched and hot-reloaded. Format (java
 * properties, one line per client; grants separated by {@code ;}, each grant is
 * {@code <keyPrefix>:<ops>} where ops are the {@link ClientOp} letters, e.g. {@code GPCDS}):
 * <pre>
 *   acl.web-1 = app/:GPCD ; session/:GPCDS
 *   acl.reporter = report/:GS
 * </pre>
 * The prefix may itself contain {@code :} -- the split is on the <b>last</b> colon.
 *
 * <p>All watching, change-gating, replay-on-subscribe and fail-fast-on-initial-load lives in the
 * shared {@link WatchedFileSource}; this class supplies only the parser.
 */
public final class FileClientAcl implements ClientAcl, AutoCloseable {


    private final WatchedFileSource<ClientAclSnapshot> source;

    public FileClientAcl(final Path file) {
        this(file, WatchedFile.DEFAULT_POLL_INTERVAL);
    }

    public FileClientAcl(final Path file, final ReloadObserver observer) {
        this(file, WatchedFile.DEFAULT_POLL_INTERVAL, observer);
    }

    public FileClientAcl(final Path file, final Duration pollInterval) {
        this(file, pollInterval, ReloadObserver.NONE);
    }

    public FileClientAcl(final Path file, final Duration pollInterval, final ReloadObserver observer) {
        final Path abs = file.toAbsolutePath();
        this.source = new WatchedFileSource<>(
                List.of(abs), pollInterval, contents -> parse(abs, contents.get(0)), observer);
    }

    @Override
    public ClientAclSnapshot snapshot() {
        return source.snapshot();
    }

    @Override
    public void addListener(final Consumer<ClientAclSnapshot> listener) {
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

    private static ClientAclSnapshot parse(final Path file, final byte[] bytes) {
        final Properties props = new Properties();
        try {
            props.load(new ByteArrayInputStream(bytes));
        } catch (final Exception e) {
            throw new RuntimeException("Cannot read client ACL file " + file, e);
        }
        final Map<ClientId, ClientPolicy> policies = new LinkedHashMap<>();
        for (final String name : props.stringPropertyNames()) {
            if (!name.startsWith("acl.")) {
                continue;
            }
            final ClientId clientId = ClientId.of(name.substring("acl.".length()));
            final List<ClientPolicy.Grant> grants = new ArrayList<>();
            for (final String grant : props.getProperty(name).split(";")) {
                final String trimmed = grant.trim();
                if (!trimmed.isEmpty()) {
                    grants.add(parseGrant(trimmed, name));
                }
            }
            policies.put(clientId, new ClientPolicy(grants));
        }
        return new ClientAclSnapshot(policies);
    }

    private static ClientPolicy.Grant parseGrant(final String grant, final String name) {
        final int colon = grant.lastIndexOf(':');
        if (colon < 0 || colon == grant.length() - 1) {
            throw new IllegalArgumentException("Bad grant '" + grant + "' for " + name);
        }
        final String prefix = grant.substring(0, colon).trim();
        final String opCodes = grant.substring(colon + 1).trim();
        final EnumSet<ClientOp> ops = EnumSet.noneOf(ClientOp.class);
        for (int i = 0; i < opCodes.length(); i++) {
            ops.add(ClientOp.fromCode(opCodes.charAt(i)));
        }
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("No ops in grant '" + grant + "' for " + name);
        }
        return new ClientPolicy.Grant(prefix, ops);
    }
}
