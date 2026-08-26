/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.io.ReloadableFileSource;
import io.github.green4j.discas.common.io.ReloadReport;

import java.io.ByteArrayInputStream;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * A {@link Members} list backed by a config file, re-read on request. Format (java
 * properties, one line per member; no {@code cluster_id} -- that is supplied to the node
 * at startup):
 * <pre>
 *   node.1 = node-1.discas.svc.cluster.local:9001
 *   node.2 = node-2.discas.svc.cluster.local:9001
 * </pre>
 *
 * <p>All the reading, change-gating, replay-on-subscribe and fail-fast-on-initial-load lives in
 * the shared {@link ReloadableFileSource}; this class supplies only the members parser. A malformed
 * reload is refused and reported, keeping the last good snapshot.
 */
public final class FileMembers implements Members<TcpMemberInfo>, AutoCloseable {


    private final ReloadableFileSource<MembersSnapshot<TcpMemberInfo>> source;

    public FileMembers(final Path file) {
        this(file, ReloadObserver.NONE);
    }

    public FileMembers(final Path file, final ReloadObserver observer) {
        final Path abs = file.toAbsolutePath();
        this.source = new ReloadableFileSource<>(
                List.of(abs), contents -> parse(abs, contents.get(0)),
                MembersSnapshot::summary, observer);
    }

    @Override
    public MembersSnapshot<TcpMemberInfo> snapshot() {
        return source.snapshot();
    }

    @Override
    public void addListener(final Consumer<MembersSnapshot<TcpMemberInfo>> listener) {
        source.addListener(listener);
    }

    /** Re-read this file alone and apply what it says. */
    public ReloadReport.Entry reloadNow() {
        return source.reloadNow();
    }

    @Override
    public void close() {
        source.close();
    }

    private static MembersSnapshot<TcpMemberInfo> parse(final Path file, final byte[] bytes) {
        final Properties props = new Properties();
        try {
            props.load(new ByteArrayInputStream(bytes));
        } catch (final Exception e) {
            throw new RuntimeException("Cannot read members file " + file, e);
        }
        final Map<NodeId, TcpMemberInfo> members = new LinkedHashMap<>();
        for (final String name : props.stringPropertyNames()) {
            if (!name.startsWith("node.")) {
                continue;
            }
            final NodeId nodeId = NodeId.of(name.substring("node.".length()));
            final String hostPort = props.getProperty(name).trim();
            final int colon = hostPort.lastIndexOf(':');
            if (colon <= 0 || colon == hostPort.length() - 1) {
                throw new IllegalArgumentException("Bad address '" + hostPort + "' for " + name);
            }
            // Both halves trimmed: "host : 9001" is the same member as "host:9001", and a
            // difference the file cannot mean must not read as a change on reload.
            final String host = hostPort.substring(0, colon).trim();
            final int port = Integer.parseInt(hostPort.substring(colon + 1).trim());
            members.put(nodeId, new TcpMemberInfo(nodeId, host, port));
        }
        if (members.isEmpty()) {
            throw new IllegalArgumentException("No node.* members in " + file);
        }
        return new MembersSnapshot<>(members);
    }
}
