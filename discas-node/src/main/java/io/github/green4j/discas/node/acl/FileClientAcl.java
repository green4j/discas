/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.acl;

import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.io.ReloadableFileSource;
import io.github.green4j.discas.common.io.ReloadReport;

import java.io.ByteArrayInputStream;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * A {@link ClientAcl} backed by a config file, re-read on request. Format (java
 * properties, one line per client; grants separated by {@code ;}, each grant is
 * {@code <keyPrefix>:<ops>} where ops are the {@link ClientOp} letters, e.g. {@code GPCDS}):
 * <pre>
 *   acl.web-1 = app/:GPCD ; session/:GPCDS
 *   acl.reporter = report/:GS
 * </pre>
 * The prefix may itself contain {@code :} -- the split is on the <b>last</b> colon.
 *
 * <p>All the reading, change-gating, replay-on-subscribe and fail-fast-on-initial-load lives in the
 * shared {@link ReloadableFileSource}; this class supplies only the parser.
 *
 * <p>A file with properties in it but no {@code acl.*} entry is refused rather than read as a
 * policy granting nobody anything -- see {@link #parse}. A file with no properties at all is that
 * policy, and is applied.
 */
public final class FileClientAcl implements ClientAcl, AutoCloseable {


    private final ReloadableFileSource<ClientAclSnapshot> source;

    public FileClientAcl(final Path file) {
        this(file, ReloadObserver.NONE);
    }

    public FileClientAcl(final Path file, final ReloadObserver observer) {
        final Path abs = file.toAbsolutePath();
        this.source = new ReloadableFileSource<>(
                List.of(abs), contents -> parse(abs, contents.get(0)),
                ClientAclSnapshot::summary, observer);
    }

    @Override
    public ClientAclSnapshot snapshot() {
        return source.snapshot();
    }

    @Override
    public void addListener(final Consumer<ClientAclSnapshot> listener) {
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
        if (policies.isEmpty() && !props.isEmpty()) {
            // A file with properties in it but not one acl.* among them is a mis-typed prefix, not a
            // policy. It would parse as "no client is granted anything", so accepting it would take
            // the cluster off the air for every client at once, quietly and on a successful reload.
            // An empty file is left alone: that one really does say nobody may do anything.
            throw new IllegalArgumentException("No acl.* entries in client ACL file " + file
                    + ", though it has " + props.size() + " other propert"
                    + (props.size() == 1 ? "y" : "ies"));
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
