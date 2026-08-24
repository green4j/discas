/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.runbook;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything the new cluster is to be, as far as {@code init} needs to know it: who the members
 * are, where their files will live on their hosts, and how clients will be let in.
 *
 * <p>Two kinds of field, and the difference is worth keeping in mind. The membership and the dump
 * decide <b>what is written into the folder</b>. Everything else -- host paths, the client port,
 * the authentication mode -- decides nothing about the bytes; it is only ever repeated back in
 * {@code RUN.md}, because a runbook that says {@code --client-bind <host>:<port>} is a runbook
 * somebody has to finish, and finishing it wrong is a cluster that does not come up.
 */
public final class ClusterPlan {

    /** How clients prove who they are, in the node's own spelling. */
    public enum ClientAuth {
        ALLOWALL("allowall", "the claimed client id is taken at face value"),
        TOKEN("token", "a PBKDF2-hashed shared token per client id"),
        MTLS("mtls", "a client certificate, whose CN is the authoritative id");

        private final String cliName;
        private final String summary;

        ClientAuth(final String cliName, final String summary) {
            this.cliName = cliName;
            this.summary = summary;
        }

        /** The value of {@code --client-auth}. */
        public String cliName() {
            return cliName;
        }

        public String summary() {
            return summary;
        }

        public static ClientAuth of(final String cliName) {
            for (final ClientAuth mode : values()) {
                if (mode.cliName.equals(cliName)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown client-auth '" + cliName
                    + "', expected one of allowall, token, mtls");
        }
    }

    private final ClusterId clusterId;
    private final Map<NodeId, InetSocketAddress> members;
    private final Path dump;
    private final int clientPort;
    private final String dataDirectory;
    private final String configDirectory;
    private final ClientAuth clientAuth;
    private final String tokenFile;
    private final String aclFile;
    private final boolean peerTls;
    private final boolean clientTls;

    private ClusterPlan(final Builder builder) {
        this.clusterId = builder.clusterId;
        this.members = Collections.unmodifiableMap(new LinkedHashMap<>(builder.members));
        this.dump = builder.dump;
        this.clientPort = builder.clientPort;
        this.dataDirectory = builder.dataDirectory;
        this.configDirectory = builder.configDirectory;
        this.clientAuth = builder.clientAuth;
        this.tokenFile = builder.tokenFile;
        this.aclFile = builder.aclFile;
        this.peerTls = builder.peerTls;
        // mTLS on the client port is not a separate decision: a client certificate cannot be
        // presented over a plaintext connection. Said once, here, rather than in every place that
        // renders a flag.
        this.clientTls = builder.clientTls || builder.clientAuth == ClientAuth.MTLS;
    }

    public ClusterId clusterId() {
        return clusterId;
    }

    /** The membership: where members talk to <em>each other</em>. */
    public Map<NodeId, InetSocketAddress> members() {
        return members;
    }

    /** The dump every member is seeded from, or {@code null} for an empty cluster. */
    public Path dump() {
        return dump;
    }

    /** The port every member will listen on for clients. */
    public int clientPort() {
        return clientPort;
    }

    /** Where member data directories will live on their hosts; each member's is {@code /<id>}. */
    public String dataDirectory() {
        return dataDirectory;
    }

    /** Where members.conf and any token/ACL files will live on their hosts. */
    public String configDirectory() {
        return configDirectory;
    }

    public ClientAuth clientAuth() {
        return clientAuth;
    }

    /** Token file path on the hosts; only meaningful under {@link ClientAuth#TOKEN}. */
    public String tokenFile() {
        return tokenFile;
    }

    /** ACL file path on the hosts, or {@code null} for no authorization file at all. */
    public String aclFile() {
        return aclFile;
    }

    /** TLS between members. */
    public boolean peerTls() {
        return peerTls;
    }

    /** TLS on the client port; always true under {@link ClientAuth#MTLS}. */
    public boolean clientTls() {
        return clientTls;
    }

    /** This member's data directory on its host. */
    public String dataDirectoryOf(final NodeId nodeId) {
        return dataDirectory + "/" + nodeId.value();
    }

    /** A file under the config directory on the hosts. */
    public String configFile(final String name) {
        return configDirectory + "/" + name;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder; the defaults are the ones the dialogue offers. */
    public static final class Builder {
        private ClusterId clusterId;
        private Map<NodeId, InetSocketAddress> members = Collections.emptyMap();
        private Path dump;
        private int clientPort = 7002;
        private String dataDirectory = "/var/lib/discas";
        private String configDirectory = "/etc/discas";
        private ClientAuth clientAuth = ClientAuth.ALLOWALL;
        private String tokenFile;
        private String aclFile;
        private boolean peerTls;
        private boolean clientTls;

        public Builder clusterId(final ClusterId value) {
            this.clusterId = value;
            return this;
        }

        public Builder members(final Map<NodeId, InetSocketAddress> value) {
            this.members = value;
            return this;
        }

        public Builder dump(final Path value) {
            this.dump = value;
            return this;
        }

        public Builder clientPort(final int value) {
            if (value < 1 || value > 65535) {
                throw new IllegalArgumentException("Client port out of range: " + value);
            }
            this.clientPort = value;
            return this;
        }

        public Builder dataDirectory(final String value) {
            this.dataDirectory = trimTrailingSlash(value);
            return this;
        }

        public Builder configDirectory(final String value) {
            this.configDirectory = trimTrailingSlash(value);
            return this;
        }

        public Builder clientAuth(final ClientAuth value) {
            this.clientAuth = value;
            return this;
        }

        public Builder tokenFile(final String value) {
            this.tokenFile = value;
            return this;
        }

        public Builder aclFile(final String value) {
            this.aclFile = value;
            return this;
        }

        public Builder peerTls(final boolean value) {
            this.peerTls = value;
            return this;
        }

        public Builder clientTls(final boolean value) {
            this.clientTls = value;
            return this;
        }

        // Read-backs. A builder filled question by question is a form, and a form has to be able to
        // show what is on it: the next question's default depends on the last answer.

        public ClientAuth clientAuthValue() {
            return clientAuth;
        }

        public String tokenFileValue() {
            return tokenFile;
        }

        public String aclFileValue() {
            return aclFile;
        }

        public boolean peerTlsValue() {
            return peerTls;
        }

        public boolean clientTlsValue() {
            return clientTls;
        }

        /** A file under the configuration directory as it will be on the hosts. */
        public String configFile(final String name) {
            return configDirectory + "/" + name;
        }

        public ClusterPlan build() {
            if (clusterId == null) {
                throw new IllegalArgumentException("clusterId is required");
            }
            if (members.isEmpty()) {
                throw new IllegalArgumentException("A cluster needs members");
            }
            return new ClusterPlan(this);
        }

        private static String trimTrailingSlash(final String path) {
            String trimmed = path.trim();
            while (trimmed.length() > 1 && trimmed.endsWith("/")) {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }
            return trimmed;
        }
    }
}
