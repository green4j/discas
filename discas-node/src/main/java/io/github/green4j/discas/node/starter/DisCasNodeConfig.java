/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.starter;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigResolver;
import io.github.green4j.discas.common.cli.config.ConfigSource;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.common.observability.ObservabilityConfig;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.StorageConfig;

import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * The effective, resolved configuration for a single {@link DisCasNode} started by
 * {@link DisCasNodeStarter}, assembled from a <b>dual</b> source: command-line flags
 * (parsed with {@link GetOpts}) and {@code DISCAS_*} environment variables. Each property
 * is settable by <em>either</em> source; the per-property precedence is
 * {@code CLI > ENV > DEFAULT}, decided independently for every value by the shared
 * {@link ConfigResolver}.
 * <p>
 * The {@code DISCAS_*} name of a property is derived mechanically from its long option name
 * ({@code UPPER_SNAKE}, {@code '-'} to {@code '_'}), so {@code --node-id} pairs with
 * {@code DISCAS_NODE_ID}, {@code --wal-dir} with {@code DISCAS_WAL_DIR}, and so on.
 * <p>
 * {@link #resolve(String[], Map)} takes the environment as a parameter (rather than reading
 * {@link System#getenv()} directly) so it is fully unit-testable; the starter passes the real
 * environment. {@link #describe()} renders every effective value with the source it came
 * from, masking security-sensitive values.
 */
public final class DisCasNodeConfig {

    static final String PROGRAM = "discas-node";

    /**
     * Every CLI default is the corresponding component's own default, read from the component
     * rather than restated here -- so an option's default, its help text and the value a
     * programmatic caller gets cannot drift apart. These are the three components the node
     * assembles.
     */
    // CLI-only defaults: switches with no component behind them to read the value from.
    // Declared once and used by both the resolve call and the help text, so the two cannot
    // disagree about what the default is.
    private static final boolean DEFAULT_TLS = false;
    private static final boolean DEFAULT_TLS_CERT_ROTATION = true;
    private static final ClientAuthMode DEFAULT_CLIENT_AUTH = ClientAuthMode.ALLOWALL;
    private static final boolean DEFAULT_CLIENT_TLS = false;
    private static final boolean DEFAULT_CLIENT_TLS_CERT_ROTATION = true;

    private static final StorageConfig STORAGE_DEFAULTS = StorageConfig.builder()
            .baseDirectory(Path.of(".")).build();
    private static final TcpTransportConfig PEER_DEFAULTS = TcpTransportConfig.defaults();
    private static final ClientTransportConfig CLIENT_DEFAULTS = ClientTransportConfig.defaults();
    // A bare builder is a valid source of every node-timing default without inventing an
    // identity, which is exactly what help text and defaulted parsing need.
    private static final NodeConfig.Builder NODE_DEFAULTS = NodeConfig.builder();
    private static final ObservabilityConfig OBSERVABILITY_DEFAULTS = ObservabilityConfig.builder().build();

    /** This node's id ({@code --node-id}, {@code DISCAS_NODE_ID}). Must appear in the member list. */
    public final NodeId nodeId;
    /** The cluster this node joins ({@code --cluster-id}, {@code DISCAS_CLUSTER_ID}). */
    public final ClusterId clusterId;
    /**
     * N, the quorum basis ({@code --cluster-size}, {@code DISCAS_CLUSTER_SIZE}). Defaults to the
     * number of members; every node must agree on it or the peer handshake refuses the connection.
     */
    public final int clusterSize;

    /**
     * Path to the members file ({@code --members-file}, {@code DISCAS_MEMBERS_FILE}), or
     * {@code null} when the list came from {@code --members}. File mode is re-read on demand at
     * runtime; the inline list is not. The two are mutually exclusive.
     */
    public final Path membersFile;
    /** The parsed member list in either mode, used to validate and to derive {@link #peerBind}. */
    public final Map<NodeId, InetSocketAddress> members;

    /**
     * Where the peer mesh listens ({@code --peer-bind}, {@code DISCAS_PEER_BIND}). Defaults to
     * this node's own entry in the member list. Port {@code 0} binds an ephemeral port.
     */
    final InetSocketAddress peerBind;
    /**
     * Where clients connect ({@code --client-bind}, {@code DISCAS_CLIENT_BIND}). Always a separate
     * port from {@link #peerBind}, with its own TLS and authentication settings.
     */
    final InetSocketAddress clientBind;

    /**
     * WAL and snapshot settings ({@code --wal-dir}, {@code --wal-max-file-bytes},
     * {@code --snapshot-retention} and their {@code DISCAS_*} equivalents).
     */
    public final StorageConfig storageConfig;

    /**
     * The node's identity, quorum basis and every timing it runs on, assembled from the identity
     * options above and the {@code --round-*}, {@code --anti-entropy-*}, {@code --repair-*},
     * {@code --snapshot-interval-*}, {@code --promise-eviction-*}, {@code --wal-force-*} and
     * {@code --shutdown-await-*} options (each with a {@code DISCAS_*} equivalent).
     * <p>
     * Every one of these was a compile-time constant until a deployment needed to fit a node to a
     * high-latency link; {@link NodeConfig} carries the defaults and the reasoning.
     */
    public final NodeConfig nodeConfig;

    // Transport tuning, composed as the configs the transports actually take -- the same shape
    // DisCasAgentConfig uses, rather than loose ints for the starter to reassemble.

    /**
     * Where the node serves {@code /health}, {@code /ready} and {@code /metrics}
     * ({@code --observability-bind}, {@code --observability-workers},
     * {@code --observability-enabled}). Loopback by default, like the agent.
     */
    public final ObservabilityConfig observabilityConfig;

    /** Peer-mesh transport budgets, from the {@code --peer-max-*} and {@code --peer-force-reconnect} options. */
    public final TcpTransportConfig peerTransportConfig;
    /** Client-port transport budgets, from the {@code --client-max-*} options. */
    public final ClientTransportConfig clientTransportConfig;

    /** Whether the peer mesh runs mTLS ({@code --tls}, {@code DISCAS_TLS}); default {@code false}. */
    public final boolean tls;
    /** This node's peer leaf certificate and key ({@code --tls-keystore}). Required when {@link #tls}. */
    public final Path tlsKeystore;
    /** Password for {@link #tlsKeystore} ({@code --tls-keystore-password}); masked by {@link #describe()}. */
    public final char[] tlsKeystorePassword;
    /** The peer CA this node trusts ({@code --tls-truststore}). Required when {@link #tls}. */
    public final Path tlsTruststore;
    /** Password for {@link #tlsTruststore} ({@code --tls-truststore-password}); masked by {@link #describe()}. */
    public final char[] tlsTruststorePassword;
    /**
     * Whether a reload may swap in renewed peer key material without a restart
     * ({@code --tls-cert-rotation}); default {@code true}.
     */
    public final boolean tlsCertRotation;

    // Client access: authentication, authorization and TLS on the client port. Entirely
    // independent of the peer-mesh TLS above -- a cluster may run an mTLS peer mesh with a
    // plaintext client port, or the reverse.

    /**
     * How clients authenticate ({@code --client-auth}, {@code DISCAS_CLIENT_AUTH}): {@code allowall}
     * (default), {@code token} or {@code mtls}. {@code allowall} accepts every client id as claimed.
     */
    public final ClientAuthMode clientAuth;
    /** Token mode: a properties file of {@code client.<id>=<spec>} ({@code --client-token-file}). */
    final Path clientTokenFile;
    /** Token mode: a directory of {@code <clientId>.token} files ({@code --client-token-dir}). */
    final Path clientTokenDir;
    /**
     * Authorization policy ({@code --client-acl-file}): {@code acl.<id>=<prefix>:<OPS> ; ...}.
     * When unset the node is permissive -- every authenticated client may do anything.
     */
    final Path clientAclFile;
    /** Whether the client port runs TLS ({@code --client-tls}); default {@code false}. */
    public final boolean clientTls;
    /** The node's client-facing server certificate and key ({@code --client-tls-keystore}). */
    final Path clientTlsKeystore;
    /** Password for {@link #clientTlsKeystore}; masked by {@link #describe()}. */
    final char[] clientTlsKeystorePassword;
    /** The client CA this node trusts ({@code --client-tls-truststore}); required for {@code mtls}. */
    final Path clientTlsTruststore;
    /** Password for {@link #clientTlsTruststore}; masked by {@link #describe()}. */
    final char[] clientTlsTruststorePassword;
    /**
     * Whether a reload may swap in renewed client key material without a restart
     * ({@code --client-tls-cert-rotation}); default {@code true}.
     */
    final boolean clientTlsCertRotation;

    private final ConfigResolver resolver;

    private DisCasNodeConfig(final String[] args, final Map<String, String> env) {
        final GetOpts opts = newOpts();
        opts.parseOrThrow(args);

        final ConfigResolver r = new ConfigResolver(PROGRAM, opts, env);
        this.resolver = r;

        this.nodeId = NodeId.of(r.required("node-id"));
        this.clusterId = ClusterId.of(r.required("cluster-id"));

        // Membership: exactly one of --members-file / --members.
        final String membersFileStr = r.optional("members-file");
        final String membersStr = r.optional("members");
        if ((membersFileStr == null) == (membersStr == null)) {
            throw new IllegalArgumentException(
                    "Exactly one of --members-file (" + ConfigSupport.envName("members-file")
                            + ") or --members (" + ConfigSupport.envName("members") + ") must be set");
        }
        if (membersFileStr != null) {
            this.membersFile = Path.of(membersFileStr);
            this.members = ConfigSupport.parseMembersFile(this.membersFile, "members");
        } else {
            this.membersFile = null;
            this.members = ConfigSupport.parseMembers(membersStr, "members");
        }
        if (!members.containsKey(nodeId)) {
            throw new IllegalArgumentException(
                    "node-id '" + nodeId + "' does not appear in the members list " + members.keySet());
        }

        // Cluster size: defaults to the number of members.
        this.clusterSize = r.integerInRange("cluster-size", members.size(), 1, 255);

        // Peer bind: defaults to this node's own members entry.
        final InetSocketAddress own = members.get(nodeId);
        final String peerBindStr = r.optional("peer-bind", hostPort(own));
        this.peerBind = ConfigSupport.parseAddress(peerBindStr, "peer-bind");

        this.clientBind = ConfigSupport.parseAddress(r.required("client-bind"), "client-bind");

        this.storageConfig = StorageConfig.builder()
                .baseDirectory(Path.of(r.required("wal-dir")))
                .walMaxFileBytes(r.integerAtLeast(
                        "wal-max-file-bytes", STORAGE_DEFAULTS.walMaxFileBytes(), 1))
                .snapshotRetentionCount(r.integerAtLeast(
                        "snapshot-retention", STORAGE_DEFAULTS.snapshotRetentionCount(), 1))
                .build();

        this.nodeConfig = NodeConfig.builder()
                .nodeId(nodeId)
                .clusterId(clusterId)
                .clusterSize(clusterSize)
                .roundTimeout(Duration.ofMillis(r.longAtLeast(
                        "round-timeout-ms", NODE_DEFAULTS.roundTimeout().toMillis(), 1)))
                .maxRoundRetries((int) r.longAtLeast(
                        "round-max-retries", NODE_DEFAULTS.maxRoundRetries(), 0))
                .proposalExpiry(Duration.ofMillis(r.longAtLeast(
                        "proposal-expiry-ms", NODE_DEFAULTS.proposalExpiry().toMillis(), 1)))
                .roundRetryBackoffBase(Duration.ofMillis(r.longAtLeast(
                        "round-retry-backoff-ms", NODE_DEFAULTS.roundRetryBackoffBase().toMillis(), 1)))
                .roundRetryBackoffJitter(Duration.ofMillis(r.longAtLeast(
                        "round-retry-jitter-ms", NODE_DEFAULTS.roundRetryBackoffJitter().toMillis(), 1)))
                .noQuorumBackoff(Duration.ofMillis(r.longAtLeast(
                        "no-quorum-backoff-ms", NODE_DEFAULTS.noQuorumBackoff().toMillis(), 1)))
                .peerResponseTimeout(Duration.ofMillis(r.longAtLeast(
                        "peer-response-timeout-ms",
                        NODE_DEFAULTS.peerResponseTimeout().toMillis(), 1)))
                .repairInterval(Duration.ofSeconds(r.longAtLeast(
                        "repair-interval-seconds", NODE_DEFAULTS.repairInterval().getSeconds(), 1)))
                .tombstoneSweepInterval(Duration.ofSeconds(r.longAtLeast(
                        "tombstone-sweep-interval-seconds",
                        NODE_DEFAULTS.tombstoneSweepInterval().getSeconds(), 1)))
                .shutdownAwaitTimeout(Duration.ofMillis(r.longAtLeast(
                        "shutdown-await-timeout-ms", NODE_DEFAULTS.shutdownAwaitTimeout().toMillis(), 1)))
                .snapshotInterval(Duration.ofSeconds(r.longAtLeast(
                        "snapshot-interval-seconds", NODE_DEFAULTS.snapshotInterval().getSeconds(), 1)))
                .promiseEvictionInterval(Duration.ofSeconds(r.longAtLeast(
                        "promise-eviction-interval-seconds",
                        NODE_DEFAULTS.promiseEvictionInterval().getSeconds(), 1)))
                .walForceInterval(Duration.ofMillis(r.longAtLeast(
                        "wal-force-interval-ms", NODE_DEFAULTS.walForceInterval().toMillis(), 1)))
                .storeHeapFraction(Double.parseDouble(r.optional("store-heap-fraction",
                        Double.toString(NODE_DEFAULTS.storeHeapFraction()))))
                .build();

        final InetSocketAddress observabilityBind = ConfigSupport.parseAddress(
                r.optional("observability-bind",
                        OBSERVABILITY_DEFAULTS.bindAddress() + ":" + OBSERVABILITY_DEFAULTS.port()),
                "observability-bind", true);
        this.observabilityConfig = ObservabilityConfig.builder()
                .enabled(r.bool("observability-enabled", OBSERVABILITY_DEFAULTS.enabled()))
                .bindAddress(observabilityBind.getHostString())
                .port(observabilityBind.getPort())
                .workerCount(r.integerAtLeast(
                        "observability-workers", OBSERVABILITY_DEFAULTS.workerCount(), 1))
                .build();

        this.peerTransportConfig = TcpTransportConfig.builder()
                .maxFrameBytes(r.integerAtLeast(
                        "peer-max-frame-bytes", PEER_DEFAULTS.maxFrameBytes(), 1))
                .maxQueuedOutBytes(r.integerAtLeast(
                        "peer-max-queued-out-bytes", PEER_DEFAULTS.maxQueuedOutBytes(), 1))
                .maxRxBufferBytes(r.integerAtLeast(
                        "peer-max-rx-buffer-bytes", PEER_DEFAULTS.maxRxBufferBytes(), 1))
                .maxInflightBytes(r.integerAtLeast(
                        "peer-max-inflight-bytes", PEER_DEFAULTS.maxInflightBytes(), 1))
                .maxConnections(r.integerAtLeast(
                        "peer-max-connections", PEER_DEFAULTS.maxConnections(), 1))
                .forceReconnect(r.bool("peer-force-reconnect", PEER_DEFAULTS.forceReconnect()))
                .reconnectBackoffBase(Duration.ofMillis(r.longAtLeast(
                        "peer-reconnect-backoff-ms", PEER_DEFAULTS.reconnectBackoffBase().toMillis(), 1)))
                .reconnectBackoffCap(Duration.ofMillis(r.longAtLeast(
                        "peer-reconnect-backoff-cap-ms",
                        PEER_DEFAULTS.reconnectBackoffCap().toMillis(), 1)))
                .build();

        // The client-facing transport is a knob here for the same reason the peer one is.
        this.clientTransportConfig = ClientTransportConfig.builder()
                .maxFrameBytes(r.integerAtLeast(
                        "client-max-frame-bytes", CLIENT_DEFAULTS.maxFrameBytes(), 1))
                .maxQueuedOutBytes(r.integerAtLeast(
                        "client-max-queued-out-bytes", CLIENT_DEFAULTS.maxQueuedOutBytes(), 1))
                .maxRxBufferBytes(r.integerAtLeast(
                        "client-max-rx-buffer-bytes", CLIENT_DEFAULTS.maxRxBufferBytes(), 1))
                .maxInflightBytes(r.integerAtLeast(
                        "client-max-inflight-bytes", CLIENT_DEFAULTS.maxInflightBytes(), 1))
                .maxConnections(r.integerAtLeast(
                        "client-max-connections", CLIENT_DEFAULTS.maxConnections(), 1))
                .build();

        this.tls = r.bool("tls", DEFAULT_TLS);
        final String keystore = r.optional("tls-keystore");
        final String keystorePw = r.secret("tls-keystore-password");
        final String truststore = r.optional("tls-truststore");
        final String truststorePw = r.secret("tls-truststore-password");
        this.tlsCertRotation = r.bool("tls-cert-rotation", DEFAULT_TLS_CERT_ROTATION);
        if (tls) {
            ConfigSupport.requirePresent(keystore, "tls-keystore");
            ConfigSupport.requirePresent(keystorePw, "tls-keystore-password");
            ConfigSupport.requirePresent(truststore, "tls-truststore");
            ConfigSupport.requirePresent(truststorePw, "tls-truststore-password");
        }
        this.tlsKeystore = keystore == null ? null : Path.of(keystore);
        this.tlsKeystorePassword = keystorePw == null ? null : keystorePw.toCharArray();
        this.tlsTruststore = truststore == null ? null : Path.of(truststore);
        this.tlsTruststorePassword = truststorePw == null ? null : truststorePw.toCharArray();

        this.clientAuth = ClientAuthMode.parse(
                r.optional("client-auth", DEFAULT_CLIENT_AUTH.cliName()));

        final String tokenFile = r.optional("client-token-file");
        final String tokenDir = r.optional("client-token-dir");
        if (clientAuth == ClientAuthMode.TOKEN) {
            if ((tokenFile == null) == (tokenDir == null)) {
                throw new IllegalArgumentException(
                        "client-auth=token requires exactly one of --client-token-file ("
                                + ConfigSupport.envName("client-token-file") + ") or --client-token-dir ("
                                + ConfigSupport.envName("client-token-dir") + ")");
            }
        } else if (tokenFile != null || tokenDir != null) {
            throw new IllegalArgumentException(
                    "--client-token-file/--client-token-dir set but --client-auth ("
                            + ConfigSupport.envName("client-auth") + ") is '" + clientAuth.cliName()
                            + "', not 'token'");
        }
        this.clientTokenFile = tokenFile == null ? null : Path.of(tokenFile);
        this.clientTokenDir = tokenDir == null ? null : Path.of(tokenDir);

        final String aclFile = r.optional("client-acl-file");
        this.clientAclFile = aclFile == null ? null : Path.of(aclFile);

        final boolean clientTlsRequested = r.bool("client-tls", DEFAULT_CLIENT_TLS);
        final String clientKeystore = r.optional("client-tls-keystore");
        final String clientKeystorePw = r.secret("client-tls-keystore-password");
        final String clientTruststore = r.optional("client-tls-truststore");
        final String clientTruststorePw = r.secret("client-tls-truststore-password");
        this.clientTlsCertRotation = r.bool("client-tls-cert-rotation", DEFAULT_CLIENT_TLS_CERT_ROTATION);

        // mTLS client auth is meaningless without TLS on the client port, so it implies it
        // rather than forcing the operator to set two flags that cannot disagree.
        this.clientTls = clientTlsRequested || clientAuth == ClientAuthMode.MTLS;

        if (clientTls) {
            // The node is the TLS server here: it always presents its own cert.
            ConfigSupport.requirePresent(clientKeystore, "client-tls-keystore");
            ConfigSupport.requirePresent(clientKeystorePw, "client-tls-keystore-password");
            if (clientAuth == ClientAuthMode.MTLS) {
                // Only mTLS needs to verify the other side, so only mTLS needs a trust store.
                ConfigSupport.requirePresent(clientTruststore, "client-tls-truststore");
                ConfigSupport.requirePresent(clientTruststorePw, "client-tls-truststore-password");
            }
        } else if (clientKeystore != null || clientTruststore != null) {
            throw new IllegalArgumentException(
                    "--client-tls-keystore/--client-tls-truststore set but --client-tls ("
                            + ConfigSupport.envName("client-tls") + ") is off");
        }
        this.clientTlsKeystore = clientKeystore == null ? null : Path.of(clientKeystore);
        this.clientTlsKeystorePassword =
                clientKeystorePw == null ? null : clientKeystorePw.toCharArray();
        this.clientTlsTruststore = clientTruststore == null ? null : Path.of(clientTruststore);
        this.clientTlsTruststorePassword =
                clientTruststorePw == null ? null : clientTruststorePw.toCharArray();
    }

    /**
     * True when no client ACL is configured, meaning {@code ClientAuthorizer} runs permissive and
     * every authenticated client may touch every key. The starter warns about this at boot.
     */
    boolean clientAuthorizationDisabled() {
        return clientAclFile == null;
    }

    /**
     * Resolve the effective configuration from CLI {@code args} and an environment map.
     * Throws {@link IllegalArgumentException} on a missing required value or an invalid
     * value, and {@link GetOpts.ParseException} on a malformed command line. Never calls
     * {@link System#exit}.
     */
    public static DisCasNodeConfig resolve(final String[] args, final Map<String, String> env) {
        return new DisCasNodeConfig(args, env == null ? Map.of() : env);
    }

    /**
     * True when {@code args} asks for help ({@code -h} or {@code --help}), so the starter can print
     * it and exit before resolving anything.
     */
    public static boolean isHelpRequested(final String[] args) {
        return ConfigSupport.isHelpRequested(args);
    }

    /** The full option reference, one line per option with its default and its {@code DISCAS_*} name. */
    public static String helpText() {
        return newOpts().helpString();
    }

    /** The one-line usage summary. */
    public static String usageText() {
        return newOpts().usageString();
    }

    boolean membersFromFile() {
        return membersFile != null;
    }

    /**
     * A {@code PROPERTY | VALUE | SOURCE} table of every effective value and where it came
     * from. Security-sensitive values are masked.
     */
    public String describe() {
        return resolver.describe();
    }

    /** Writes {@link #describe()} to {@code out}; what the starter logs at boot. */
    public void print(final PrintStream out) {
        resolver.print(out);
    }

    /** The resolved source for a property's long name; {@code null} if it was not resolved. */
    ConfigSource sourceOf(final String longName) {
        return resolver.sourceOf(longName);
    }

    /** The display (masked for secrets) value recorded for a property; {@code null} if absent. */
    String displayValueOf(final String longName) {
        return resolver.displayValueOf(longName);
    }

    private static String hostPort(final InetSocketAddress address) {
        return address.getHostString() + ":" + address.getPort();
    }

    private static GetOpts newOpts() {
        return new GetOpts(PROGRAM,
                "Start a single DisCas cluster node (TCP peers + FileWal + TCP client server). "
                        + "Every option can also be set by its DISCAS_* environment variable; a flag "
                        + "overrides the environment variable, which overrides the default.")
                .group("Identity")
                .stringOpt("node-id", 'n', ConfigSupport.helpWithEnv("node-id", "This node's id [required]."))
                .stringOpt("cluster-id", 'c', ConfigSupport.helpWithEnv("cluster-id",
                        "Cluster id shared by all peers [required]."))
                .stringOpt("cluster-size", null, ConfigSupport.helpWithEnv("cluster-size",
                        "Cluster size / quorum basis [default: number of members].")).metavar("<n>")
                .group("Membership")
                .stringOpt("members-file", null, ConfigSupport.helpWithEnv("members-file",
                        "Path to a members file, node.<id>=host:port, re-read on POST /reload. "
                                + "Mutually exclusive with --members.")).metavar("<path>")
                .stringOpt("members", null, ConfigSupport.helpWithEnv("members",
                        "Static members list id=host:port,id2=host:port. "
                                + "Mutually exclusive with --members-file.")).metavar("<list>")
                .group("Transport")
                .stringOpt("peer-bind", null, ConfigSupport.helpWithEnv("peer-bind",
                        "host:port this node binds for peers [default: this node's members entry]."))
                .metavar("<host:port>")
                .stringOpt("client-bind", null, ConfigSupport.helpWithEnv("client-bind",
                        "host:port this node binds for clients [required].")).metavar("<host:port>")
                .group("Storage")
                .stringOpt("wal-dir", null, ConfigSupport.helpWithEnv("wal-dir",
                        "FileWal base directory [required].")).metavar("<path>")
                .stringOpt("wal-max-file-bytes", null, ConfigSupport.helpWithEnv("wal-max-file-bytes",
                        "Max WAL segment size [default: "
                                + STORAGE_DEFAULTS.walMaxFileBytes() + "].")).metavar("<bytes>")
                .stringOpt("snapshot-retention", null, ConfigSupport.helpWithEnv("snapshot-retention",
                        "Snapshots to retain [default: "
                                + STORAGE_DEFAULTS.snapshotRetentionCount() + "].")).metavar("<n>")
                .group("Runtime")
                .stringOpt("repair-interval-seconds", null, ConfigSupport.helpWithEnv("repair-interval-seconds",
                        "Anti-entropy repair interval seconds [default: "
                                + NODE_DEFAULTS.repairInterval().getSeconds() + "].")).metavar("<seconds>")
                .stringOpt("round-timeout-ms", null, ConfigSupport.helpWithEnv("round-timeout-ms",
                        "Consensus round timeout ms; raise for high-latency links [default: "
                                + NODE_DEFAULTS.roundTimeout().toMillis() + "].")).metavar("<ms>")
                .stringOpt("round-max-retries", null, ConfigSupport.helpWithEnv("round-max-retries",
                        "Retries of a failed round before failing the caller [default: "
                                + NODE_DEFAULTS.maxRoundRetries() + "].")).metavar("<n>")
                .stringOpt("proposal-expiry-ms", null, ConfigSupport.helpWithEnv("proposal-expiry-ms",
                        "Total ms one write may be driven, retries included, before the coordinator "
                                + "abandons it; bounds how long an indeterminate write may still "
                                + "apply [default: "
                                + NODE_DEFAULTS.proposalExpiry().toMillis() + "].")).metavar("<ms>")
                .stringOpt("round-retry-backoff-ms", null, ConfigSupport.helpWithEnv("round-retry-backoff-ms",
                        "Base of the exponential round-retry backoff ms [default: "
                                + NODE_DEFAULTS.roundRetryBackoffBase().toMillis() + "].")).metavar("<ms>")
                .stringOpt("round-retry-jitter-ms", null, ConfigSupport.helpWithEnv("round-retry-jitter-ms",
                        "Random jitter added to each round-retry backoff ms [default: "
                                + NODE_DEFAULTS.roundRetryBackoffJitter().toMillis() + "].")).metavar("<ms>")
                .stringOpt("no-quorum-backoff-ms", null, ConfigSupport.helpWithEnv("no-quorum-backoff-ms",
                        "How long a 'cannot see a majority' verdict makes this node refuse "
                                + "linearizable work before retrying optimistically [default: "
                                + NODE_DEFAULTS.noQuorumBackoff().toMillis() + "].")).metavar("<ms>")
                .stringOpt("peer-response-timeout-ms", null,
                        ConfigSupport.helpWithEnv("peer-response-timeout-ms",
                        "How long a background exchange -- an anti-entropy page or a tombstone "
                                + "sweep's purge checks -- waits for the peers it asked before "
                                + "acting on whoever replied; not a failure detector [default: "
                                + NODE_DEFAULTS.peerResponseTimeout().toMillis() + "].")).metavar("<ms>")
                .stringOpt("tombstone-sweep-interval-seconds", null,
                        ConfigSupport.helpWithEnv("tombstone-sweep-interval-seconds",
                        "How long after one tombstone collection is decided the next is swept; one "
                                + "key per sweep, so this is the collection rate [default: "
                                + NODE_DEFAULTS.tombstoneSweepInterval().getSeconds() + "].")).metavar("<seconds>")
                .stringOpt("snapshot-interval-seconds", null,
                        ConfigSupport.helpWithEnv("snapshot-interval-seconds",
                        "How often the node snapshots its state [default: "
                                + NODE_DEFAULTS.snapshotInterval().getSeconds() + "].")).metavar("<seconds>")
                .stringOpt("promise-eviction-interval-seconds", null,
                        ConfigSupport.helpWithEnv("promise-eviction-interval-seconds",
                        "How often expired promises are swept [default: "
                                + NODE_DEFAULTS.promiseEvictionInterval().getSeconds() + "].")).metavar("<seconds>")
                .stringOpt("wal-force-interval-ms", null, ConfigSupport.helpWithEnv("wal-force-interval-ms",
                        "How often buffered WAL writes are forced ms [default: "
                                + NODE_DEFAULTS.walForceInterval().toMillis() + "].")).metavar("<ms>")
                .stringOpt("store-heap-fraction", null,
                        ConfigSupport.helpWithEnv("store-heap-fraction",
                        "Share of max heap the store's estimated footprint may take before this "
                                + "node refuses to grow; deletes are never refused [default: "
                                + NODE_DEFAULTS.storeHeapFraction() + "].")).metavar("<0..1>")
                .stringOpt("shutdown-await-timeout-ms", null,
                        ConfigSupport.helpWithEnv("shutdown-await-timeout-ms",
                        "How long close() waits for the loop to drain ms [default: "
                                + NODE_DEFAULTS.shutdownAwaitTimeout().toMillis() + "].")).metavar("<ms>")
                .group("Observability")
                .stringOpt("observability-enabled", null,
                        ConfigSupport.helpWithEnv("observability-enabled",
                        "Serve /health, /ready and /metrics [default: "
                                + OBSERVABILITY_DEFAULTS.enabled() + "]."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .stringOpt("observability-bind", null,
                        ConfigSupport.helpWithEnv("observability-bind",
                        "host:port for the observability endpoints; loopback by default because "
                                + "they expose peer and topology detail [default: "
                                + OBSERVABILITY_DEFAULTS.bindAddress() + ":"
                                + OBSERVABILITY_DEFAULTS.port() + "]."))
                .metavar("<host:port>")
                .stringOpt("observability-workers", null,
                        ConfigSupport.helpWithEnv("observability-workers",
                        "HTTP worker threads for the observability endpoints [default: "
                                + OBSERVABILITY_DEFAULTS.workerCount() + "].")).metavar("<n>")
                .stringOpt("peer-max-frame-bytes", null, ConfigSupport.helpWithEnv("peer-max-frame-bytes",
                        "Peer transport max frame bytes [default: "
                                + PEER_DEFAULTS.maxFrameBytes() + "].")).metavar("<bytes>")
                .stringOpt("peer-max-queued-out-bytes", null, ConfigSupport.helpWithEnv("peer-max-queued-out-bytes",
                        "Peer transport tx queue bytes [default: "
                                + PEER_DEFAULTS.maxQueuedOutBytes() + "].")).metavar("<bytes>")
                .stringOpt("peer-max-rx-buffer-bytes", null, ConfigSupport.helpWithEnv("peer-max-rx-buffer-bytes",
                        "Peer transport rx buffer bytes [default: "
                                + PEER_DEFAULTS.maxRxBufferBytes() + "].")).metavar("<bytes>")
                .stringOpt("peer-max-inflight-bytes", null, ConfigSupport.helpWithEnv("peer-max-inflight-bytes",
                        "Peer transport inflight bytes [default: "
                                + PEER_DEFAULTS.maxInflightBytes() + "].")).metavar("<bytes>")
                .stringOpt("peer-max-connections", null, ConfigSupport.helpWithEnv("peer-max-connections",
                        "Peer transport max connections [default: "
                                + PEER_DEFAULTS.maxConnections() + "].")).metavar("<n>")
                .stringOpt("peer-force-reconnect", null, ConfigSupport.helpWithEnv("peer-force-reconnect",
                        "Drop live peer connections on member address change [default: "
                                + PEER_DEFAULTS.forceReconnect() + "]."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .stringOpt("peer-reconnect-backoff-ms", null,
                        ConfigSupport.helpWithEnv("peer-reconnect-backoff-ms",
                        "First peer reconnect delay ms, doubled per failure [default: "
                                + PEER_DEFAULTS.reconnectBackoffBase().toMillis() + "].")).metavar("<ms>")
                .stringOpt("peer-reconnect-backoff-cap-ms", null,
                        ConfigSupport.helpWithEnv("peer-reconnect-backoff-cap-ms",
                        "Ceiling the peer reconnect delay saturates at ms [default: "
                                + PEER_DEFAULTS.reconnectBackoffCap().toMillis() + "].")).metavar("<ms>")
                .stringOpt("client-max-frame-bytes", null, ConfigSupport.helpWithEnv("client-max-frame-bytes",
                        "Client transport max frame bytes [default: "
                                + CLIENT_DEFAULTS.maxFrameBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-queued-out-bytes", null,
                        ConfigSupport.helpWithEnv("client-max-queued-out-bytes",
                                "Client transport tx queue bytes [default: "
                                        + CLIENT_DEFAULTS.maxQueuedOutBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-rx-buffer-bytes", null,
                        ConfigSupport.helpWithEnv("client-max-rx-buffer-bytes",
                                "Client transport rx buffer bytes [default: "
                                        + CLIENT_DEFAULTS.maxRxBufferBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-inflight-bytes", null,
                        ConfigSupport.helpWithEnv("client-max-inflight-bytes",
                                "Client transport inflight bytes [default: "
                                        + CLIENT_DEFAULTS.maxInflightBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-connections", null, ConfigSupport.helpWithEnv("client-max-connections",
                        "Client transport max connections [default: "
                                + CLIENT_DEFAULTS.maxConnections() + "].")).metavar("<n>")
                .group("Security (TLS/mTLS)")
                .stringOpt("tls", null, ConfigSupport.helpWithEnv("tls",
                        "Enable mTLS for the peer transport [default: " + DEFAULT_TLS + "]."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .stringOpt("tls-keystore", null, ConfigSupport.helpWithEnv("tls-keystore",
                        "PKCS12 key store path [required with TLS].")).metavar("<path>")
                .stringOpt("tls-keystore-password", null, ConfigSupport.helpWithEnv("tls-keystore-password",
                        "Key store password [required with TLS].")).metavar("<secret>")
                .stringOpt("tls-truststore", null, ConfigSupport.helpWithEnv("tls-truststore",
                        "PKCS12 trust store path [required with TLS].")).metavar("<path>")
                .stringOpt("tls-truststore-password", null, ConfigSupport.helpWithEnv("tls-truststore-password",
                        "Trust store password [required with TLS].")).metavar("<secret>")
                .stringOpt("tls-cert-rotation", null, ConfigSupport.helpWithEnv("tls-cert-rotation",
                        "Swap in rotated certificates on POST /reload, without a restart [default: "
                                + DEFAULT_TLS_CERT_ROTATION + "]."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .group("Security (client access)")
                .stringOpt("client-auth", null, ConfigSupport.helpWithEnv("client-auth",
                        "How clients authenticate: allowall (trusted network; the claimed client "
                                + "id is taken at face value), token (PBKDF2-hashed shared token) or "
                                + "mtls (client cert, CN is the authoritative id) [default: "
                                + DEFAULT_CLIENT_AUTH.cliName() + "]."))
                .metavar("<mode>").choices("allowall", "token", "mtls")
                .stringOpt("client-token-file", null, ConfigSupport.helpWithEnv("client-token-file",
                        "Token file, client.<id>=<pbkdf2 spec>, re-read on POST /reload [token "
                                + "auth: one of this or --client-token-dir].")).metavar("<path>")
                .stringOpt("client-token-dir", null, ConfigSupport.helpWithEnv("client-token-dir",
                        "Directory of <clientId>.token files, rescanned on POST /reload [token "
                                + "auth: one of this or --client-token-file].")).metavar("<path>")
                .stringOpt("client-acl-file", null, ConfigSupport.helpWithEnv("client-acl-file",
                        "Authorization file, acl.<id>=<prefix>:<OPS> ; ... , re-read on POST "
                                + "/reload. Without it every authenticated client may access every key."))
                .metavar("<path>")
                .stringOpt("client-tls", null, ConfigSupport.helpWithEnv("client-tls",
                        "Enable TLS on the client port [default: " + DEFAULT_CLIENT_TLS + "; implied by "
                                + "--client-auth mtls]."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .stringOpt("client-tls-keystore", null, ConfigSupport.helpWithEnv("client-tls-keystore",
                        "PKCS12 key store holding this node's client-port server certificate "
                                + "[required with client TLS].")).metavar("<path>")
                .stringOpt("client-tls-keystore-password", null,
                        ConfigSupport.helpWithEnv("client-tls-keystore-password",
                                "Client-port key store password [required with client TLS]."))
                .metavar("<secret>")
                .stringOpt("client-tls-truststore", null, ConfigSupport.helpWithEnv("client-tls-truststore",
                        "PKCS12 trust store of the CA signing client certificates "
                                + "[required with --client-auth mtls].")).metavar("<path>")
                .stringOpt("client-tls-truststore-password", null,
                        ConfigSupport.helpWithEnv("client-tls-truststore-password",
                                "Client-port trust store password [required with --client-auth mtls]."))
                .metavar("<secret>")
                .stringOpt("client-tls-cert-rotation", null,
                        ConfigSupport.helpWithEnv("client-tls-cert-rotation",
                                "Swap in rotated client-port certificates on POST /reload [default: "
                                        + DEFAULT_CLIENT_TLS_CERT_ROTATION + "]."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .epilogue("All options accept an equivalent DISCAS_* environment variable; the command line "
                        + "wins over the environment. Example:\n"
                        + "  discas-node --node-id 1 --cluster-id demo \\\n"
                        // Inline --members takes bare ids (id=host:port); the node.<id> prefix
                        // belongs to the --members-file properties format, not to this list.
                        + "    --members 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 \\\n"
                        + "    --client-bind 127.0.0.1:7001 --wal-dir /var/lib/discas/1");
    }
}
