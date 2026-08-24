/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent.starter;

import io.github.green4j.discas.agent.DisCasAgent;
import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigResolver;
import io.github.green4j.discas.common.cli.config.ConfigSource;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.observability.ObservabilityConfig;

import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * The effective, resolved configuration for a {@link DisCasAgent}, assembled from a <b>dual</b>
 * source: command-line flags (parsed with {@link GetOpts}) and {@code DISCAS_*} environment
 * variables. Each property is settable by <em>either</em> source; the per-property precedence is
 * {@code CLI > ENV > DEFAULT}, decided independently for every value by the shared
 * {@link ConfigResolver}.
 * <p>
 * The {@code DISCAS_*} name of a property is derived mechanically from its long option name
 * ({@code UPPER_SNAKE}, {@code '-'} to {@code '_'}), so {@code --nodes} pairs with
 * {@code DISCAS_NODES}, {@code --http-bind} with {@code DISCAS_HTTP_BIND}, and so on.
 * <p>
 * {@link #resolve(String[], Map)} takes the environment as a parameter (rather than reading
 * {@link System#getenv()} directly) so it is fully unit-testable; the entry point passes the real
 * environment. {@link #describe()} renders every effective value with the source it came from,
 * masking security-sensitive values. This mirrors {@code DisCasNodeConfig} in {@code discas-node};
 * the two share the resolution toolkit in {@code discas-common} but keep their own option catalogs
 * so the agent does not depend on {@code discas-node}.
 */
public final class DisCasAgentConfig {

    public static final String PROGRAM = "discas-agent";

    private static final String DEFAULT_CLIENT_ID = "agent";
    private static final String DEFAULT_HTTP_BIND = "127.0.0.1:8500";
    private static final long DEFAULT_REQUEST_TIMEOUT_SECONDS = 10L;
    private static final boolean DEFAULT_TLS = false;
    private static final boolean DEFAULT_TLS_CERT_ROTATION = true;

    /**
     * One past the node's 9600, so an agent and a node on the same host both get their metrics
     * scraped without either being reconfigured -- the same reason the node's own default sits
     * clear of the agent's 8500.
     */
    private static final String DEFAULT_OBSERVABILITY_BIND = "127.0.0.1:9601";

    /**
     * Client transport sizing comes from the transport's own defaults rather than being restated
     * here: the node and the agent open the same kind of connection.
     */
    private static final ClientTransportConfig CLIENT_DEFAULTS = ClientTransportConfig.defaults();

    /** Shared with the node, so {@code enabled} and {@code workerCount} do not drift between them. */
    private static final ObservabilityConfig OBSERVABILITY_DEFAULTS = ObservabilityConfig.builder().build();

    // Identity of this agent's client connection to the cluster.
    public final ClientId clientId;

    // Target nodes this agent connects to (id -> host:port). Populated from either mode; in file
    // mode this is the initial view, refreshed at runtime by DisCasAgent's watcher.
    public final Map<NodeId, InetSocketAddress> nodes;

    // Membership: exactly one of the two modes is active.
    public final Path nodesFile; // non-null => file mode (hot-reloaded at runtime)

    // Optional client authentication token sent to the nodes; null for AllowAll/mTLS.
    public final String token;

    public final InetSocketAddress httpBind;
    public final int httpWorkers;

    /**
     * The metrics endpoint, on a port of its own ({@code --observability-bind},
     * {@code --observability-workers}, {@code --observability-enabled}). Separate from the KV
     * surface for the same reason the node keeps one: a scrape target and a data plane have
     * different audiences, and metrics name every node this agent talks to.
     */
    public final ObservabilityConfig observabilityConfig;

    // Per-request wait on a client operation before the agent returns 504.
    public final Duration requestTimeout;

    public final ClientTransportConfig clientTransportConfig;

    // TLS / mTLS for the outbound client connection.
    public final boolean tls;
    public final Path tlsKeystore;
    public final char[] tlsKeystorePassword;
    public final Path tlsTruststore;
    public final char[] tlsTruststorePassword;
    public final boolean tlsCertRotation;

    private final ConfigResolver resolver;

    private DisCasAgentConfig(final String[] args, final Map<String, String> env) {
        final GetOpts opts = newOpts();
        opts.parseOrThrow(args);

        final ConfigResolver r = new ConfigResolver(PROGRAM, opts, env);
        this.resolver = r;

        this.clientId = ClientId.of(r.optional("client-id", DEFAULT_CLIENT_ID));

        // Membership: exactly one of --nodes-file / --nodes.
        final String nodesFileStr = r.optional("nodes-file");
        final String nodesStr = r.optional("nodes");
        if ((nodesFileStr == null) == (nodesStr == null)) {
            throw new IllegalArgumentException(
                    "Exactly one of --nodes-file (" + ConfigSupport.envName("nodes-file")
                            + ") or --nodes (" + ConfigSupport.envName("nodes") + ") must be set");
        }
        if (nodesFileStr != null) {
            this.nodesFile = Path.of(nodesFileStr);
            this.nodes = ConfigSupport.parseMembersFile(this.nodesFile, "nodes");
        } else {
            this.nodesFile = null;
            this.nodes = ConfigSupport.parseMembers(nodesStr, "nodes");
        }

        this.token = r.secret("token");

        // http-bind allows port 0 (ephemeral) -- useful for tests and dynamic port assignment.
        this.httpBind = ConfigSupport.parseAddress(r.optional("http-bind", DEFAULT_HTTP_BIND), "http-bind", true);
        this.httpWorkers = r.integerAtLeast(
                "http-workers", Runtime.getRuntime().availableProcessors(), 1);

        this.requestTimeout = Duration.ofSeconds(
                r.longAtLeast("request-timeout-seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS, 1));

        // Port 0 (ephemeral) is allowed here too, for the same reason as http-bind.
        final InetSocketAddress observabilityBind = ConfigSupport.parseAddress(
                r.optional("observability-bind", DEFAULT_OBSERVABILITY_BIND),
                "observability-bind", true);
        this.observabilityConfig = ObservabilityConfig.builder()
                .enabled(r.bool("observability-enabled", OBSERVABILITY_DEFAULTS.enabled()))
                .bindAddress(observabilityBind.getHostString())
                .port(observabilityBind.getPort())
                .workerCount(r.integerAtLeast(
                        "observability-workers", OBSERVABILITY_DEFAULTS.workerCount(), 1))
                .build();

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

        // TLS / mTLS. Two modes, selected by the presence of a keystore when TLS is on:
        //   * keystore present  -> mTLS (client presents a cert with CN=clientId);
        //   * keystore absent    -> server-authenticated TLS, identity from --token.
        this.tls = r.bool("tls", DEFAULT_TLS);
        final String keystore = r.optional("tls-keystore");
        final String keystorePw = r.secret("tls-keystore-password");
        final String truststore = r.optional("tls-truststore");
        final String truststorePw = r.secret("tls-truststore-password");
        this.tlsCertRotation = r.bool("tls-cert-rotation", DEFAULT_TLS_CERT_ROTATION);
        if (tls) {
            ConfigSupport.requirePresent(truststore, "tls-truststore");
            ConfigSupport.requirePresent(truststorePw, "tls-truststore-password");
            if (keystore != null) {
                ConfigSupport.requirePresent(keystorePw, "tls-keystore-password");
            } else if (token == null) {
                throw new IllegalArgumentException(
                        "TLS without a client keystore requires --token (" + ConfigSupport.envName("token")
                                + "): the encrypted channel still needs a client identity");
            }
        } else if (keystore != null || truststore != null) {
            throw new IllegalArgumentException(
                    "--tls-keystore/--tls-truststore set but --tls (" + ConfigSupport.envName("tls") + ") is off");
        }
        this.tlsKeystore = keystore == null ? null : Path.of(keystore);
        this.tlsKeystorePassword = keystorePw == null ? null : keystorePw.toCharArray();
        this.tlsTruststore = truststore == null ? null : Path.of(truststore);
        this.tlsTruststorePassword = truststorePw == null ? null : truststorePw.toCharArray();
    }

    /**
     * Resolve the effective configuration from CLI {@code args} and an environment map. Throws
     * {@link IllegalArgumentException} on a missing required value or an invalid value, and
     * {@link GetOpts.ParseException} on a malformed command line. Never calls {@link System#exit}.
     */
    public static DisCasAgentConfig resolve(final String[] args, final Map<String, String> env) {
        return new DisCasAgentConfig(args, env == null ? Map.of() : env);
    }

    /** True if {@code args} explicitly requests help ({@code -h} / {@code --help}). */
    public static boolean isHelpRequested(final String[] args) {
        return ConfigSupport.isHelpRequested(args);
    }

    public static String helpText() {
        return newOpts().helpString();
    }

    public static String usageText() {
        return newOpts().usageString();
    }

    public boolean nodesFromFile() {
        return nodesFile != null;
    }

    /**
     * A {@code PROPERTY | VALUE | SOURCE} table of every effective value and where it came from.
     * Security-sensitive values are masked.
     */
    public String describe() {
        return resolver.describe();
    }

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

    private static GetOpts newOpts() {
        return new GetOpts(PROGRAM,
                "Run a local DisCas agent: a long-lived DisCasClient connected to a running cluster, "
                        + "re-exposed over HTTP (Consul-agent style). Every option can also be set by its "
                        + "DISCAS_* environment variable; a flag overrides the environment variable, which "
                        + "overrides the default.")
                .group("Cluster connection")
                .stringOpt("nodes", 'N', ConfigSupport.helpWithEnv("nodes",
                        "Target nodes id=host:port,id2=host:port,... "
                                + "Mutually exclusive with --nodes-file.")).metavar("<list>")
                .stringOpt("nodes-file", null, ConfigSupport.helpWithEnv("nodes-file",
                        "Path to a hot-reloaded nodes file, node.<id>=host:port (same format as the "
                                + "node's --members-file). Mutually exclusive with --nodes.")).metavar("<path>")
                .stringOpt("client-id", 'i', ConfigSupport.helpWithEnv("client-id",
                        "This agent's client id [default: " + DEFAULT_CLIENT_ID + "].")).metavar("<id>")
                .stringOpt("token", null, ConfigSupport.helpWithEnv("token",
                        "Client authentication token sent to the nodes [default: none].")).metavar("<secret>")
                .group("HTTP front-end")
                .stringOpt("http-bind", 'b', ConfigSupport.helpWithEnv("http-bind",
                        "host:port the agent binds for HTTP [default: " + DEFAULT_HTTP_BIND + "]."))
                .metavar("<host:port>")
                .stringOpt("http-workers", null, ConfigSupport.helpWithEnv("http-workers",
                        "HTTP worker threads [default: available processors].")).metavar("<n>")
                .group("Observability")
                .stringOpt("observability-enabled", null,
                        ConfigSupport.helpWithEnv("observability-enabled",
                        "Serve the metrics endpoint [default: "
                                + OBSERVABILITY_DEFAULTS.enabled() + "].")).metavar("<true|false>")
                .stringOpt("observability-bind", null,
                        ConfigSupport.helpWithEnv("observability-bind",
                        "host:port for GET /metrics; loopback by default because the exposition "
                                + "names every node this agent talks to [default: "
                                + DEFAULT_OBSERVABILITY_BIND + "].")).metavar("<host:port>")
                .stringOpt("observability-workers", null,
                        ConfigSupport.helpWithEnv("observability-workers",
                        "HTTP worker threads for the metrics endpoint [default: "
                                + OBSERVABILITY_DEFAULTS.workerCount() + "].")).metavar("<n>")
                .group("Runtime")
                .stringOpt("request-timeout-seconds", null, ConfigSupport.helpWithEnv("request-timeout-seconds",
                        "Per-request wait on a client op before 504 [default: "
                                + DEFAULT_REQUEST_TIMEOUT_SECONDS + "].")).metavar("<seconds>")
                .stringOpt("client-max-frame-bytes", null, ConfigSupport.helpWithEnv("client-max-frame-bytes",
                        "Client transport max frame bytes [default: "
                                + CLIENT_DEFAULTS.maxFrameBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-queued-out-bytes", null, ConfigSupport.helpWithEnv("client-max-queued-out-bytes",
                        "Client transport tx queue bytes [default: "
                                + CLIENT_DEFAULTS.maxQueuedOutBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-rx-buffer-bytes", null, ConfigSupport.helpWithEnv("client-max-rx-buffer-bytes",
                        "Client transport rx buffer bytes [default: "
                                + CLIENT_DEFAULTS.maxRxBufferBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-inflight-bytes", null, ConfigSupport.helpWithEnv("client-max-inflight-bytes",
                        "Client transport inflight bytes [default: "
                                + CLIENT_DEFAULTS.maxInflightBytes() + "].")).metavar("<bytes>")
                .stringOpt("client-max-connections", null, ConfigSupport.helpWithEnv("client-max-connections",
                        "Client transport max connections [default: "
                                + CLIENT_DEFAULTS.maxConnections() + "].")).metavar("<n>")
                .group("Security (TLS/mTLS)")
                .stringOpt("tls", null, ConfigSupport.helpWithEnv("tls",
                        "Encrypt the client connection with TLS [default: " + DEFAULT_TLS + "]. With a keystore this "
                                + "is mTLS (client cert CN=client-id); without one, identity is the token."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .stringOpt("tls-keystore", null, ConfigSupport.helpWithEnv("tls-keystore",
                        "PKCS12 key store path holding this client's cert+key (CN=client-id) [mTLS]."))
                .metavar("<path>")
                .stringOpt("tls-keystore-password", null, ConfigSupport.helpWithEnv("tls-keystore-password",
                        "Key store password [required with a keystore].")).metavar("<secret>")
                .stringOpt("tls-truststore", null, ConfigSupport.helpWithEnv("tls-truststore",
                        "PKCS12 trust store path verifying the node cert [required with TLS].")).metavar("<path>")
                .stringOpt("tls-truststore-password", null, ConfigSupport.helpWithEnv("tls-truststore-password",
                        "Trust store password [required with TLS].")).metavar("<secret>")
                .stringOpt("tls-cert-rotation", null, ConfigSupport.helpWithEnv("tls-cert-rotation",
                        "Hot-reload rotated client certificates [default: "
                                + DEFAULT_TLS_CERT_ROTATION + "; mTLS only]."))
                .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .epilogue("All options accept an equivalent DISCAS_* environment variable; the command line "
                        + "wins over the environment. Provide the target nodes inline or in a hot-reloaded "
                        + "file. Examples:\n"
                        + "  discas-agent --nodes 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \\\n"
                        + "    --http-bind 127.0.0.1:8500\n"
                        + "  discas-agent --nodes-file /etc/discas/nodes.conf --http-bind 127.0.0.1:8500");
    }
}
