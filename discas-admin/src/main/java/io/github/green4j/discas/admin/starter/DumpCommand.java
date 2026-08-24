/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.admin.DumpOperation;
import io.github.green4j.discas.client.dump.ClusterDump.DumpSummary;
import io.github.green4j.discas.client.dump.DumpProgress;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigResolver;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * {@code discas-admin dump} -- writes a cluster's live pairs to a dump file.
 *
 * <p>Each positional argument is a key prefix; with none, the whole key space is dumped. The prefix
 * set is the operator's only filter and the whole of it -- nothing reads a value, so a prefix that
 * holds locks dumps locks.
 */
final class DumpCommand extends AbstractCommand {

    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 30_000L;

    DumpCommand() {
        super("dump", "write a cluster's live pairs to a dump file");
    }

    @Override
    protected void run(final GetOpts opts,
                       final ConfigResolver config,
                       final ProgressLine progressLine) throws Exception {
        final Map<NodeId, InetSocketAddress> nodes =
                ConfigSupport.parseMembers(config.required("nodes"), "nodes");
        final ClientId clientId =
                ClientId.of(config.optional("client-id", ClusterOptions.DEFAULT_CLIENT_ID));
        final String token = config.secret("token");
        final long connectTimeoutMs =
                config.longAtLeast("connect-timeout-ms", DEFAULT_CONNECT_TIMEOUT_MS, 0L);
        final Path out = Paths.get(config.required("out"));
        final List<String> prefixText = opts.positionals();
        final List<ByteBuffer> prefixes = utf8(prefixText);

        // What is on disk before the trailer is an uncommitted dump, which every reader refuses --
        // but a tool that reports failure and leaves a file leaves someone believing they have a
        // backup.
        removeOnFailure(out);

        say(nodes.keySet() + " -> " + out + " ("
                + (prefixText.isEmpty() ? "the whole key space" : "prefixes " + prefixText) + ")");

        // Absolute, because that is all a dump knows: the scan has no count in front of it, so
        // there is no fraction to show and no honest estimate of what is left.
        final DumpProgress progress = (entries, bytes) -> progress(progressLine,
                entries + " pairs, " + ProgressLine.humanBytes(bytes) + "...");
        final DumpSummary summary = DumpOperation.run(
                nodes, clientId, token, prefixes, out, connectTimeoutMs, progress);

        finish(progressLine, summary.entries() + " pairs, "
                + ProgressLine.humanBytes(Files.size(out)) + " written to " + out
                + (summary.keysVanished() == 0 ? ""
                : " (" + summary.keysVanished() + " keys were deleted mid-dump)"));
    }

    @Override
    protected GetOpts options() {
        return ClusterOptions.declare(new GetOpts(program,
                "Write the live key/value pairs of a DisCas cluster to a dump file, which "
                        + "'discas-admin load' can write into another cluster and 'discas-admin init' "
                        + "can seed a new one from. Every option can also be set by its DISCAS_* "
                        + "environment variable; a flag overrides the environment variable, which "
                        + "overrides the default."))
                .stringOpt("out", 'o', ConfigSupport.helpWithEnv("out",
                        "File to write. Must not already exist.")).metavar("<file>")
                .stringOpt("connect-timeout-ms", null, ConfigSupport.helpWithEnv("connect-timeout-ms",
                        "How long to wait for the cluster to answer before giving up [default: "
                                + DEFAULT_CONNECT_TIMEOUT_MS + "].")).metavar("<millis>")
                .epilogue("Each positional argument is a key prefix to carry; with none, the whole "
                        + "key space is carried. Nothing here reads a value, so a prefix that holds "
                        + "locks dumps locks.\n"
                        + "  " + program + " -N 1=10.0.0.1:7001,2=10.0.0.2:7001 "
                        + "-o /backups/prod.dump users/ orders/\n"
                        + "A dump is a scan followed by a linearizable read per key: it costs the "
                        + "cluster a round trip per key and is not a point-in-time snapshot. Stop "
                        + "writes first if the pairs have to agree with each other.")
                .width(100);
    }
}
