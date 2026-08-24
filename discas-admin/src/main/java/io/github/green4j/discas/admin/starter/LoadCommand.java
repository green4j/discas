/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.admin.LoadOperation;
import io.github.green4j.discas.client.dump.ClusterLoad.LoadSummary;
import io.github.green4j.discas.client.dump.LoadProgress;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigResolver;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * {@code discas-admin load} -- writes a dump's pairs into a running cluster, overwriting whatever is
 * at those keys.
 *
 * <p>A merge, not a replacement: keys the dump does not mention are left alone, and every pair goes
 * in as an ordinary write. {@code --cleanup} turns it into a replacement of the prefixes given as
 * positional arguments. Seeding a cluster that does not exist yet is {@code init}.
 *
 * <p>Nothing is registered with {@code removeOnFailure}: the pairs already written are ordinary
 * values in a live cluster, and taking them back out would be a guess about what was there before.
 */
final class LoadCommand extends AbstractCommand {

    LoadCommand() {
        super("load", "write a dump's pairs into a running cluster, overwriting duplicates");
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
        final Path in = Paths.get(config.required("in"));

        final boolean cleanup = opts.getBool("cleanup");
        final List<String> scopeText = opts.positionals();
        if (!cleanup && !scopeText.isEmpty()) {
            throw new IllegalArgumentException("Prefixes " + scopeText
                    + " given without --cleanup; a load without cleanup writes the dump's keys and "
                    + "touches nothing else, so a prefix would have nothing to bound");
        }
        final List<ByteBuffer> cleanupPrefixes = cleanup ? utf8(scopeText) : null;

        say(in + " -> " + nodes.keySet() + (cleanup ? ", then deleting keys not in it under "
                + (scopeText.isEmpty() ? "the whole key space" : scopeText) : ""));

        final LoadProgress progress = new LoadProgress() {
            @Override
            public void advanced(final long written, final long total) {
                // Relative: the dump's trailer carries the count, and the reader has proven the
                // file whole before the first write.
                progress(progressLine,
                        written + "/" + total + " pairs (" + percent(written, total) + ")...");
            }

            @Override
            public void cleaning(final long examined, final long deleted) {
                // Absolute again: this walks the cluster's key space, which has no count.
                progress(progressLine,
                        "cleanup, " + examined + " keys examined, " + deleted + " deleted...");
            }
        };
        final LoadSummary summary =
                LoadOperation.run(nodes, clientId, token, in, cleanupPrefixes, progress);

        finish(progressLine, summary.written() + " pairs written to " + nodes.keySet()
                + (cleanup ? ", " + summary.deleted() + " stale keys deleted" : ""));
    }

    private static String percent(final long written, final long total) {
        return total == 0 ? "100%" : (written * 100 / total) + "%";
    }

    @Override
    protected GetOpts options() {
        return ClusterOptions.declare(new GetOpts(program,
                "Write the pairs of a dump file into a running DisCas cluster. Every key in the "
                        + "dump is overwritten; keys the dump does not mention are left alone. "
                        + "Every option can also be set by its DISCAS_* environment variable."))
                .stringOpt("in", 'i', ConfigSupport.helpWithEnv("in",
                        "Dump file to read. Refused unless it is a whole one.")).metavar("<file>")
                .flag("cleanup", null,
                        "After every pair is in, delete the keys the dump did not carry, so the "
                                + "loaded key space holds exactly what the dump holds. Each "
                                + "positional argument is a prefix bounding where it looks; with "
                                + "none, it looks at the whole key space.")
                .epilogue("  " + program + " -N 1=10.0.0.1:7001,2=10.0.0.2:7001 -i /backups/prod.dump\n"
                        + "  " + program + " -N 1=10.0.0.1:7001 -i prod.dump --cleanup users/\n"
                        + "Pairs land one at a time, as ordinary writes: an interrupted load leaves "
                        + "the keys it reached written and the rest as they were, and a writer "
                        + "racing the load wins or loses by ballot order the way two writers always "
                        + "do. Cleanup holds the dump's keys (never its values) in memory, deletes "
                        + "unfenced, and cannot tell a key somebody wrote during the load from a "
                        + "stale one -- it is for a key space you have taken charge of, not one "
                        + "under live traffic. To seed a cluster that does not exist yet, use "
                        + "'discas-admin init'.")
                .width(100);
    }
}
