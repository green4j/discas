/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.admin.InitOperation;
import io.github.green4j.discas.admin.InitOperation.InitSummary;
import io.github.green4j.discas.admin.InitProgress;
import io.github.green4j.discas.admin.runbook.ClusterPlan;
import io.github.green4j.discas.admin.runbook.Runbook;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.Prompt;
import io.github.green4j.discas.common.cli.config.ConfigResolver;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * {@code discas-admin init} -- builds a folder holding everything a cluster that does not exist yet
 * needs to be started: one data directory per member, the membership file, and {@code RUN.md} with
 * the instructions.
 *
 * <p>The only command here that connects to nothing. With {@code --in} the members are seeded with
 * a dump's pairs; without it they are empty, which is still a cluster somebody has to stand up.
 *
 * <p>What is written into the folder comes from three options -- the membership, the cluster id and
 * the dump. Everything else the runbook needs is asked for on a terminal (and defaulted without
 * one), because those answers change no bytes and only ever appear in {@code RUN.md}.
 */
final class InitCommand extends AbstractCommand {

    InitCommand() {
        super("init",
                "build a new cluster's on-disk state and its instructions, optionally from a dump");
    }

    @Override
    protected void run(final GetOpts opts,
                       final ConfigResolver config,
                       final ProgressLine progressLine) throws Exception {
        final Path outDirectory = Paths.get(config.required("out-dir"));
        final ClusterId clusterId = ClusterId.of(config.required("cluster-id"));
        final Map<NodeId, InetSocketAddress> members =
                ConfigSupport.parseMembers(config.required("members"), "members");
        final String dumpText = config.optional("in");
        final Path dump = dumpText == null ? null : Paths.get(dumpText);

        final ClusterPlan.Builder builder = ClusterPlan.builder()
                .clusterId(clusterId)
                .members(members)
                .dump(dump);
        final ClusterPlan plan = new InitDialogue(Prompt.console())
                .ask(builder, fromFlags(config, builder));

        // Half a cluster's directories are worse than none: somebody would copy one to a host.
        removeOnFailure(outDirectory);

        System.out.println();
        say("cluster " + clusterId.value() + ", members " + members.keySet() + " -> " + outDirectory
                + (dump == null ? " (empty)" : " (from " + dump + ")"));

        // Relative on both counts: the membership was given on the command line and the pair count
        // is in the dump's trailer, so nothing here has to be guessed at.
        final InitProgress progress = (member, memberCount, written, total) -> progress(progressLine,
                "member " + member + "/" + memberCount + ", " + written + "/" + total + " pairs...");

        final InitSummary summary = InitOperation.run(outDirectory, plan, progress);

        finish(progressLine, summary.members() + " member directories, "
                + summary.pairsPerMember() + " pairs each, in " + outDirectory);
        say("read " + outDirectory.resolve(Runbook.FILE_NAME) + " before starting anything.");
    }

    /**
     * Applies the runbook options an operator did set, and records which those were so the dialogue
     * does not ask again. A flag is an answer already given.
     */
    private static InitDialogue.AskedAlready fromFlags(final ConfigResolver config,
                                                       final ClusterPlan.Builder builder) {
        final InitDialogue.AskedAlready asked = new InitDialogue.AskedAlready();

        final String clientPort = config.optional("client-port");
        if (clientPort != null) {
            builder.clientPort(Integer.parseInt(clientPort.trim()));
            asked.clientPort = true;
        }
        final String dataDirectory = config.optional("data-dir");
        if (dataDirectory != null) {
            builder.dataDirectory(dataDirectory);
            asked.dataDirectory = true;
        }
        final String configDirectory = config.optional("config-dir");
        if (configDirectory != null) {
            builder.configDirectory(configDirectory);
            asked.configDirectory = true;
        }
        final String clientAuth = config.optional("client-auth");
        if (clientAuth != null) {
            builder.clientAuth(ClusterPlan.ClientAuth.of(clientAuth.trim()));
            asked.clientAuth = true;
        }
        final String tokenFile = config.optional("client-token-file");
        if (tokenFile != null) {
            builder.tokenFile(tokenFile);
            asked.tokenFile = true;
        }
        final String aclFile = config.optional("client-acl-file");
        if (aclFile != null) {
            builder.aclFile(aclFile);
        }
        builder.peerTls(config.bool("tls", false));
        builder.clientTls(config.bool("client-tls", false));
        return asked;
    }

    @Override
    protected GetOpts options() {
        return new GetOpts(program,
                "Build the on-disk state of a new DisCas cluster in a local folder: one data "
                        + "directory per member, the membership file they share, and a RUN.md with "
                        + "the operator's instructions. With --in, every member is seeded with the "
                        + "pairs of a dump. Every option can also be set by its DISCAS_* "
                        + "environment variable.")
                .group("What is written")
                .stringOpt("out-dir", 'd', ConfigSupport.helpWithEnv("out-dir",
                        "Folder to build. Must not already hold anything.")).metavar("<dir>")
                .stringOpt("cluster-id", 'c', ConfigSupport.helpWithEnv("cluster-id",
                        "Id of the new cluster. Must differ from any cluster these members could "
                                + "reach.")).metavar("<id>")
                .stringOpt("members", 'm', ConfigSupport.helpWithEnv("members",
                        "The new membership, id=host:port,... -- where the members talk to each "
                                + "other. Their count is N, and N is frozen for the cluster's "
                                + "life.")).metavar("<list>")
                .stringOpt("in", 'i', ConfigSupport.helpWithEnv("in",
                        "Dump file to seed every member with [default: none, an empty cluster]."))
                        .metavar("<file>")
                .group("What RUN.md says (asked on a terminal, defaulted without one)")
                .stringOpt("client-port", null, ConfigSupport.helpWithEnv("client-port",
                        "Port every member will listen on for clients [default: 7002]."))
                        .metavar("<port>")
                .stringOpt("data-dir", null, ConfigSupport.helpWithEnv("data-dir",
                        "Where member data directories will live on their hosts "
                                + "[default: /var/lib/discas].")).metavar("<path>")
                .stringOpt("config-dir", null, ConfigSupport.helpWithEnv("config-dir",
                        "Where their configuration will live on their hosts "
                                + "[default: /etc/discas].")).metavar("<path>")
                .stringOpt("client-auth", null, ConfigSupport.helpWithEnv("client-auth",
                        "How clients will authenticate [default: allowall]."))
                        .metavar("<mode>").choices("allowall", "token", "mtls")
                .stringOpt("client-token-file", null, ConfigSupport.helpWithEnv("client-token-file",
                        "Token file path on the members, under token auth "
                                + "[default: <config-dir>/tokens.conf].")).metavar("<path>")
                .stringOpt("client-acl-file", null, ConfigSupport.helpWithEnv("client-acl-file",
                        "ACL file path on the members [default: none -- every authenticated client "
                                + "may touch every key].")).metavar("<path>")
                .stringOpt("tls", null, ConfigSupport.helpWithEnv("tls",
                        "Members will use mTLS between themselves [default: false]."))
                        .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .stringOpt("client-tls", null, ConfigSupport.helpWithEnv("client-tls",
                        "The client port will use TLS [default: false; implied by mtls auth]."))
                        .metavar("<true|false>").choices("true", "false").optionalArg("true")
                .epilogue("  " + program + " -d /tmp/prod-2 -c prod-2 "
                        + "-m 1=10.0.0.1:7001,2=10.0.0.2:7001,3=10.0.0.3:7001 -i /backups/prod.dump\n"
                        + "Every member is seeded with the same pairs at the same fixed low ballot, "
                        + "each with its own freshly minted incarnation, so the members are "
                        + "identical from their first second and have nothing to reconcile. "
                        + "Nothing here is a restore into a running cluster: for that, and only "
                        + "for that, there is 'discas-admin load'.")
                .width(100);
    }
}
