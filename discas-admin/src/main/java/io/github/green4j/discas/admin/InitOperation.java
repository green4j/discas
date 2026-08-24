/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin;

import io.github.green4j.discas.admin.runbook.ClusterPlan;
import io.github.green4j.discas.admin.runbook.Runbook;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.seed.MemberSeed;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds, in one local folder, everything needed to start a cluster that does not exist yet: one
 * data directory per member, the membership file they share, and {@code RUN.md} telling an operator
 * what to do with them.
 *
 * <p>Nothing here connects to anything. The whole point is that the cluster is not running: its
 * members' state is written here, carried to their hosts, and started. When there is a dump, that
 * state is the dump's pairs; when there is not, the members are empty and this is simply a correct
 * empty cluster with its instructions.
 *
 * <p>Four steps, in this order and for a reason: <b>refuse a folder that holds anything</b>, seed
 * every member, write the membership, write the runbook. The runbook is last because it states what
 * happened -- the pair count in it is the one that was written, not the one that was intended.
 */
public final class InitOperation {

    private InitOperation() {
    }

    /** What was written, for the line the tool prints and for the runbook itself. */
    public static final class InitSummary {
        private final int members;
        private final long pairsPerMember;

        InitSummary(final int members, final long pairsPerMember) {
            this.members = members;
            this.pairsPerMember = pairsPerMember;
        }

        public int members() {
            return members;
        }

        /** Pairs seeded into each member -- the same number in every directory. */
        public long pairsPerMember() {
            return pairsPerMember;
        }
    }

    /** @param outDirectory created; must not already hold anything */
    public static InitSummary run(final Path outDirectory,
                                  final ClusterPlan plan,
                                  final InitProgress progress) throws IOException {
        final InitProgress report = progress == null ? InitProgress.NONE : progress;

        requireEmpty(outDirectory);
        Files.createDirectories(outDirectory);

        final long pairs = seedMembers(outDirectory, plan, report);
        writeMembersFile(outDirectory, plan);

        final InitSummary summary = new InitSummary(plan.members().size(), pairs);
        writeRunbook(outDirectory, plan, summary);
        return summary;
    }

    /**
     * One directory per member, each with its own freshly minted incarnation and, otherwise,
     * byte-identical state.
     */
    private static long seedMembers(final Path outDirectory,
                                    final ClusterPlan plan,
                                    final InitProgress progress) throws IOException {
        final List<NodeId> ids = new ArrayList<>(plan.members().keySet());
        long pairs = 0;
        for (int i = 0; i < ids.size(); i++) {
            final int member = i + 1;
            pairs = MemberSeed.write(outDirectory.resolve(ids.get(i).value()), plan.dump(),
                    (written, total) -> progress.advanced(member, ids.size(), written, total));
        }
        return pairs;
    }

    private static void writeMembersFile(final Path outDirectory, final ClusterPlan plan)
            throws IOException {
        Files.write(outDirectory.resolve(Runbook.MEMBERS_FILE_NAME),
                Runbook.membersFile(plan.members()).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The instructions, written into the folder rather than printed: the folder is what gets copied
     * to a jump host, handed to a colleague, or opened at three in the morning six months from now,
     * and by then nobody has the terminal that produced it.
     */
    private static void writeRunbook(final Path outDirectory,
                                     final ClusterPlan plan,
                                     final InitSummary summary) throws IOException {
        Files.write(outDirectory.resolve(Runbook.FILE_NAME),
                Runbook.render(plan, summary.pairsPerMember()).getBytes(StandardCharsets.UTF_8));
    }

    private static void requireEmpty(final Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        if (!Files.isDirectory(directory)) {
            throw new IOException(directory + " is not a directory");
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            if (entries.iterator().hasNext()) {
                throw new IOException(directory + " is not empty; init writes a whole cluster's "
                        + "state and will not mix it with whatever is already there");
            }
        }
    }
}
