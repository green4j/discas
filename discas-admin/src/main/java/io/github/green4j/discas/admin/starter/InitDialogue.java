/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.admin.runbook.ClusterPlan;
import io.github.green4j.discas.admin.runbook.ClusterPlan.ClientAuth;
import io.github.green4j.discas.common.cli.Prompt;

/**
 * The questions {@code init} asks, in the order an operator thinks about them: where the files will
 * live, how clients get in, what they may touch, and what is encrypted.
 *
 * <p>None of these answers change a single byte of what is written into the folder -- they are what
 * {@code RUN.md} has to say to be followable. The alternative was a runbook full of
 * {@code <client-port>} brackets, which is a runbook somebody finishes wrongly.
 *
 * <p>Every question has a default and a flag. Answered by flag, it is not asked; unanswered on a
 * terminal, it is asked; unanswered with no terminal, the default stands and the runbook says what
 * was assumed. So the same command line works in a script and in a shell.
 */
final class InitDialogue {

    private final Prompt prompt;

    InitDialogue(final Prompt prompt) {
        this.prompt = prompt;
    }

    /**
     * Fills in everything the runbook needs that {@code builder} does not already carry from flags.
     *
     * @param fromFlags names of options the operator set explicitly, which are therefore not asked
     */
    ClusterPlan ask(final ClusterPlan.Builder builder, final AskedAlready fromFlags) {
        if (prompt.interactive()) {
            prompt.heading("Starting this cluster");
            prompt.say("These answers do not change what is written into the folder. They are what");
            prompt.say("RUN.md needs to say to be followable. Enter accepts the default in [ ].");
        }
        hosts(builder, fromFlags);
        clientAccess(builder, fromFlags);
        authorization(builder);
        transportSecurity(builder);
        return builder.build();
    }

    /** Where the members' files will live once they are on their hosts, and where clients call. */
    private void hosts(final ClusterPlan.Builder builder, final AskedAlready fromFlags) {
        if (!fromFlags.clientPort) {
            prompt.heading("Ports and paths");
            prompt.say("A membership says where members talk to each other. Everything below is");
            prompt.say("about the hosts they will run on.");
            builder.clientPort(number(
                    "Which port will every member listen on for clients?", 7002));
        }
        if (!fromFlags.dataDirectory) {
            builder.dataDirectory(prompt.ask(
                    "Where will member data directories live on their hosts? "
                            + "(each member's is <dir>/<id>)", "/var/lib/discas"));
        }
        if (!fromFlags.configDirectory) {
            builder.configDirectory(prompt.ask(
                    "Where will their configuration live? (members.conf, and any token or ACL file)",
                    "/etc/discas"));
        }
    }

    /** How a client proves who it is -- and, under token auth, where the tokens are kept. */
    private void clientAccess(final ClusterPlan.Builder builder, final AskedAlready fromFlags) {
        final ClientAuth auth;
        if (fromFlags.clientAuth) {
            auth = builder.clientAuthValue();
        } else {
            prompt.heading("How clients authenticate");
            prompt.say("The node's --client-auth. It cannot be changed without restarting members,");
            prompt.say("so it is worth a moment now.");
            auth = prompt.choose("Which mode?", Prompt.choices(
                    Prompt.choice(ClientAuth.ALLOWALL, "allowall",
                            "trusted network; a client's claimed id is believed"),
                    Prompt.choice(ClientAuth.TOKEN, "token",
                            "a PBKDF2-hashed shared token per client id, hot-reloaded from a file"),
                    Prompt.choice(ClientAuth.MTLS, "mtls",
                            "a client certificate; its CN is the id, and the client port runs TLS")),
                    0);
            builder.clientAuth(auth);
        }
        if (auth == ClientAuth.TOKEN && !fromFlags.tokenFile) {
            // Asked even without a terminal, where it answers with the default: a token file's path
            // is in the start command either way, and a blank there is a node that will not start.
            builder.tokenFile(prompt.ask("Where will the token file live on the members?",
                    builder.configFile("tokens.conf")));
        }
    }

    /** Whether there is an ACL file at all -- without one, every authenticated client sees all. */
    private void authorization(final ClusterPlan.Builder builder) {
        if (builder.aclFileValue() != null) {
            return;
        }
        prompt.heading("What clients may touch");
        prompt.say("Without an ACL file, every client that authenticates may read and write every");
        prompt.say("key. That is fine for one application and wrong for a shared cluster.");
        if (prompt.confirm("Will this cluster use an ACL file?", false)) {
            builder.aclFile(prompt.ask("Where will it live on the members?",
                    builder.configFile("acl.conf")));
        }
    }

    /** TLS between members, and on the client port when the auth mode has not already decided it. */
    private void transportSecurity(final ClusterPlan.Builder builder) {
        prompt.heading("Transport security");
        prompt.say("Key store paths are assumed to sit in the configuration directory; RUN.md names");
        prompt.say("them, and passwords go in the environment, never in the runbook.");
        builder.peerTls(prompt.confirm("Encrypt traffic between members (mTLS on the peer port)?",
                builder.peerTlsValue()));
        if (builder.clientAuthValue() == ClientAuth.MTLS) {
            // Not a question: a client certificate cannot be presented over a plaintext connection.
            return;
        }
        builder.clientTls(prompt.confirm("Encrypt the client port (TLS)?",
                builder.clientTlsValue()));
    }

    private int number(final String question, final int defaultValue) {
        for (;;) {
            final String answer = prompt.ask(question, Integer.toString(defaultValue));
            try {
                final int parsed = Integer.parseInt(answer.trim());
                if (parsed >= 1 && parsed <= 65535) {
                    return parsed;
                }
            } catch (final NumberFormatException notANumber) {
                // asked again below
            }
            if (!prompt.interactive()) {
                return defaultValue;
            }
        }
    }

    /** Which answers came from the command line, and so are not asked for again. */
    static final class AskedAlready {
        boolean clientPort;
        boolean dataDirectory;
        boolean configDirectory;
        boolean clientAuth;
        boolean tokenFile;
    }
}
