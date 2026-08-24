/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigSupport;

import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * {@code discas-admin} -- the operator's command line for a DisCas cluster. One program, one command
 * per operation:
 *
 * <pre>
 *   discas-admin dump --nodes 1=host:7001,... --out prod.dump  users/ orders/
 *   discas-admin load --nodes 1=host:7001,... --in prod.dump --cleanup users/
 *   discas-admin init --out-dir ./prod-2 --cluster-id prod-2 --members 1=host:7001,... --in prod.dump
 *   discas-admin token --client-id web-1 --ttl-days 30
 * </pre>
 *
 * <p>Separate from {@code discas-agent}, which is a long-lived sidecar answering HTTP; these are
 * things an operator does once, standing in front of a cluster, with a file. Behind an endpoint a
 * backup would require running the agent, and {@code init}, which writes a data directory before
 * any cluster exists, could not live there at all.
 *
 * <p>This class picks a command and gets out of the way: the options, the work and the words each
 * command prints are the command's own. What every command shares is in {@link AbstractCommand},
 * and it is shared by being inherited, not by being discovered.
 */
public final class DisCasAdminStarter {

    static final String PROGRAM = "discas-admin";

    /**
     * Every command there is, in the order they are listed to an operator.
     * <p>
     * A plain list, written out by hand, and that is the design: nothing is scanned for, no
     * annotation marks a class as a command, and no service file has to agree with the code. A
     * command that is not on this list does not exist -- which means the answer to "what can this
     * tool do" is the list itself, and adding a line to it is the only registration step there is.
     */
    private static final List<Command> ALL = Arrays.asList(
            new DumpCommand(),
            new LoadCommand(),
            new InitCommand(),
            new TokenCommand());

    private static final Map<String, Command> COMMANDS = byName(ALL);

    private DisCasAdminStarter() {
    }

    public static void main(final String[] args) {
        if (args.length == 0 || isHelp(args[0])) {
            usage(System.out);
            return;
        }
        final Command command = COMMANDS.get(args[0]);
        if (command == null) {
            System.err.println(PROGRAM + ": unknown command '" + args[0] + "'");
            System.err.println();
            usage(System.err);
            System.exit(2);
            return;
        }
        try {
            command.run(Arrays.copyOfRange(args, 1, args.length));
        } catch (final GetOpts.ParseException | IllegalArgumentException misuse) {
            // The command line itself is wrong: exit 2, printed in the one shape every discas
            // executable prints it in, so a script can tell "you asked wrongly" from "it did not
            // work" and an operator meets the same page whichever program they ran.
            ConfigSupport.usageError(PROGRAM + " " + command.name(),
                    misuse.getMessage(), command.usageText());
        } catch (final Exception failed) {
            System.err.println(PROGRAM + " " + command.name() + ": " + rootCause(failed));
            System.exit(1);
        }
    }

    private static Map<String, Command> byName(final List<Command> commands) {
        final Map<String, Command> index = new LinkedHashMap<>();
        for (final Command command : commands) {
            if (index.put(command.name(), command) != null) {
                // A typo in the list above, and one that would otherwise show as a command quietly
                // shadowing another; this class is loaded before anything runs, so it fails at once.
                throw new IllegalStateException("Two commands named '" + command.name() + "'");
            }
        }
        return index;
    }

    private static boolean isHelp(final String arg) {
        return "-h".equals(arg) || "--help".equals(arg) || "help".equals(arg);
    }

    private static void usage(final PrintStream out) {
        out.println("usage: " + PROGRAM + " <command> [options]");
        out.println();
        out.println("commands:");
        for (final Command command : ALL) {
            out.println("  " + pad(command.name()) + command.summary());
        }
        out.println();
        out.println("Try '" + PROGRAM + " <command> --help' for a command's options.");
    }

    private static String pad(final String name) {
        final StringBuilder padded = new StringBuilder(name);
        while (padded.length() < 12) {
            padded.append(' ');
        }
        return padded.toString();
    }

    /**
     * The message an operator can act on: the outermost one that actually says something.
     *
     * <p>Not the innermost, which throws away the sentence somebody wrote: "the cluster did not
     * answer within 30000ms" reduced to {@code java.util.concurrent.TimeoutException} is the same
     * fact with the meaning removed. What does get unwrapped is the plumbing that carries no
     * message of its own, and a throwable with no message falls through to its cause.
     */
    private static String rootCause(final Throwable failure) {
        Throwable cause = failure;
        while (carriesNoMeaning(cause) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof FileSystemException) {
            return fileProblem((FileSystemException) cause);
        }
        final String message = cause.getMessage();
        if (message != null) {
            return message;
        }
        return cause.getCause() != null
                ? rootCause(cause.getCause()) : cause.getClass().getSimpleName();
    }

    /** Plumbing whose message is only its cause's {@code toString()}. */
    private static boolean carriesNoMeaning(final Throwable failure) {
        return failure instanceof ExecutionException
                || failure instanceof CompletionException
                || failure instanceof UncheckedIOException;
    }

    /**
     * A file failure in words. The JDK's file exceptions carry the path and, usually, nothing else
     * -- {@code NoSuchFileException.getMessage()} is the file name alone, so reporting the message
     * printed a bare path and left the operator to guess what was wrong with it. These three are
     * every file failure this tool meets in practice; anything else keeps its type, which is more
     * use than a path on its own.
     */
    private static String fileProblem(final FileSystemException failure) {
        final String reason;
        if (failure.getReason() != null) {
            reason = failure.getReason().trim();
        } else if (failure instanceof NoSuchFileException) {
            reason = "no such file";
        } else if (failure instanceof FileAlreadyExistsException) {
            reason = "already exists";
        } else if (failure instanceof AccessDeniedException) {
            reason = "permission denied";
        } else {
            reason = failure.getClass().getSimpleName();
        }
        return failure.getFile() + ": " + reason;
    }
}
