/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigResolver;
import io.github.green4j.discas.common.cli.config.ConfigSupport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * What every {@code discas-admin} command does the same way, so that no command has to decide it
 * again: show its help, parse its own options, print lines under its own name, keep one progress
 * line, and leave nothing behind when it fails.
 *
 * <h2>Adding a command</h2>
 * Extend this, implement {@link #options()} and {@link #run(GetOpts, ConfigResolver, ProgressLine)},
 * and add one line to {@code DisCasAdminStarter}'s registry. That is the whole of it -- there is no
 * annotation to remember, nothing is scanned for, and the list of commands is a list you can read.
 * A command that is not in that list does not exist, which is the property worth having.
 *
 * <h2>What is not shared</h2>
 * The <b>options</b> are each command's own. A dump needs a cluster, prefixes and a file to write;
 * a restore needs a file to read and a directory to create, and does not connect to a cluster at
 * all. Fragments that genuinely repeat live in {@link ClusterOptions}, which a command opts into.
 */
abstract class AbstractCommand implements Command {

    private final String name;
    private final String summary;

    /** {@code discas-admin <name>} -- what an operator sees in every line this command prints. */
    protected final String program;

    /**
     * Set by {@link #removeOnFailure}: what this command creates and must take back if it does not
     * finish. One process runs one command once, so a field is the whole of the bookkeeping.
     */
    private Path createdPath;

    /**
     * Whether {@link #createdPath} was already there when the command registered it -- in which case
     * the command did not create it and must not remove it.
     * <p>
     * Learned the hard way: {@code dump} refuses to overwrite an existing file, and the cleanup then
     * deleted that very file when the refusal threw. A tool whose failure path destroys the backup
     * its success path declined to touch is worse than one with no cleanup at all.
     */
    private boolean createdPathExistedBefore;

    protected AbstractCommand(final String name, final String summary) {
        this.name = name;
        this.summary = summary;
        this.program = DisCasAdminStarter.PROGRAM + " " + name;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final String summary() {
        return summary;
    }

    /**
     * This command's option schema. Built fresh on every call: {@link GetOpts} parses into itself,
     * so the instance that printed help is not the instance that reads a command line.
     */
    protected abstract GetOpts options();

    /** This command's usage, for the launcher to print when a command line does not parse. */
    @Override
    public final String usageText() {
        return options().usageString();
    }

    /**
     * The work, with the command line already parsed.
     *
     * @param opts     for positionals and presence-based flags
     * @param config   for values, which come from the command line or the {@code DISCAS_*}
     *                 environment
     * @param progress the one line this command redraws; already abandoned for you on a failure
     */
    protected abstract void run(GetOpts opts,
                                ConfigResolver config,
                                ProgressLine progress) throws Exception;

    @Override
    public final void run(final String[] args) throws Exception {
        // Only an explicit -h/--help prints help, the way discas-node and discas-agent do it, and
        // for their reason: every option can come from a DISCAS_* environment variable, so a bare
        // command line is a complete invocation rather than a request for help. Printing help here
        // meant `DISCAS_NODES=... DISCAS_OUT=... discas-admin dump` showed the help text instead of
        // taking a backup.
        if (ConfigSupport.isHelpRequested(args)) {
            System.out.print(options().helpString());
            return;
        }
        final GetOpts opts = options();
        opts.parseOrThrow(args);
        final ConfigResolver config = new ConfigResolver(program, opts, System.getenv());
        final ProgressLine progress = new ProgressLine(System.out);
        try {
            run(opts, config, progress);
        } catch (final Exception failed) {
            // One rule for every command: what a failed run leaves behind must not look like a
            // result. A file or a folder that is only half a backup is worse than none, because
            // somebody finds it later and believes it.
            progress.abandon();
            if (!createdPathExistedBefore) {
                removeQuietly(createdPath);
            }
            throw failed;
        }
    }

    /**
     * Names the file or folder this command is about to create, so a failure takes it back --
     * unless it was already there, which makes it somebody else's and not this run's to remove.
     */
    protected final void removeOnFailure(final Path path) {
        this.createdPath = path;
        this.createdPathExistedBefore = Files.exists(path);
    }

    /** A line of this command's own output, under its own name. */
    protected final void say(final String message) {
        System.out.println(program + ": " + message);
    }

    /** Redraws the progress line, throttled by {@link ProgressLine}. Safe to call per key. */
    protected final void progress(final ProgressLine line, final String message) {
        line.update(program + ": " + message);
    }

    /** The last line: the progress line ends here, saying what happened. */
    protected final void finish(final ProgressLine line, final String message) {
        line.finish(program + ": " + message);
    }

    /** Positional arguments as key prefixes. Keys are bytes; a command line is UTF-8. */
    protected static List<ByteBuffer> utf8(final List<String> text) {
        final List<ByteBuffer> encoded = new ArrayList<>(text.size());
        for (int i = 0; i < text.size(); i++) {
            encoded.add(ByteBuffer.wrap(text.get(i).getBytes(StandardCharsets.UTF_8)));
        }
        return encoded;
    }

    private static void removeQuietly(final Path path) {
        if (path == null) {
            return;
        }
        try {
            if (Files.isDirectory(path)) {
                deleteTree(path);
            } else {
                Files.deleteIfExists(path);
            }
        } catch (final IOException ignored) {
            // Already reporting a failure; a second one about the cleanup would bury the first.
        }
    }

    private static void deleteTree(final Path root) throws IOException {
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path directory, final IOException failed)
                    throws IOException {
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
