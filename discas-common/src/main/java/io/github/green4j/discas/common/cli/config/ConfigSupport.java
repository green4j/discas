/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli.config;

import io.github.green4j.discas.common.identity.NodeId;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Stateless helpers shared by the command-line configuration of every DisCas program
 * ({@code discas-node}, {@code discas-agent}, ...). Each program resolves its properties
 * from CLI flags and {@code DISCAS_*} environment variables (see {@link ConfigResolver}),
 * and derives the environment-variable name of a property mechanically from its long option
 * name ({@code UPPER_SNAKE}, {@code '-'} to {@code '_'}), so {@code --node-id} pairs with
 * {@code DISCAS_NODE_ID}, {@code --http-bind} with {@code DISCAS_HTTP_BIND}, and so on.
 */
public final class ConfigSupport {

    /** Environment-variable prefix shared by every DisCas program. */
    public static final String ENV_PREFIX = "DISCAS_";

    private ConfigSupport() {
    }

    /** The {@code DISCAS_*} environment-variable name derived from a property's long option name. */
    public static String envName(final String longName) {
        return ENV_PREFIX + longName.toUpperCase(Locale.ROOT).replace('-', '_');
    }

    /**
     * Option help text with the derived {@code (env DISCAS_*)} note appended, so every option
     * documents its environment variable uniformly and none can forget it.
     */
    public static String helpWithEnv(final String longName, final String text) {
        return text + " (env " + envName(longName) + ")";
    }

    /** True if {@code args} explicitly requests help ({@code -h} / {@code --help}). */
    public static boolean isHelpRequested(final String[] args) {
        if (args == null) {
            return false;
        }
        for (int i = 0; i < args.length; i++) {
            final String a = args[i];
            if ("-h".equals(a) || "--help".equals(a)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reports a command line this tool could not use, and exits {@code 2}.
     *
     * <p>The one shape every {@code discas} executable prints it in: what was wrong, a blank line,
     * the usage, and where to read more. Exit {@code 2} is "you asked wrongly", as against
     * {@code 1} for "it did not work" -- the distinction a script needs and the one all three
     * programs used to spell out separately, in three copies that were free to drift.
     *
     * @param program   the name an operator typed, e.g. {@code discas-node} or {@code discas-admin
     *                  dump}
     * @param message   what was wrong with the command line
     * @param usageText the program's usage, ready to print
     */
    public static void usageError(final String program,
                                  final String message,
                                  final String usageText) {
        System.err.println(program + ": " + message);
        System.err.println();
        System.err.print(usageText);
        System.err.println("Try '" + program + " --help' for more information.");
        System.exit(2);
    }

    /** Throws {@link IllegalArgumentException} naming the option and its env var when {@code value} is null. */
    public static void requirePresent(final String value, final String name) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "--" + name + " (" + envName(name) + ") is required when TLS is enabled");
        }
    }

    /** Parse a {@code host:port} into a resolved address; {@code what} names the field for errors. */
    public static InetSocketAddress parseAddress(final String hostPort, final String what) {
        return parseAddress(hostPort, what, false);
    }

    /**
     * Parse a {@code host:port} into a resolved (not created-unresolved) address: binds open real
     * server sockets and static members are dialed directly. {@code allowZeroPort} permits an
     * ephemeral (port {@code 0}) bind, useful for tests and dynamic port assignment.
     */
    public static InetSocketAddress parseAddress(final String hostPort, final String what,
                                                 final boolean allowZeroPort) {
        final int colon = hostPort.lastIndexOf(':');
        if (colon <= 0 || colon == hostPort.length() - 1) {
            throw new IllegalArgumentException("Bad " + what + " address '" + hostPort + "', expected host:port");
        }
        final String host = hostPort.substring(0, colon).trim();
        final int port = parsePort(hostPort.substring(colon + 1), what, allowZeroPort);
        return new InetSocketAddress(host, port);
    }

    private static int parsePort(final String s, final String what, final boolean allowZero) {
        final int port;
        try {
            port = Integer.parseInt(s.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Bad " + what + " port '" + s + "'");
        }
        final int min = allowZero ? 0 : 1;
        if (port < min || port > 65535) {
            throw new IllegalArgumentException(what + " port out of range: " + port);
        }
        return port;
    }

    /**
     * Parse {@code id=host:port,id2=host:port,...} into an ordered address view. {@code what}
     * names the field for errors ({@code "members"} for a node, {@code "nodes"} for the agent).
     */
    public static Map<NodeId, InetSocketAddress> parseMembers(final String spec, final String what) {
        final Map<NodeId, InetSocketAddress> out = new LinkedHashMap<>();
        for (final String entry : spec.split(",")) {
            final String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final int eq = trimmed.indexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw new IllegalArgumentException(
                        "Bad " + what + " entry '" + trimmed + "', expected id=host:port");
            }
            final NodeId id = NodeId.of(trimmed.substring(0, eq).trim());
            out.put(id, parseAddress(trimmed.substring(eq + 1).trim(), what));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("No " + what + " parsed from '" + spec + "'");
        }
        return out;
    }

    /**
     * Read and parse a java-properties members file ({@code node.<id> = host:port}, one member per
     * line) into an ordered address view. This is the same format {@code FileMembers} reads, so a
     * single file can drive both a node's membership and an agent's target-node list. {@code what}
     * names the field for errors ({@code "members"} for a node, {@code "nodes"} for the agent).
     * Throws {@link IllegalArgumentException} if the file cannot be read or parsed, or has no members.
     */
    public static Map<NodeId, InetSocketAddress> parseMembersFile(final Path file, final String what) {
        final byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Cannot read " + what + " file " + file + ": " + e.getMessage());
        }
        return parseMembersProperties(bytes, what, file);
    }

    /**
     * Parse the raw bytes of a members file (java properties, {@code node.<id> = host:port}) into an
     * ordered address view. Used by {@link #parseMembersFile} and by reloadable sources that already
     * hold the file bytes. {@code what} names the field for errors.
     */
    public static Map<NodeId, InetSocketAddress> parseMembersProperties(final byte[] bytes, final String what) {
        return parseMembersProperties(bytes, what, null);
    }

    private static Map<NodeId, InetSocketAddress> parseMembersProperties(
            final byte[] bytes, final String what, final Path file) {
        final String where = file != null ? (" " + file) : "";
        final Properties props = new Properties();
        try {
            props.load(new ByteArrayInputStream(bytes));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Cannot parse " + what + " file" + where + ": " + e.getMessage());
        }
        final Map<NodeId, InetSocketAddress> out = new LinkedHashMap<>();
        for (final String name : props.stringPropertyNames()) {
            if (!name.startsWith("node.")) {
                continue;
            }
            final NodeId id = NodeId.of(name.substring("node.".length()));
            out.put(id, parseAddress(props.getProperty(name).trim(), what));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("No node.* " + what + " in file" + where);
        }
        return out;
    }
}
