/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli.config;

import io.github.green4j.discas.common.cli.GetOpts;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Resolves a program's effective configuration from a <b>dual</b> source: command-line flags
 * (already parsed into a {@link GetOpts}) and {@code DISCAS_*} environment variables. Each
 * property is settable by <em>either</em> source; the per-property precedence is
 * {@code CLI > ENV > DEFAULT}, decided independently for every value and recorded so it can be
 * surfaced to an operator.
 * <p>
 * The environment map is passed in (rather than read from {@link System#getenv()}) so callers
 * are fully unit-testable. Each typed getter records a row; {@link #describe()} renders every
 * effective value with the {@link ConfigSource} it came from, masking security-sensitive values.
 */
public final class ConfigResolver {

    private final String programName;
    private final GetOpts opts;
    private final Map<String, String> env;
    private final List<Row> rows = new ArrayList<>();

    public ConfigResolver(final String programName, final GetOpts opts, final Map<String, String> env) {
        this.programName = programName;
        this.opts = opts;
        this.env = env == null ? Map.of() : env;
    }

    public String optional(final String name, final String def) {
        final String value;
        final ConfigSource source;
        if (opts.isPresent(name)) {
            value = opts.getString(name);
            source = ConfigSource.CLI;
        } else {
            final String e = env.get(ConfigSupport.envName(name));
            if (e != null) {
                value = e;
                source = ConfigSource.ENV;
            } else {
                value = def;
                source = ConfigSource.DEFAULT;
            }
        }
        rows.add(new Row(name, value == null ? "<unset>" : value, source));
        return value;
    }

    /**
     * As {@link #optional(String, String)} for an option that has no default at all: {@code null}
     * when neither the command line nor the environment set it.
     * <p>
     * Spelled separately because {@code optional(name, null)} was the commonest call in the repo,
     * and a literal {@code null} in an argument list says nothing about whether it is a default
     * somebody chose or the absence of one.
     */
    public String optional(final String name) {
        return optional(name, null);
    }

    public String required(final String name) {
        final String value = optional(name);
        if (value == null) {
            throw new IllegalArgumentException(
                    "--" + name + " (" + ConfigSupport.envName(name) + ") is required");
        }
        return value;
    }

    public String secret(final String name) {
        final String value;
        final ConfigSource source;
        if (opts.isPresent(name)) {
            value = opts.getString(name);
            source = ConfigSource.CLI;
        } else {
            final String e = env.get(ConfigSupport.envName(name));
            if (e != null) {
                value = e;
                source = ConfigSource.ENV;
            } else {
                value = null;
                source = ConfigSource.DEFAULT;
            }
        }
        rows.add(new Row(name, value == null ? "<unset>" : "****", source));
        return value;
    }

    public int integer(final String name, final int def) {
        final int value;
        final ConfigSource source;
        if (opts.isPresent(name)) {
            value = parseInt(name, opts.getString(name));
            source = ConfigSource.CLI;
        } else {
            final String e = env.get(ConfigSupport.envName(name));
            if (e != null) {
                value = parseInt(name, e);
                source = ConfigSource.ENV;
            } else {
                value = def;
                source = ConfigSource.DEFAULT;
            }
        }
        rows.add(new Row(name, Integer.toString(value), source));
        return value;
    }

    /**
     * Resolve an integer option and reject anything below {@code min}.
     * <p>
     * Validating here rather than at the consumer keeps the check ahead of {@link #print}: an
     * option that will blow up later -- in a transport config or a storage builder -- must not
     * first be reported as the effective value.
     */
    public int integerAtLeast(final String name, final int def, final int min) {
        final int value = integer(name, def);
        if (value < min) {
            throw new IllegalArgumentException(
                    "--" + name + " must be >= " + min + ", got " + value);
        }
        return value;
    }

    /** Resolve an integer option and reject anything outside {@code [min, max]}. */
    public int integerInRange(final String name, final int def, final int min, final int max) {
        final int value = integer(name, def);
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "--" + name + " must be in [" + min + ", " + max + "], got " + value);
        }
        return value;
    }

    public long longValue(final String name, final long def) {
        final long value;
        final ConfigSource source;
        if (opts.isPresent(name)) {
            value = parseLong(name, opts.getString(name));
            source = ConfigSource.CLI;
        } else {
            final String e = env.get(ConfigSupport.envName(name));
            if (e != null) {
                value = parseLong(name, e);
                source = ConfigSource.ENV;
            } else {
                value = def;
                source = ConfigSource.DEFAULT;
            }
        }
        rows.add(new Row(name, Long.toString(value), source));
        return value;
    }

    /** Resolve a long option and reject anything below {@code min}. */
    public long longAtLeast(final String name, final long def, final long min) {
        final long value = longValue(name, def);
        if (value < min) {
            throw new IllegalArgumentException(
                    "--" + name + " must be >= " + min + ", got " + value);
        }
        return value;
    }

    public boolean bool(final String name, final boolean def) {
        final boolean value;
        final ConfigSource source;
        if (opts.isPresent(name)) {
            value = parseBool(name, opts.getString(name));
            source = ConfigSource.CLI;
        } else {
            final String e = env.get(ConfigSupport.envName(name));
            if (e != null) {
                value = parseBool(name, e);
                source = ConfigSource.ENV;
            } else {
                value = def;
                source = ConfigSource.DEFAULT;
            }
        }
        rows.add(new Row(name, Boolean.toString(value), source));
        return value;
    }

    /**
     * A {@code PROPERTY | VALUE | SOURCE} table of every effective value and where it came
     * from. Security-sensitive values are masked.
     */
    public String describe() {
        int nameW = "PROPERTY".length();
        int valueW = "VALUE".length();
        for (int i = 0; i < rows.size(); i++) {
            final Row row = rows.get(i);
            nameW = Math.max(nameW, row.name.length());
            valueW = Math.max(valueW, row.value.length());
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(programName).append(" effective configuration:").append(System.lineSeparator());
        appendRow(sb, "PROPERTY", nameW, "VALUE", valueW, "SOURCE");
        for (int i = 0; i < rows.size(); i++) {
            final Row row = rows.get(i);
            appendRow(sb, row.name, nameW, row.value, valueW, row.source.name());
        }
        return sb.toString();
    }

    public void print(final PrintStream out) {
        out.print(describe());
    }

    /** The resolved source for a property's long name; {@code null} if it was not resolved. */
    public ConfigSource sourceOf(final String longName) {
        for (int i = 0; i < rows.size(); i++) {
            final Row row = rows.get(i);
            if (row.name.equals(longName)) {
                return row.source;
            }
        }
        return null;
    }

    /** The display (masked for secrets) value recorded for a property; {@code null} if absent. */
    public String displayValueOf(final String longName) {
        for (int i = 0; i < rows.size(); i++) {
            final Row row = rows.get(i);
            if (row.name.equals(longName)) {
                return row.value;
            }
        }
        return null;
    }

    private static void appendRow(final StringBuilder sb, final String name, final int nameW,
                                  final String value, final int valueW, final String source) {
        sb.append("  ")
                .append(pad(name, nameW)).append("  ")
                .append(pad(value, valueW)).append("  ")
                .append(source)
                .append(System.lineSeparator());
    }

    private static String pad(final String s, final int width) {
        if (s.length() >= width) {
            return s;
        }
        final StringBuilder sb = new StringBuilder(s);
        while (sb.length() < width) {
            sb.append(' ');
        }
        return sb.toString();
    }

    private static int parseInt(final String name, final String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " expects an integer, got '" + raw + "'");
        }
    }

    private static long parseLong(final String name, final String raw) {
        try {
            return Long.parseLong(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " expects an integer, got '" + raw + "'");
        }
    }

    /**
     * Parse a boolean, rejecting anything that is not {@code true} or {@code false}.
     * <p>
     * The accepted set is exactly what {@code GetOpts} enforces for these options on the
     * command line, so CLI and environment agree. Silently mapping every unrecognised value
     * to {@code false} -- as this once did -- meant {@code DISCAS_TLS=ture} started the node
     * in plaintext with no diagnostic, while a malformed numeric option threw.
     */
    private static boolean parseBool(final String name, final String raw) {
        final String v = raw.trim().toLowerCase(Locale.ROOT);
        if (v.equals("true")) {
            return true;
        }
        if (v.equals("false")) {
            return false;
        }
        throw new IllegalArgumentException(
                "--" + name + " expects 'true' or 'false', got '" + raw + "'");
    }

    private static final class Row {
        final String name;
        final String value;
        final ConfigSource source;

        Row(final String name, final String value, final ConfigSource source) {
            this.name = name;
            this.value = value;
            this.source = source;
        }
    }
}
