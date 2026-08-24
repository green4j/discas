/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@code GetOpts} - a small, dependency-free command-line option parser inspired
 * by the C {@code getopt}/{@code getopt_long} libraries.
 *
 * <h2>Features</h2>
 * <ul>
 *   <li>Short options ({@code -v}) and long options ({@code --verbose}).</li>
 *   <li>Short option clustering ({@code -vz} == {@code -v -z}).</li>
 *   <li>Typed values: {@code String}, {@code int}, {@code long}, {@code double}.</li>
 *   <li>Presence-based boolean options ({@link Type#FLAG} and {@link Type#BOOLEAN}):
 *       the option is {@code true} when it appears on the command line and
 *       consumes <em>no</em> value.</li>
 *   <li>Default values and a {@code required} constraint.</li>
 *   <li>Positional (non-option) argument capture, and {@code --} end-of-options
 *       marker.</li>
 *   <li>Unambiguous long-option prefix matching ({@code --verb} -> {@code --verbose}).</li>
 *   <li>Automatic, word-wrapped {@code --help} and usage generation.</li>
 * </ul>
 *
 * <h2>Recommended pattern: a process-wide {@code static final} instance</h2>
 * Command-line options describe the configuration of the whole process, so the
 * option schema is naturally a singleton. Declare a {@code static final}
 * {@code GetOpts} in your main class, build the schema once in a static
 * initializer (or inline), and expose typed accessors. This keeps the option
 * definitions and their retrieval in one place and avoids passing the parser
 * around.
 *
 * <pre>{@code
 * public final class BackupTool {
 *
 *     // 1. Declare the schema once, process-wide.
 *     private static final GetOpts OPTS = new GetOpts(
 *             "backup-tool",
 *             "Backs up files from a source directory to a destination, "
 *                     + "with optional compression and verification.")
 *             // Presence-based flags (Type.FLAG): true when present, no value.
 *             .flag("verbose", 'v', "Enable verbose output.")
 *             .flag("dry-run", 'n', "Show what would be done without doing it.")
 *             // Presence-based BOOLEAN: true when present; may carry a default.
 *             .boolOpt("compress", 'z', false, "Compress the archive.")
 *             // Typed value options: a default makes it optional; .required() means no default
 *             // (a no-default overload omits the default argument entirely).
 *             .stringOpt("source", 's', ".", "Source directory.")
 *             .stringOpt("dest",   'd', "Destination.").required()
 *             .intOpt("threads",   't', 4,   "Worker threads.")
 *             .longOpt("max-size", 'm', null, "Max archive size (bytes).")
 *             .doubleOpt("ratio",  'r', 0.75, "Target compression ratio.")
 *             .epilogue("Example: backup-tool -vz -d /backup file1 file2")
 *             .width(80);
 *
 *     private BackupTool() {
 *     }
 *
 *     public static void main(final String[] args) {
 *         // 2. Parse. Prints help and exits(0) on -h/--help (or on no args
 *         //    when helpWhenEmpty is true); prints usage and exits(2) on error.
 *         OPTS.parse(args, true);
 *
 *         // 3. Read typed values anywhere via the static instance.
 *         if (OPTS.getBool("verbose")) {
 *             System.out.println("verbose mode");
 *         }
 *         final String dest = OPTS.getString("dest");
 *         final int threads = OPTS.getInt("threads");
 *         final List<String> files = OPTS.positionals();
 *         // ... run ...
 *     }
 * }
 * }</pre>
 *
 * <h2>Defining options</h2>
 * All builder methods return {@code this} so definitions can be chained. Each
 * option must have at least a long name or a short name (either may be
 * {@code null}, but not both).
 *
 * <table border="1">
 *   <caption>Builder methods</caption>
 *   <tr><th>Method</th><th>Type</th><th>Takes a value?</th><th>Notes</th></tr>
 *   <tr><td>{@link #flag(String, Character, String)}</td>
 *       <td>{@link Type#FLAG}</td><td>no</td>
 *       <td>Default {@code false}; never required.</td></tr>
 *   <tr><td>{@link #booleanOpt(String, Character, Boolean, String)}</td>
 *       <td>{@link Type#BOOLEAN}</td><td>no</td>
 *       <td>Presence-based; carries a default; never required.</td></tr>
 *   <tr><td>{@link #stringOpt(String, Character, String, String)}</td>
 *       <td>{@link Type#STRING}</td><td>yes</td><td></td></tr>
 *   <tr><td>{@link #intOpt(String, Character, Integer, String)}</td>
 *       <td>{@link Type#INT}</td><td>yes</td><td></td></tr>
 *   <tr><td>{@link #longOpt(String, Character, Long, String)}</td>
 *       <td>{@link Type#LONG}</td><td>yes</td><td></td></tr>
 *   <tr><td>{@link #doubleOpt(String, Character, Double, String)}</td>
 *       <td>{@link Type#DOUBLE}</td><td>yes</td><td></td></tr>
 * </table>
 *
 * <p>A value option with a default is optional; call {@link #required()} (which forbids a
 * default) to require it instead. An option is thus <b>required XOR has a default</b>. Each value
 * builder also has a <b>no-default overload</b> that omits the default argument -- use it for a
 * required option or one that is simply defaultless (e.g. {@code stringOpt(long, short, help)}).
 *
 * <p>A built-in {@code -h}/{@code --help} flag is added automatically by the
 * constructor.
 *
 * <h2>Refining an option, and grouping</h2>
 * These fluent post-modifiers refine the <em>most recently added</em> option and may be
 * chained onto any builder call; help is rendered in the git/docker convention (lowercase
 * {@code <name>} placeholders, space-separated, aligned columns):
 * <ul>
 *   <li>{@link #metavar(String)} -- override the value placeholder (default {@code <long-name>}),
 *       e.g. {@code .stringOpt("template", 't', ...).metavar("<file>")} -> {@code -t, --template <file>}.</li>
 *   <li>{@link #choices(String...)} -- restrict the value to a set; it is listed in the help as
 *       {@code (one of: ...)} and enforced at parse time.</li>
 *   <li>{@link #optionalArg(String)} -- make the value optional ({@code -u, --untracked[=<mode>]}):
 *       the bare flag yields the implied value; such options are <b>never required</b> and never
 *       consume the following argument.</li>
 *   <li>{@link #required()} -- require the option (parsing fails if absent). A required option
 *       must be a value option with <b>no default</b>: it is mutually exclusive with a default
 *       value and with {@link #optionalArg(String)}.</li>
 * </ul>
 * {@link #group(String)} starts a named section; options registered after it are listed under
 * that header. Options added before any {@code group} form the default root {@code "Options"}
 * section. The usage line stays flat regardless of grouping.
 *
 * <h2>Command-line syntax accepted</h2>
 * <table border="1">
 *   <caption>Accepted forms</caption>
 *   <tr><th>Form</th><th>Meaning</th></tr>
 *   <tr><td>{@code -v}</td><td>Short flag / bool (true when present).</td></tr>
 *   <tr><td>{@code -vz}</td><td>Cluster of short flags/bools.</td></tr>
 *   <tr><td>{@code -t 8}</td><td>Short value option, value in next argument.</td></tr>
 *   <tr><td>{@code -t8}</td><td>Short value option, value attached.</td></tr>
 *   <tr><td>{@code -vzt8}</td><td>Cluster ending in a value option.</td></tr>
 *   <tr><td>{@code --verbose}</td><td>Long flag / bool.</td></tr>
 *   <tr><td>{@code --dest /backup}</td><td>Long value option, next argument.</td></tr>
 *   <tr><td>{@code --dest=/backup}</td><td>Long value option, inline value.</td></tr>
 *   <tr><td>{@code --verb}</td><td>Unambiguous prefix of {@code --verbose}.</td></tr>
 *   <tr><td>{@code --}</td><td>End of options; everything after is positional.</td></tr>
 *   <tr><td>{@code -}</td><td>A single dash is treated as a positional argument.</td></tr>
 * </table>
 *
 * <p><b>Presence-based booleans do not accept a value.</b> Passing
 * {@code --compress=true} raises a {@link ParseException} ("doesn't allow an
 * argument"). If a boolean option has a default of {@code true}, it cannot be
 * turned off from the command line with the same flag; model an "off" switch as
 * a separate {@link #flag(String, Character, String) flag} (for example
 * {@code --no-verify}) and negate it in code:
 * <pre>{@code
 * // verify defaults ON; --no-verify turns it off
 * OPTS.flag("no-verify", null, "Disable integrity verification.");
 * boolean verify = !OPTS.getBool("no-verify");
 * }</pre>
 *
 * <h2>Parsing and exit behavior</h2>
 * <ul>
 *   <li>{@link #parse(String[])} -- on error prints the message and usage to
 *       {@code stderr} and calls {@code System.exit(2)}; on {@code -h}/{@code --help}
 *       prints the full help to {@code stdout} and calls {@code System.exit(0)}.</li>
 *   <li>{@link #parse(String[], boolean)} -- same, and additionally shows help and
 *       exits(0) when invoked with no arguments (if {@code helpWhenEmpty} is
 *       {@code true}) -- the conventional CLI behavior.</li>
 *   <li>{@link #parseOrThrow(String[])} -- never exits; throws
 *       {@link ParseException} on error. Use this in tests or when you want full
 *       control. Check {@link #helpRequested()} afterwards.</li>
 * </ul>
 *
 * <pre>{@code
 * // Manual control (no System.exit), e.g. in a unit test:
 * try {
 *     OPTS.parseOrThrow(argv);
 * } catch (GetOpts.ParseException e) {
 *     System.err.println(e.getMessage());
 *     OPTS.printUsage();
 *     return;
 * }
 * if (OPTS.helpRequested()) {
 *     OPTS.printHelp();
 *     return;
 * }
 * }</pre>
 *
 * <h2>Reading parsed values</h2>
 * Use the option's <em>long name</em> if it has one, otherwise its short name as
 * a one-character string. The accessors coerce {@code null} defaults to
 * type-appropriate zero/empty values, except {@link #getString(String)} which
 * returns {@code null}.
 *
 * <table border="1">
 *   <caption>Accessors</caption>
 *   <tr><th>Accessor</th><th>Returns</th><th>When option absent</th></tr>
 *   <tr><td>{@link #getBool(String)}</td><td>{@code boolean}</td>
 *       <td>the default ({@code false} for flags).</td></tr>
 *   <tr><td>{@link #getString(String)}</td><td>{@code String}</td>
 *       <td>the default, possibly {@code null}.</td></tr>
 *   <tr><td>{@link #getInt(String)}</td><td>{@code int}</td>
 *       <td>the default, or {@code 0} if {@code null}.</td></tr>
 *   <tr><td>{@link #getLong(String)}</td><td>{@code long}</td>
 *       <td>the default, or {@code 0L} if {@code null}.</td></tr>
 *   <tr><td>{@link #getDouble(String)}</td><td>{@code double}</td>
 *       <td>the default, or {@code 0.0} if {@code null}.</td></tr>
 *   <tr><td>{@link #isPresent(String)}</td><td>{@code boolean}</td>
 *       <td>{@code false} (i.e. tells you whether it was on the command line).</td></tr>
 *   <tr><td>{@link #positionals()}</td><td>{@code List<String>}</td>
 *       <td>the collected non-option arguments.</td></tr>
 * </table>
 *
 * <p>{@link #isPresent(String)} is distinct from {@link #getBool(String)}: use
 * {@code isPresent} to detect whether a value option was explicitly supplied
 * (versus falling back to its default), for example:
 * <pre>{@code
 * if (OPTS.isPresent("max-size")) {
 *     long cap = OPTS.getLong("max-size"); // user set an explicit cap
 * }
 * }</pre>
 *
 * <h2>Worked example: inputs and results</h2>
 * Using the {@code BackupTool} schema above:
 * <pre>{@code
 * $ backup-tool -vz -s /data -d /backup -t 8 report.txt notes.md
 *   verbose  = true            // -v
 *   dry-run  = false
 *   compress = true            // -z present => true (no value consumed)
 *   source   = /data           // -s /data
 *   dest     = /backup         // -d /backup
 *   threads  = 8               // -t 8
 *   ratio    = 0.75            // default
 *   positionals = [report.txt, notes.md]
 *
 * $ backup-tool --verbose --compress --dest=/backup
 *   verbose = true, compress = true, dest = /backup
 *
 * $ backup-tool -v                       // missing required --dest
 *   (stderr) backup-tool: The following required option(s) are missing: --dest
 *   (stderr) Usage: backup-tool [-h] [-v] [-n] [-z] [-s <source>] -d <dest> ...
 *   (stderr) Try 'backup-tool --help' for more information.
 *   (exit 2)
 *
 * $ backup-tool --compress=true --dest /backup   // BOOLEAN rejects a value
 *   (stderr) backup-tool: option '--compress' doesn't allow an argument
 *   (exit 2)
 *
 * $ backup-tool                          // no args, parse(args, true)
 *   (stdout) full help    (exit 0)
 *
 * $ backup-tool --help                   // or -h, or -vh in a cluster
 *   (stdout) full help    (exit 0)
 * }</pre>
 *
 * <h2>Customizing help output</h2>
 * <ul>
 *   <li>{@link #width(int)} -- set the wrap width (min 40, default 80).</li>
 *   <li>{@link #epilogue(String)} -- trailing text printed after the options table.</li>
 *   <li>{@link #usage(String)} -- override the generated {@code Usage:} line.</li>
 *   <li>{@link #printHelp()} / {@link #printUsage()} -- print to {@code stdout};
 *       {@link #helpString()} / {@link #usageString()} return the text instead.</li>
 * </ul>
 *
 * <h2>Thread-safety</h2>
 * The schema (builder methods) and {@link #parse(String[]) parsing} are intended
 * to be performed once, at startup, from the main thread before any worker
 * threads read values. After {@code parse} completes, the typed accessors are
 * read-only and safe to call from multiple threads.
 */
public final class GetOpts {
    public static final String DOUBLE_DASH = "--";
    public static final String DASH = "-";
    public static final String DOUBLE_WHITESPACE = "  ";
    public static final String COMMA_SEPARATION = ", ";
    public static final String EMPTY_STRING = "";
    public static final String LINE_BREAK = "\n";
    public static final char WHITESPACE_CHAR = ' ';
    public static final String WHITESPACE = " ";

    public enum Type { STRING, INT, LONG, DOUBLE, BOOLEAN, FLAG }

    private static final class Option {
        private final String longName;       // may be null
        private final Character shortName;   // may be null
        private final Type type;
        private final Object defaultVal;     // may be null
        private boolean required;            // optionalArg forces this false
        private final String help;

        // Tunable via the fluent post-modifiers (metavar/choices/optionalArg) and group().
        private String metaVar;              // null => derived <name>; set via metavar()
        private List<String> choices;        // null => unconstrained; set via choices()
        private boolean optionalArg;         // value may be omitted (never required)
        private Object impliedVal;           // value used when an optional-arg flag appears bare
        private String group;                // section header this option belongs to; null => root

        private Object value;                // parsed value
        private boolean present;             // whether it appeared on command line

        private Option(final String longName,
                       final Character shortName,
                       final Type type,
                       final Object defaultVal,
                       final String help) {
            this.longName = longName;
            this.shortName = shortName;
            this.type = type;
            this.defaultVal = defaultVal;
            this.required = false; // set via required()
            this.help = help == null ? EMPTY_STRING : help;
            this.value = defaultVal;
        }

        private boolean takesArg() {
            // FLAG and BOOLEAN are presence-based: true when present, no value consumed
            return type != Type.FLAG && type != Type.BOOLEAN;
        }

        /**
         * Placeholder shown for the argument value: a custom metavar, else {@code <long-name>}.
         */
        private String metaVarOrDefault() {
            if (metaVar != null) {
                return metaVar;
            }
            if (longName != null) {
                return "<" + longName + ">";
            }
            return "<value>";
        }

        private String primaryKey() {
            if (longName != null) {
                return longName;
            }
            return shortName != null ? String.valueOf(shortName) : "?";
        }
    }

    /**
     * Thrown when parsing fails. Carries a user-friendly message.
     */
    public static final class ParseException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public ParseException(final String msg) {
            super(msg);
        }
    }

    private final String programName;
    private final String description;
    private String epilogue = null;
    private String usageOverride = null;

    private final Map<String, Option> byLong = new LinkedHashMap<>();
    private final Map<Character, Option> byShort = new LinkedHashMap<>();
    private final List<Option> ordered = new ArrayList<>();
    private final List<String> positionals = new ArrayList<>();

    /**
     * Header for the default (root) group -- options added before any {@link #group}.
     */
    private static final String ROOT_GROUP = "Options";

    private String currentGroup = null; // null => root group
    private Option last = null;         // most recently added option (target of fluent modifiers)

    private int helpWidth = 80;

    public GetOpts(final String programName, final String description) {
        this.programName = programName == null ? "program" : programName;
        this.description = description == null ? EMPTY_STRING : description;
        // Built-in help option.
        flag("help", 'h', "Show this help message and exit.");
    }

    /**
     * String value option with no default (optional, or required via {@link #required()}).
     */
    public GetOpts stringOpt(final String longName, final Character shortName, final String help) {
        return stringOpt(longName, shortName, null, help);
    }

    public GetOpts stringOpt(final String longName, final Character shortName, final String defValue,
            final String help) {
        return add(new Option(longName, shortName, Type.STRING, defValue, help));
    }

    /**
     * Integer value option with no default (optional, or required via {@link #required()}).
     */
    public GetOpts intOpt(final String longName, final Character shortName, final String help) {
        return intOpt(longName, shortName, null, help);
    }

    public GetOpts intOpt(final String longName, final Character shortName, final Integer defValue, final String help) {
        return add(new Option(longName, shortName, Type.INT, defValue, help));
    }

    /**
     * Long value option with no default (optional, or required via {@link #required()}).
     */
    public GetOpts longOpt(final String longName, final Character shortName, final String help) {
        return longOpt(longName, shortName, null, help);
    }

    public GetOpts longOpt(final String longName, final Character shortName, final Long defValue, final String help) {
        return add(new Option(longName, shortName, Type.LONG, defValue, help));
    }

    /**
     * Double value option with no default (optional, or required via {@link #required()}).
     */
    public GetOpts doubleOpt(final String longName, final Character shortName, final String help) {
        return doubleOpt(longName, shortName, null, help);
    }

    public GetOpts doubleOpt(final String longName, final Character shortName, final Double defValue,
            final String help) {
        return add(new Option(longName, shortName, Type.DOUBLE, defValue, help));
    }

    /**
     * Presence-based boolean option defaulting to {@code false}.
     */
    public GetOpts booleanOpt(final String longName, final Character shortName, final String help) {
        return booleanOpt(longName, shortName, Boolean.FALSE, help);
    }

    public GetOpts booleanOpt(final String longName, final Character shortName, final Boolean defValue,
            final String help) {
        return add(new Option(longName, shortName, Type.BOOLEAN, defValue == null ? Boolean.FALSE : defValue, help));
    }

    public GetOpts flag(final String longName, final Character shortName, final String help) {
        return add(new Option(longName, shortName, Type.FLAG, Boolean.FALSE, help));
    }

    /**
     * Start a named group. Every option registered after this call (until the next
     * {@code group}) is listed under this section header in the help. Options added before
     * any {@code group} belong to the default (root) {@code "Options"} section.
     */
    public GetOpts group(final String name) {
        this.currentGroup = name;
        return this;
    }

    /**
     * Override the placeholder shown for the most recently added option's value, e.g.
     * {@code .stringOpt("template", 't', null, "...").metavar("<file>")}. Angle-bracket
     * form is conventional. Without this, the placeholder defaults to {@code <long-name>}.
     */
    public GetOpts metavar(final String name) {
        requireLast("metavar").metaVar = name;
        return this;
    }

    /**
     * Restrict the most recently added (value) option to a fixed set of allowed values. The
     * set is shown in the help as {@code (one of: ...)} and enforced at parse time; the option's
     * default (and any optional-arg implied value), if non-null, must be a member.
     */
    public GetOpts choices(final String... allowed) {
        final Option o = requireLast("choices");
        o.choices = List.of(allowed);
        requireChoiceMember(o, o.defaultVal, "default");
        return this;
    }

    /**
     * Make the most recently added option's value <em>optional</em>: passing the flag bare
     * (e.g. {@code --sign}) yields {@code impliedWhenBare}; passing {@code --sign=<value>} (or
     * an attached short {@code -s<value>}) yields that value. An optional-value option is
     * <b>never required</b>. The value is never taken from the following argument.
     */
    public GetOpts optionalArg(final String impliedWhenBare) {
        final Option o = requireLast("optionalArg");
        if (o.required) {
            throw new IllegalStateException("Option '" + o.primaryKey()
                    + "' cannot be both required and optional-value");
        }
        o.optionalArg = true;
        o.impliedVal = impliedWhenBare;
        requireChoiceMember(o, impliedWhenBare, "optional-arg implied");
        return this;
    }

    /**
     * Mark the most recently added option as required: parsing fails if it is absent. A required
     * option is a value option with <b>no default</b> -- it is mutually exclusive with a default
     * value and with {@link #optionalArg(String)} (presence-based flags are never required).
     */
    public GetOpts required() {
        final Option o = requireLast("required");
        if (!o.takesArg()) {
            throw new IllegalStateException("Presence-based option '" + o.primaryKey() + "' cannot be required");
        }
        if (o.optionalArg) {
            throw new IllegalStateException("Option '" + o.primaryKey()
                    + "' cannot be both optional-value and required");
        }
        if (o.defaultVal != null) {
            throw new IllegalStateException("Required option '" + o.primaryKey() + "' must not have a default value");
        }
        o.required = true;
        return this;
    }

    private Option requireLast(final String modifier) {
        if (last == null) {
            throw new IllegalStateException(modifier + "() must follow an option definition");
        }
        return last;
    }

    private static void requireChoiceMember(final Option o, final Object value, final String what) {
        if (value != null && o.choices != null && !o.choices.contains(value.toString())) {
            throw new IllegalArgumentException("Option '" + o.primaryKey() + "' " + what + " value '" + value
                    + "' is not one of the choices " + o.choices);
        }
    }

    public GetOpts epilogue(final String text) {
        this.epilogue = text;
        return this;
    }

    public GetOpts usage(final String line) {
        this.usageOverride = line;
        return this;
    }

    public GetOpts width(final int cols) {
        this.helpWidth = Math.max(40, cols);
        return this;
    }

    private GetOpts add(final Option o) {
        if (o.longName == null && o.shortName == null) {
            throw new IllegalArgumentException("Option needs a long or short name");
        }
        if (o.longName != null) {
            if (byLong.containsKey(o.longName)) {
                throw new IllegalArgumentException("Duplicate long option: --" + o.longName);
            }
            byLong.put(o.longName, o);
        }
        if (o.shortName != null) {
            if (byShort.containsKey(o.shortName)) {
                throw new IllegalArgumentException("Duplicate short option: -" + o.shortName);
            }
            byShort.put(o.shortName, o);
        }
        o.group = currentGroup;
        ordered.add(o);
        last = o;
        return this;
    }

    /**
     * Parse the given arguments. On error, prints a message and usage to
     * stderr and exits (status 2). If --help/-h is supplied, prints help to
     * stdout and exits (status 0).
     */
    public void parse(final String[] args) {
        parse(args, false);
    }

    /**
     * Parse the given arguments.
     * <p>
     * Behavior:
     * <ul>
     *   <li>On parse error: prints error + usage to stderr, exits(2).</li>
     *   <li>On --help/-h: prints full help to stdout, exits(0).</li>
     *   <li>If {@code helpWhenEmpty} is true and no args were given:
     *       prints full help to stdout, exits(0).</li>
     * </ul>
     *
     * @param args          the raw CLI arguments
     * @param helpWhenEmpty show help and exit(0) when args is empty
     */
    public void parse(final String[] args, final boolean helpWhenEmpty) {
        if (helpWhenEmpty && (args == null || args.length == 0)) {
            System.out.print(helpString());
            System.exit(0);
        }
        try {
            parseOrThrow(args);
        } catch (final ParseException e) {
            System.err.println(programName + ": " + e.getMessage());
            System.err.println();
            System.err.print(usageString());
            System.err.println("Try '" + programName + " --help' for more information.");
            System.exit(2);
        }
        if (helpRequested()) {
            System.out.print(helpString());
            System.exit(0);
        }
    }

    /**
     * Parse without exiting. Throws {@link ParseException} on error.
     * Does not automatically print help; caller may inspect
     * {@link #helpRequested()}.
     */
    public void parseOrThrow(final String[] args) {
        int i = 0;
        boolean onlyPositional = false;

        final int n = args == null ? 0 : args.length;
        while (i < n) {
            final String arg = args[i];

            if (onlyPositional) {
                positionals.add(arg);
                i++;
                continue;
            }

            if (arg.equals(DOUBLE_DASH)) { // everything after is positional
                onlyPositional = true;
                i++;
                continue;
            }

            if (arg.startsWith(DOUBLE_DASH)) { // long option
                i = handleLong(arg, args, i);
            } else if (arg.startsWith(DASH) && arg.length() > 1) { // short cluster
                i = handleShort(arg, args, i);
            } else {                          // positional ("-" alone counts)
                positionals.add(arg);
                i++;
            }
        }

        // If help was requested, skip required-checks (user just wants help)
        if (!helpRequested()) {
            checkRequired();
        }
    }

    /**
     * True if the built-in --help/-h option was supplied.
     */
    public boolean helpRequested() {
        return getBool("help");
    }

    private int handleLong(final String arg, final String[] args, final int i) {
        final String body = arg.substring(2);
        final String name;
        String inlineVal = null;
        final int eq = body.indexOf('=');
        if (eq >= 0) {
            name = body.substring(0, eq);
            inlineVal = body.substring(eq + 1);
        } else {
            name = body;
        }

        Option o = byLong.get(name);
        if (o == null) {
            // Attempt unambiguous prefix match (getopt_long-like convenience)
            o = resolvePrefix(name);
            if (o == null) {
                throw new ParseException("Unrecognized option '--" + name + "'");
            }
        }

        o.present = true;
        if (!o.takesArg()) {
            // Presence-based options (FLAG, BOOLEAN) do not accept a value.
            if (inlineVal != null) {
                throw new ParseException("Option '--" + o.longName + "' doesn't allow an argument");
            }
            o.value = Boolean.TRUE;
            return i + 1;
        }

        final int next = i + 1;
        if (inlineVal != null) {
            o.value = convert(o, inlineVal);
        } else if (o.optionalArg) {
            // Optional value: bare '--opt' takes the implied value; never consume the next arg.
            o.value = o.impliedVal;
        } else {
            if (next >= args.length) {
                throw new ParseException("Option '--" + o.longName + "' requires an argument");
            }
            o.value = convert(o, args[next]);
            return next + 1;
        }
        return next;
    }

    private Option resolvePrefix(final String prefix) {
        Option found = null;
        for (final Map.Entry<String, Option> e : byLong.entrySet()) {
            if (e.getKey().startsWith(prefix)) {
                if (found != null) {
                    return null; // ambiguous
                }
                found = e.getValue();
            }
        }
        return found;
    }

    private int handleShort(final String arg,
                            final String[] args,
                            final int i) {
        // e.g. -v, -vc, -c5, -c 5
        final String cluster = arg.substring(1);
        int next = i + 1;
        for (int k = 0; k < cluster.length(); k++) {
            final char c = cluster.charAt(k);
            final Option o = byShort.get(c);
            if (o == null) {
                throw new ParseException("Invalid option -- '" + c + "'");
            }

            o.present = true;
            if (!o.takesArg()) {
                // Presence-based options (FLAG, BOOLEAN): true when present.
                o.value = Boolean.TRUE;
                continue;
            }
            // Option takes an argument: the rest of the cluster is the value; else the next
            // arg (for a required value) or the implied value (for an optional-value option).
            if (k + 1 < cluster.length()) {
                o.value = convert(o, cluster.substring(k + 1));
            } else if (o.optionalArg) {
                o.value = o.impliedVal; // bare '-o' takes the implied value; no next arg consumed
            } else {
                if (next >= args.length) {
                    throw new ParseException("Option requires an argument -- '" + c + "'");
                }
                o.value = convert(o, args[next++]);
            }
            return next;
        }
        return next;
    }

    private Object convert(final Option o, final String raw) {
        final Object v;
        try {
            switch (o.type) {
                case INT:
                    v = Integer.parseInt(raw.trim());
                    break;
                case LONG:
                    v = Long.parseLong(raw.trim());
                    break;
                case DOUBLE:
                    v = Double.parseDouble(raw.trim());
                    break;
                case BOOLEAN:
                    v = parseBool(raw.trim());
                    break;
                case STRING:
                default:
                    v = raw;
                    break;
            }
        } catch (final NumberFormatException e) {
            throw new ParseException("Invalid " + o.type.name().toLowerCase() + " value '" + raw + "' for option '"
                    + displayName(o) + "'");
        }
        if (o.choices != null && !o.choices.contains(v.toString())) {
            throw new ParseException("Invalid value '" + raw + "' for option '" + displayName(o)
                    + "'; expected one of: " + String.join(COMMA_SEPARATION, o.choices));
        }
        return v;
    }

    private Boolean parseBool(final String s) {
        final String l = s.toLowerCase();
        if (l.equals("true")
                || l.equals("yes")
                || l.equals("1")
                || l.equals("on")) {
            return Boolean.TRUE;
        }
        if (l.equals("false")
                || l.equals("no")
                || l.equals("0")
                || l.equals("off")) {
            return Boolean.FALSE;
        }
        throw new ParseException("Invalid boolean value '" + s + "'");
    }

    private void checkRequired() {
        final List<String> missing = new ArrayList<>();
        for (final Option o : ordered) {
            if (o.required && !o.present) {
                missing.add(displayName(o));
            }
        }
        if (!missing.isEmpty()) {
            throw new ParseException("The following required option(s) are missing: " + String.join(COMMA_SEPARATION,
                    missing));
        }
    }

    private static String displayName(final Option o) {
        if (o.longName != null) {
            return DOUBLE_DASH + o.longName;
        }
        return DASH + o.shortName;
    }

    private Option require(final String name) {
        Option o = byLong.get(name);
        if (o == null && name.length() == 1) {
            o = byShort.get(name.charAt(0));
        }
        if (o == null) {
            throw new IllegalArgumentException("Unknown option: " + name);
        }
        return o;
    }

    public boolean isPresent(final String name) {
        return require(name).present;
    }

    public boolean getBool(final String name) {
        final Object v = require(name).value;
        return v != null && (Boolean) v;
    }

    public String getString(final String name) {
        final Object v = require(name).value;
        return v == null ? null : v.toString();
    }

    public int getInt(final String name) {
        final Object v = require(name).value;
        return v == null ? 0 : ((Number) v).intValue();
    }

    public long getLong(final String name) {
        final Object v = require(name).value;
        return v == null ? 0L : ((Number) v).longValue();
    }

    public double getDouble(final String name) {
        final Object v = require(name).value;
        return v == null ? 0.0 : ((Number) v).doubleValue();
    }

    public List<String> positionals() {
        return new ArrayList<>(positionals);
    }

    public void printHelp() {
        System.out.print(helpString());
    }

    public void printUsage() {
        System.out.print(usageString());
    }

    public String usageString() {
        if (usageOverride != null) {
            return "Usage: " + usageOverride + System.lineSeparator();
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("Usage: ").append(programName);

        for (final Option o : ordered) {
            sb.append(WHITESPACE_CHAR);
            final String core = optionSyntax(o);
            sb.append(o.required ? core : "[" + core + "]");
        }
        sb.append(" [ARGS...]");

        // Wrap the usage line under an indent aligned after "Usage: "
        final String prefix = "Usage: ";
        final String content = sb.substring(prefix.length());
        return wrapWithHanging(prefix, content, helpWidth) + System.lineSeparator();
    }

    private String optionSyntax(final Option o) {
        final StringBuilder core = new StringBuilder();
        if (o.shortName != null) {
            core.append(DASH).append(o.shortName).append(valueSuffixShort(o));
        } else {
            core.append(DOUBLE_DASH).append(o.longName).append(valueSuffixLong(o));
        }
        return core.toString();
    }

    /**
     * Value placeholder attached to a long option: {@code " <m>"}, or {@code "[=<m>]"} if optional.
     */
    private static String valueSuffixLong(final Option o) {
        if (!o.takesArg()) {
            return EMPTY_STRING;
        }
        final String m = o.metaVarOrDefault();
        return o.optionalArg ? "[=" + m + "]" : WHITESPACE + m;
    }

    /**
     * Value placeholder attached to a short option: {@code " <m>"}, or {@code "[<m>]"} if optional.
     */
    private static String valueSuffixShort(final Option o) {
        if (!o.takesArg()) {
            return EMPTY_STRING;
        }
        final String m = o.metaVarOrDefault();
        return o.optionalArg ? "[" + m + "]" : WHITESPACE + m;
    }

    public String helpString() {
        final StringBuilder sb = new StringBuilder();

        sb.append(usageString());
        sb.append(System.lineSeparator());

        if (!description.isEmpty()) {
            sb.append(wrap(description, helpWidth));
            sb.append(System.lineSeparator());
            sb.append(System.lineSeparator());
        }

        // The left column and the description column are computed across ALL options, so
        // descriptions align across every group.
        final List<String> left = new ArrayList<>();
        int maxLeft = 0;
        for (final Option o : ordered) {
            final String l = leftColumn(o);
            left.add(l);
            maxLeft = Math.max(maxLeft, l.length());
        }

        final int indent = 2; // left margin
        final int gap = 2; // between columns
        final int optCol = indent + maxLeft + gap;
        // Cap the option column so help text has room; overflow onto the next line
        final int maxOptCol = Math.min(optCol, helpWidth / 2);
        final int textWidth = Math.max(20, helpWidth - maxOptCol);

        // Section order: the root group first, then named groups in first-appearance order
        final List<String> groupOrder = new ArrayList<>();
        groupOrder.add(null);
        for (final Option o : ordered) {
            if (o.group != null && !groupOrder.contains(o.group)) {
                groupOrder.add(o.group);
            }
        }

        boolean firstSection = true;
        for (final String g : groupOrder) {
            boolean headerWritten = false;
            for (int idx = 0; idx < ordered.size(); idx++) {
                if (!Objects.equals(ordered.get(idx).group, g)) {
                    continue;
                }
                if (!headerWritten) {
                    if (!firstSection) {
                        sb.append(System.lineSeparator());
                    }
                    sb.append(g == null ? ROOT_GROUP : g).append(':').append(System.lineSeparator());
                    headerWritten = true;
                    firstSection = false;
                }
                appendOptionRow(sb, left.get(idx), buildHelpText(ordered.get(idx)), indent, gap, maxOptCol, textWidth);
            }
        }

        if (epilogue != null && !epilogue.isEmpty()) {
            sb.append(System.lineSeparator());
            sb.append(wrap(epilogue, helpWidth));
            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }

    private void appendOptionRow(final StringBuilder sb,
                                 final String left,
                                 final String helpText,
                                 final int indent,
                                 final int gap,
                                 final int maxOptCol,
                                 final int textWidth) {
        final StringBuilder line = new StringBuilder();
        line.append(spaces(indent)).append(left);
        final List<String> wrapped = wrapToLines(helpText, textWidth);
        if (line.length() + gap > maxOptCol && !left.isEmpty()) {
            // Left column too wide: put help on subsequent lines.
            sb.append(line).append(System.lineSeparator());
            for (final String w : wrapped) {
                sb.append(spaces(maxOptCol)).append(w).append(System.lineSeparator());
            }
        } else {
            // Pad to the option column, then the first help line.
            while (line.length() < maxOptCol) {
                line.append(WHITESPACE_CHAR);
            }
            if (wrapped.isEmpty()) {
                sb.append(line).append(System.lineSeparator());
            } else {
                sb.append(line).append(wrapped.get(0)).append(System.lineSeparator());
                for (int w = 1; w < wrapped.size(); w++) {
                    sb.append(spaces(maxOptCol)).append(wrapped.get(w)).append(System.lineSeparator());
                }
            }
        }
    }

    private String leftColumn(final Option o) {
        final StringBuilder s = new StringBuilder();
        if (o.shortName != null) {
            s.append(DASH).append(o.shortName);
        } else {
            s.append(DOUBLE_WHITESPACE); // align long-only options under the short slot
        }
        if (o.shortName != null && o.longName != null) {
            s.append(COMMA_SEPARATION);
        } else if (o.longName != null) {
            s.append(DOUBLE_WHITESPACE);
        }
        if (o.longName != null) {
            s.append(DOUBLE_DASH).append(o.longName).append(valueSuffixLong(o));
        } else {
            s.append(valueSuffixShort(o)); // short-only: value after the short name
        }
        return s.toString();
    }

    private String buildHelpText(final Option o) {
        final StringBuilder s = new StringBuilder(o.help);
        if (o.choices != null) {
            appendParenthetical(s, "one of: " + String.join(COMMA_SEPARATION, o.choices));
        }
        final List<String> annos = new ArrayList<>();
        if (o.required) {
            annos.add("required");
        } else if (o.takesArg() && o.defaultVal != null) {
            annos.add("default: " + o.defaultVal);
        } else if (o.type == Type.BOOLEAN && Boolean.TRUE.equals(o.defaultVal)) {
            // BOOLEAN is presence-based but may carry a non-false default
            annos.add("default: true");
        }
        if (!annos.isEmpty()) {
            appendParenthetical(s, String.join(COMMA_SEPARATION, annos));
        }
        return s.toString();
    }

    private static void appendParenthetical(final StringBuilder s, final String content) {
        if (s.length() > 0 && s.charAt(s.length() - 1) != WHITESPACE_CHAR) {
            s.append(WHITESPACE_CHAR);
        }
        s.append('(').append(content).append(')');
    }

    /**
     * Wrap plain text to width, no indentation.
     */
    private static String wrap(final String text,
                               final int width) {
        return String.join(System.lineSeparator(), wrapToLines(text, width));
    }

    /**
     * Wrap a prefixed line so that continuation lines hang under the content
     * (indented by prefix length).
     */
    private static String wrapWithHanging(final String prefix,
                                          final String content,
                                          final int width) {
        final int hang = prefix.length();
        final int avail = Math.max(10, width - hang);
        final List<String> lines = wrapToLines(content, avail);
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            if (i == 0) {
                sb.append(prefix);
            } else {
                sb.append(spaces(hang));
            }
            sb.append(lines.get(i));
            if (i < lines.size() - 1) {
                sb.append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    /**
     * Core greedy word-wrap. Preserves explicit newlines in the input as
     * paragraph breaks. Words longer than the width are hard-split.
     */
    private static List<String> wrapToLines(final String text,
                                            final int widthArg) {
        final List<String> out = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            out.add(EMPTY_STRING);
            return out;
        }
        final int width = Math.max(1, widthArg);

        for (final String paragraph : text.split(LINE_BREAK, -1)) {
            final String[] words = paragraph.trim().split("\\s+");
            final StringBuilder line = new StringBuilder();
            for (final String word : words) {
                if (word.isEmpty()) {
                    continue;
                }

                String remaining = word;

                // Hard-split words longer than the whole width
                while (remaining.length() > width) {
                    if (line.length() > 0) {
                        out.add(line.toString());
                        line.setLength(0);
                    }
                    out.add(remaining.substring(0, width));
                    remaining = remaining.substring(width);
                }

                if (line.length() == 0) {
                    line.append(remaining);
                } else if (line.length() + 1 + remaining.length() <= width) {
                    line.append(WHITESPACE_CHAR).append(remaining);
                } else {
                    out.add(line.toString());
                    line.setLength(0);
                    line.append(remaining);
                }
            }
            out.add(line.toString()); // flush paragraph (may be empty)
        }
        // Avoid trailing empty line artifact when input had no newline
        if (out.size() > 1 && out.get(out.size() - 1).isEmpty() && !text.endsWith(LINE_BREAK)) {
            out.remove(out.size() - 1);
        }
        return out;
    }

    private static String spaces(final int n) {
        if (n <= 0) {
            return EMPTY_STRING;
        }
        final char[] c = new char[n];
        Arrays.fill(c, WHITESPACE_CHAR);
        return new String(c);
    }
}