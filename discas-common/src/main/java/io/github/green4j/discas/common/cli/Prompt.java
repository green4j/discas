/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A small terminal dialogue: a question, an optional set of answers to pick from, and a default.
 * Dependency-free, like {@link GetOpts} beside it, and for the same reason -- a tool an operator
 * runs at three in the morning should not drag a terminal library into the shipped jars.
 *
 * <h2>The one rule: a prompt never blocks a script</h2>
 * When there is no console -- a pipe, a CI job, {@code nohup} -- {@link #interactive()} is false and
 * every question <b>returns its default without printing or reading anything</b>. So the same
 * command line works in both places, and a tool built on this can be run unattended without a
 * special flag to say so. A value that has no safe default therefore has to be a required option,
 * not a question.
 *
 * <h2>What "pretty" is allowed to mean</h2>
 * Colour and the box rule are emitted only when a console is attached and {@code TERM} is not
 * {@code dumb}; the questions read the same without them. Nothing here moves the cursor, clears the
 * screen or reads raw keys: a numbered list and a typed number work over ssh, in tmux, and in the
 * terminal emulator nobody has updated since 2011, which arrow-key menus do not.
 */
public final class Prompt {

    private static final String BOLD = "[1m";
    private static final String DIM = "[2m";
    private static final String RESET = "[0m";

    private final BufferedReader in;
    private final PrintStream out;
    private final boolean interactive;
    private final boolean styled;

    public Prompt(final BufferedReader in,
                  final PrintStream out,
                  final boolean interactive,
                  final boolean styled) {
        this.in = in;
        this.out = out;
        this.interactive = interactive;
        this.styled = styled;
    }

    /** A prompt on this process's console, or a silent one that answers with defaults. */
    public static Prompt console() {
        final boolean interactive = System.console() != null;
        final String term = System.getenv("TERM");
        return new Prompt(
                new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)),
                System.out,
                interactive,
                interactive && term != null && !"dumb".equals(term));
    }

    /** False when nobody is there to answer: every question then returns its default. */
    public boolean interactive() {
        return interactive;
    }

    /** A section title, so a long dialogue reads as a form rather than a stream of questions. */
    public void heading(final String title) {
        if (!interactive) {
            return;
        }
        out.println();
        out.println(styled ? BOLD + title + RESET : title);
        out.println(rule(title.length()));
    }

    /** A line of context under a heading -- what this section is deciding, and why it matters. */
    public void say(final String text) {
        if (!interactive) {
            return;
        }
        out.println(styled ? DIM + text + RESET : text);
    }

    /** Free text, defaulting to {@code defaultValue} on an empty answer. */
    public String ask(final String question, final String defaultValue) {
        if (!interactive) {
            return defaultValue;
        }
        out.println();
        out.println(question);
        out.print(styled
                ? "  " + BOLD + ">" + RESET + " " + DIM + "[" + defaultValue + "]" + RESET + " "
                : "  > [" + defaultValue + "] ");
        out.flush();
        final String answer = readLine();
        return answer.isEmpty() ? defaultValue : answer;
    }

    /** Yes or no, defaulting on an empty answer; anything unrecognised asks again. */
    public boolean confirm(final String question, final boolean defaultYes) {
        if (!interactive) {
            return defaultYes;
        }
        for (;;) {
            out.println();
            out.print(question + (styled ? DIM : "") + (defaultYes ? " [Y/n] " : " [y/N] ")
                    + (styled ? RESET : ""));
            out.flush();
            final String answer = readLine().toLowerCase();
            if (answer.isEmpty()) {
                return defaultYes;
            }
            if ("y".equals(answer) || "yes".equals(answer)) {
                return true;
            }
            if ("n".equals(answer) || "no".equals(answer)) {
                return false;
            }
        }
    }

    /**
     * One of {@code choices}, by number, defaulting on an empty answer. An answer that is not a
     * number in range asks again rather than falling through to the default -- a mistyped choice is
     * not a preference.
     */
    public <T> T choose(final String question,
                        final List<Choice<T>> choices,
                        final int defaultIndex) {
        if (choices.isEmpty()) {
            throw new IllegalArgumentException("A choice needs choices");
        }
        final int fallback = Math.max(0, Math.min(defaultIndex, choices.size() - 1));
        if (!interactive) {
            return choices.get(fallback).value;
        }
        for (;;) {
            out.println();
            out.println(question);
            for (int i = 0; i < choices.size(); i++) {
                final Choice<T> choice = choices.get(i);
                final boolean isDefault = i == fallback;
                final String marker = isDefault ? " > " : "   ";
                final String label = (i + 1) + ") " + choice.label;
                out.println((styled && isDefault ? BOLD + marker + label + RESET : marker + label)
                        + (choice.description.isEmpty() ? ""
                        : (styled ? DIM : "") + "  -- " + choice.description
                        + (styled ? RESET : "")));
            }
            out.print(styled
                    ? "  " + BOLD + ">" + RESET + " " + DIM + "[" + (fallback + 1) + "]" + RESET + " "
                    : "  > [" + (fallback + 1) + "] ");
            out.flush();

            final String answer = readLine();
            if (answer.isEmpty()) {
                return choices.get(fallback).value;
            }
            final int picked = parseIndex(answer, choices.size());
            if (picked >= 0) {
                return choices.get(picked).value;
            }
            out.println("  not one of 1.." + choices.size());
        }
    }

    /** One answer of a {@link #choose}: the value it stands for, its name, and a line about it. */
    public static final class Choice<T> {
        private final T value;
        private final String label;
        private final String description;

        public Choice(final T value, final String label, final String description) {
            this.value = value;
            this.label = label;
            this.description = description == null ? "" : description;
        }
    }

    /** {@code Prompt.choice(...)} reads better inside a list literal than a constructor call. */
    public static <T> Choice<T> choice(final T value, final String label, final String description) {
        return new Choice<>(value, label, description);
    }

    /** {@code Prompt.choices(a, b, c)} -- a list of choices without the ceremony. */
    @SafeVarargs
    public static <T> List<Choice<T>> choices(final Choice<T>... items) {
        final List<Choice<T>> list = new ArrayList<>(items.length);
        for (final Choice<T> item : items) {
            list.add(item);
        }
        return list;
    }

    private static int parseIndex(final String answer, final int size) {
        try {
            final int picked = Integer.parseInt(answer.trim());
            return picked >= 1 && picked <= size ? picked - 1 : -1;
        } catch (final NumberFormatException notANumber) {
            return -1;
        }
    }

    private String readLine() {
        try {
            final String line = in.readLine();
            // End of input while a question is open: the caller asked something nobody will answer,
            // and looping on an endless stream of nulls would hang the tool.
            if (line == null) {
                throw new IllegalStateException("Input ended while waiting for an answer");
            }
            return line.trim();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String rule(final int length) {
        final StringBuilder line = new StringBuilder();
        for (int i = 0; i < length; i++) {
            line.append('-');
        }
        return styled ? DIM + line + RESET : line.toString();
    }
}
