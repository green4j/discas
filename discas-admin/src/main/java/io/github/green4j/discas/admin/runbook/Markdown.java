/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.runbook;

/**
 * A very small markdown builder, so the sections of a runbook are written as headings, paragraphs
 * and code blocks rather than as string concatenation with {@code \n\n} in it.
 */
final class Markdown {

    private final StringBuilder text = new StringBuilder();

    /**
     * Whether the last thing written was a list item. A list needs a blank line before it and none
     * inside it, which is the one piece of markdown state worth keeping rather than asking every
     * caller to remember.
     */
    private boolean inList;

    Markdown h1(final String heading) {
        return block("# " + heading);
    }

    Markdown h2(final String heading) {
        return block("## " + heading);
    }

    Markdown h3(final String heading) {
        return block("### " + heading);
    }

    /** A paragraph. */
    public Markdown p(final String paragraph) {
        return block(paragraph);
    }

    /** One item of a bullet list; consecutive calls stay in the same list. */
    Markdown bullet(final String item) {
        return line("- " + item);
    }

    /** One item of a numbered list, numbered by the caller so the reasons stay in order. */
    public Markdown step(final int number, final String item) {
        return line(number + ". " + item);
    }

    /** A fenced block. {@code language} may be empty for a plain one. */
    public Markdown code(final String language, final String body) {
        separate();
        inList = false;
        text.append("```").append(language).append('\n');
        text.append(body);
        if (!body.endsWith("\n")) {
            text.append('\n');
        }
        text.append("```\n");
        return this;
    }

    /** One list item: separated from whatever came before, joined to the item before it. */
    private Markdown line(final String content) {
        if (!inList) {
            separate();
            inList = true;
        }
        text.append(content).append('\n');
        return this;
    }

    private Markdown block(final String content) {
        separate();
        inList = false;
        text.append(content).append('\n');
        return this;
    }

    /** One blank line between blocks, and never two. */
    private void separate() {
        if (text.length() == 0) {
            return;
        }
        if (text.charAt(text.length() - 1) != '\n') {
            text.append('\n');
        }
        if (text.length() >= 2 && text.charAt(text.length() - 2) != '\n') {
            text.append('\n');
        }
    }

    @Override
    public String toString() {
        return text.toString();
    }
}
