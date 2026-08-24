/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

/**
 * One {@code discas-admin} operation, as the launcher sees it: a name, a line of help, and a run.
 *
 * <p>Commands are written by extending {@link AbstractCommand}, which supplies everything that is
 * the same for all of them -- help, option parsing, the output prefix, the progress line, and taking
 * back what a failed run created. This interface is only the part the launcher needs, and it is
 * kept separate so that "what a command is" stays readable in three methods.
 *
 * <p><b>The parameters are not here.</b> Nothing is shared between what a dump needs
 * (a cluster to read, prefixes, a file to write) and what an init needs (a membership, a folder to
 * create, and answers about hosts it never contacts). Each command declares its own options; the
 * fragments that genuinely repeat live in {@link ClusterOptions}, which a command opts into by
 * calling it.
 */
interface Command {

    /** The word an operator types after {@code discas-admin}. */
    String name();

    /** One line, for the list of commands. */
    String summary();

    /** This command's usage, printed by the launcher when its command line does not parse. */
    String usageText();

    /**
     * Runs the command with everything after its name.
     * <p>
     * Prints its own progress and result. A thrown exception is a failed run: the launcher reports
     * it and exits non-zero -- {@code 2} when the command line itself was wrong, {@code 1} otherwise.
     */
    void run(String[] args) throws Exception;
}
