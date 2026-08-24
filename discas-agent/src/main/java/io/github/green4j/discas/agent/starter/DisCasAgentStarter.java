/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent.starter;

import io.github.green4j.discas.agent.DisCasAgent;
import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigSupport;

import java.util.concurrent.CountDownLatch;

/**
 * Command-line entry point for the local DisCas agent. Resolves configuration from CLI flags and
 * {@code DISCAS_*} environment variables (see {@link DisCasAgentConfig}), then starts a
 * {@link DisCasAgent} and blocks until the JVM is asked to shut down. The effective values (with
 * their source) are printed at startup with secrets masked.
 *
 * <p>Since every value can come from the environment, an empty command line is <em>not</em> treated
 * as a help request; only an explicit {@code -h}/{@code --help} prints help. When required values are
 * absent from both sources, the error and usage are printed and the process exits non-zero.
 */
public final class DisCasAgentStarter {

    private DisCasAgentStarter() {
    }

    public static void main(final String[] args) throws Exception {
        if (DisCasAgentConfig.isHelpRequested(args)) {
            System.out.print(DisCasAgentConfig.helpText());
            return;
        }

        final DisCasAgentConfig cfg;
        try {
            cfg = DisCasAgentConfig.resolve(args, System.getenv());
        } catch (final GetOpts.ParseException | IllegalArgumentException e) {
            ConfigSupport.usageError(
                    DisCasAgentConfig.PROGRAM, e.getMessage(), DisCasAgentConfig.usageText());
            return;
        }

        cfg.print(System.out);
        run(cfg);
    }

    private static void run(final DisCasAgentConfig cfg) throws Exception {
        final DisCasAgent agent = DisCasAgent.start(cfg);
        System.out.println(DisCasAgentConfig.PROGRAM + ": listening on "
                + cfg.httpBind.getHostString() + ":" + agent.port()
                + " (client " + cfg.clientId.value() + " -> " + cfg.nodes.keySet() + ")");
        awaitShutdown(agent);
    }

    private static void awaitShutdown(final DisCasAgent agent) throws InterruptedException {
        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            agent.close();
            stopped.countDown();
        }, "discas-agent-shutdown"));
        stopped.await();
    }
}
