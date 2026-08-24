/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.crash;

import io.github.green4j.discas.node.starter.DisCasNodeStarter;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * One cluster member running as its own operating-system process, so it can be stopped the way a
 * machine stops it.
 *
 * <p>The subject is {@link DisCasNodeStarter} itself -- the shipped daemon entry point, not a
 * test-local {@code main}. That is deliberate: what this kills is what an operator runs, including
 * the shutdown hook that {@code SIGKILL} skips. A test main would prove the same thing about a
 * process nobody deploys, and would quietly stop covering the starter's own wiring (WAL open,
 * recovery, client port, peer mesh) the moment that wiring changed.
 *
 * <p><b>Readiness comes from the node, not from a sleep.</b> {@code GET /ready} answers 200 once the
 * node has recovered its state <em>and</em> can see a majority, which is exactly the condition the
 * nemesis must wait for before it takes the next member down. Polling it also detects a subject that
 * died during startup, which a sleep would turn into a mystery further down the run.
 *
 * <p>Output goes to a file rather than a pipe. A pipe nobody drains fills and blocks the child --
 * and this child logs at boot -- so a reader thread would be mandatory; a file needs no thread and
 * still gives the whole log to whatever failure message needs it.
 *
 * @see ClusterKillNineChaosTest the cluster these make up
 */
final class NodeProcess {

    /** {@code 128 + SIGKILL}: the exit code that says the process was killed rather than stopped. */
    private static final int SIGKILL_EXIT = 137;

    /** How much of the node's log a failure message carries. Enough to hold a stack trace. */
    private static final int LOG_TAIL_LINES = 60;

    private final int id;
    private final String clusterId;
    private final String members;
    private final InetSocketAddress clientAddress;
    private final int observabilityPort;
    private final Path walDir;
    private final Path logFile;

    private Process process;
    private int kills;

    NodeProcess(final int id,
                final String clusterId,
                final String members,
                final int clientPort,
                final int observabilityPort,
                final Path rootDir) throws IOException {
        this.id = id;
        this.clusterId = clusterId;
        this.members = members;
        this.clientAddress = new InetSocketAddress("127.0.0.1", clientPort);
        this.observabilityPort = observabilityPort;
        this.walDir = rootDir.resolve("node-" + id);
        this.logFile = rootDir.resolve("node-" + id + ".log");
        Files.createDirectories(walDir);
    }

    int id() {
        return id;
    }

    /** Where clients reach this member. Fixed across restarts: the client already holds it. */
    InetSocketAddress clientAddress() {
        return clientAddress;
    }

    /** How many times this member has been killed, for the run's report. */
    int kills() {
        return kills;
    }

    /**
     * Spawn the daemon on the same storage directory and the same ports as any previous
     * incarnation. Restarting onto its own ports is the realistic shape -- an operator restarts a
     * node where it was -- and it works because every listener sets {@code SO_REUSEADDR}.
     */
    void start() throws IOException {
        if (process != null && process.isAlive()) {
            throw new IllegalStateException("Node " + id + " is already running");
        }
        final Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        final List<String> command = new ArrayList<>(List.of(
                java.toString(),
                // A node holding three keys needs nothing; the cap keeps four JVMs on one runner
                // from competing for memory instead of for the bugs this test is looking for.
                "-Xmx256m",
                "-cp", System.getProperty("java.class.path"),
                DisCasNodeStarter.class.getName(),
                "--node-id", Integer.toString(id),
                "--cluster-id", clusterId,
                "--members", members,
                "--client-bind", clientAddress.getHostString() + ":" + clientAddress.getPort(),
                "--wal-dir", walDir.toString(),
                // Every member needs its own, or the second process to start finds 9600 taken and
                // dies -- as an observability failure, which is not what this test is about.
                "--observability-bind", "127.0.0.1:" + observabilityPort,
                // The two defaults sized for a wide-area link. On loopback they only decide how
                // long an operation waits on a member that has just been killed, and waiting five
                // seconds for that verdict starves the history rather than testing anything.
                "--round-timeout-ms", "1000",
                "--no-quorum-backoff-ms", "500"));
        final ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));
        process = builder.start();
    }

    /**
     * Wait until the node reports itself ready, i.e. recovered and quorum-connected. Fails fast if
     * the process is gone, so a subject that could not start is reported as that rather than as a
     * timeout.
     */
    void awaitReady(final Duration budget) throws Exception {
        final long deadline = System.nanoTime() + budget.toNanos();
        while (System.nanoTime() - deadline < 0) {
            if (!process.isAlive()) {
                throw new IllegalStateException("Node " + id + " exited with "
                        + process.exitValue() + " before it became ready:\n" + logTail());
            }
            if (ready()) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new IllegalStateException(
                "Node " + id + " was not ready within " + budget + ":\n" + logTail());
    }

    /** True on {@code 200 /ready}; false while the endpoint is absent, refusing or answering 503. */
    private boolean ready() {
        HttpURLConnection connection = null;
        try {
            final URL url = new URL("http://127.0.0.1:" + observabilityPort + "/ready");
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(250);
            connection.setReadTimeout(250);
            return connection.getResponseCode() == 200;
        } catch (final IOException notUpYet) {
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * {@code kill -9}, by name rather than {@code destroyForcibly()}, so what this test does is not
     * a detail of the JDK's process API. The exit code is checked for the same reason
     * {@code KillNineCrashTest} checks it: a member that managed to exit any other way ran the
     * orderly shutdown this test exists to skip, and the run would be claiming more than it did.
     */
    void killNine() throws Exception {
        if (!process.isAlive()) {
            throw new IllegalStateException("Node " + id + " died on its own (exit "
                    + process.exitValue() + ") before the nemesis reached it:\n" + logTail());
        }
        new ProcessBuilder("kill", "-9", Long.toString(process.pid())).start().waitFor();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Node " + id + " survived SIGKILL");
        }
        if (process.exitValue() != SIGKILL_EXIT) {
            throw new IllegalStateException("Node " + id + " exited with " + process.exitValue()
                    + " rather than of SIGKILL (" + SIGKILL_EXIT + "), so it was not crashed:\n"
                    + logTail());
        }
        kills++;
    }

    boolean isAlive() {
        return process != null && process.isAlive();
    }

    /** Teardown only: stop the process however it will stop, and never mind how it exited. */
    void stop() {
        if (process == null) {
            return;
        }
        process.destroyForcibly();
        try {
            process.waitFor(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** The tail of this node's log, for a failure message that would otherwise say nothing. */
    String logTail() {
        try {
            final List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
            final List<String> tail = lines.subList(
                    Math.max(0, lines.size() - LOG_TAIL_LINES), lines.size());
            return "--- node " + id + " log (last " + tail.size() + " lines) ---\n"
                    + String.join("\n", tail);
        } catch (final IOException e) {
            return "--- node " + id + " log unreadable: " + e + " ---";
        }
    }
}
