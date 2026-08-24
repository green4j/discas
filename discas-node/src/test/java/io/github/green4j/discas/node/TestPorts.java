/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Port allocation for tests that must know an address before binding it.
 * <p>
 * Deliberately identical to {@code io.github.green4j.discas.TestPorts} in discas-int-test, name
 * for name and line for line: the two test trees cannot see each other, and one convention read
 * two different ways in two modules is what this sweep exists to remove. Change both together.
 *
 * <h2>The window</h2>
 * Choosing a port and closing the probe is racy: the port is free between the probe and the real
 * bind, so anything may take it. That window cannot be closed while the address has to be published
 * in config before the server exists -- a peer mesh needs every member's address up front. What
 * this class can do is make the window uninteresting, on two axes.
 * <p>
 * <b>Within the JVM:</b> a port is never issued twice, and the probes for a batch are held open
 * until the whole batch is chosen, so members of one cluster cannot collide with each other. A
 * caller that can bind immediately should use {@link #bindHeld()}, which has no window at all.
 *
 * <h2>Why the ports come from a fixed low range, not from {@code ServerSocket(0)}</h2>
 * Probing with {@code ServerSocket(0)} yields a port from the OS <b>ephemeral</b> range -- the same
 * pool the kernel draws from when any socket calls {@code connect()} without binding first. So an
 * earmarked port is not merely free between probe and bind; it is <em>a candidate the kernel will
 * actively hand to some other connection in this JVM</em>. Remembering issued ports cannot prevent
 * that: {@code ISSUED} governs this class, not the kernel's outbound allocator.
 * <p>
 * That is not theoretical. It is how a node restarting onto its own client port -- a port it must
 * reuse, because the client already holds that address -- found the port occupied by an outbound
 * connection belonging to a test running in parallel. {@code SO_REUSEADDR} does not help: it
 * permits rebinding over TIME_WAIT, never over a live socket.
 * <p>
 * Allocating from a range <b>outside</b> every common ephemeral range (Linux 32768-60999, macOS and
 * BSD 49152-65535) removes the cause rather than shrinking the window: a port here can only be
 * taken by something that deliberately binds it, and nothing in these suites does.
 * <p>
 * Still not a hard guarantee -- another process on the machine may bind one of these -- but the
 * remaining risk no longer scales with how much of the suite runs concurrently, which is what made
 * the old scheme fail exactly when the whole suite ran and pass when a class ran alone.
 */
public final class TestPorts {

    /**
     * Allocation range, chosen to sit below every common ephemeral range so the kernel never hands
     * one of these to an outbound connection. See the class javadoc.
     */
    private static final int RANGE_START = 20_000;
    private static final int RANGE_END = 30_000;

    /** Random per-JVM start, so parallel Gradle test JVMs do not collide port-for-port. */
    private static final AtomicInteger CURSOR =
            new AtomicInteger(new Random().nextInt(RANGE_END - RANGE_START));

    private static final int MAX_ATTEMPTS = 20;
    private static final long INITIAL_BACKOFF_MS = 25L;
    private static final long MAX_BACKOFF_MS = 500L;

    /** Serialises allocation so two batches cannot probe the same port simultaneously. */
    private static final ReentrantLock LOCK = new ReentrantLock();

    /** Every port handed out by this JVM. Never reissued, even long after the test that used it. */
    private static final Set<Integer> ISSUED = ConcurrentHashMap.newKeySet();

    private TestPorts() {
    }

    /**
     * A {@link ServerSocket} already bound to a port this JVM has not issued before. The caller
     * owns it and must close it.
     * <p>
     * For a listener that can bind straight away this is strictly better than {@link #free()}:
     * there is no probe-to-bind window at all. It also keeps the port out of {@link #free()}'s
     * results, which matters because anything calling {@code new ServerSocket(0)} directly can
     * otherwise be handed a port already earmarked for a server that has not bound yet -- the
     * chaos harness hit exactly that, allocating node ports and then letting its proxies grab one
     * before the nodes came up ("Failed to start node N").
     */
    public static ServerSocket bindHeld() {
        LOCK.lock();
        try {
            final List<ServerSocket> held = new ArrayList<>(1);
            final int port = probeUnissued(held);
            ISSUED.add(port);
            return held.get(0);
        } finally {
            LOCK.unlock();
        }
    }

    /** A single free port. */
    public static int free() {
        return allocate(1).get(0);
    }

    /**
     * {@code count} distinct free ports. The probe sockets are all held open until every port has
     * been chosen, so no two ports in the batch can be the same.
     */
    public static List<Integer> allocate(final int count) {
        LOCK.lock();
        try {
            final List<ServerSocket> probes = new ArrayList<>(count);
            final List<Integer> ports = new ArrayList<>(count);
            try {
                for (int i = 0; i < count; i++) {
                    ports.add(probeUnissued(probes));
                }
            } finally {
                for (int i = 0; i < probes.size(); i++) {
                    try {
                        probes.get(i).close();
                    } catch (final Exception ignored) {
                        // A probe that will not close cannot make the chosen port less usable.
                    }
                }
            }
            ISSUED.addAll(ports);
            return ports;
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * Probe until a port turns up that this JVM has not issued before, keeping the probe socket
     * open (added to {@code probes}) so the same port cannot be handed out again in this batch.
     */
    private static int probeUnissued(final List<ServerSocket> probes) {
        final StringBuilder log = new StringBuilder();
        long backoffMs = INITIAL_BACKOFF_MS;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            ServerSocket probe = null;
            final int port = nextCandidate();
            try {
                if (ISSUED.contains(port)) {
                    continue;   // cheap: no socket opened, no backoff needed
                }
                // A specific port, not 0: binding succeeds only if nothing holds it, and the port
                // is outside the range the kernel draws outbound connections from.
                probe = new ServerSocket(port);
                probes.add(probe);
                return port;
            } catch (final Exception e) {
                log.append(" attempt=").append(attempt).append(" port=").append(port)
                        .append(" probeFailed=").append(e.getClass().getSimpleName());
                if (probe != null) {
                    try {
                        probe.close();
                    } catch (final Exception ignored) {
                        // Nothing useful to do; the retry allocates a fresh probe.
                    }
                }
            }
            sleep(backoffMs);
            backoffMs = Math.min(MAX_BACKOFF_MS, backoffMs * 2L);
        }
        throw new IllegalStateException(
                "No free port after " + MAX_ATTEMPTS + " attempts:" + log);
    }


    /**
     * The next port to try, walking a fixed range from a per-JVM random start so two forked test
     * JVMs do not march in step through the same ports.
     */
    private static int nextCandidate() {
        final int span = RANGE_END - RANGE_START;
        return RANGE_START + Math.floorMod(CURSOR.getAndIncrement(), span);
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Port allocation interrupted", e);
        }
    }
}
