/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.io.FileWatchDaemon;
import io.github.green4j.discas.common.io.Reloadable;

import java.security.cert.X509Certificate;
import io.github.green4j.discas.common.io.ReloadObserver;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Applies rotated TLS material into a {@link ReloadableTlsContext} so the mesh is
 * never disrupted. It is the <b>consumer</b> of a {@link Reloadable} TLS-material source:
 * <ul>
 *   <li>The source (e.g. {@link FileTlsMaterialSource}) self-watches and <b>pushes</b>
 *       fresh material; this manager subscribes (replay-on-subscribe delivers the current
 *       material immediately) and marshals each apply onto an {@link Executor} -- the
 *       node's event loop in production, so the swap is serialized with the
 *       {@code SSLEngine} handshakes that also run there.</li>
 *   <li>Reloads trust-first (via {@link ReloadableTlsContext#reload}); only new
 *       handshakes see the rotated material -- established connections ride on.</li>
 *   <li>Warns when the leaf is within {@link RenewalPolicy#warnBeforeExpiry()} of expiry
 *       and nothing new has arrived (a stalled issuer/renewal agent).</li>
 * </ul>
 * Owns no thread of its own: watching runs on the single {@link FileWatchDaemon} thread;
 * application runs on the supplied {@code apply} executor.
 */
public final class CertRotationManager implements AutoCloseable {

    /** Applies synchronously on the caller's thread -- the default and what tests use. */
    private static final Executor DIRECT = Runnable::run;

    private final ReloadableTlsContext context;
    private final Reloadable<TlsMaterial> source;
    private final RenewalPolicy policy;
    private final Executor apply;
    private final ReloadObserver observer;

    private volatile X509Certificate currentLeaf;
    private volatile boolean started;
    private volatile boolean closed;
    private volatile boolean warnedNearExpiry;

    private FileWatchDaemon.Registration periodic; // near-expiry warning

    public CertRotationManager(final ReloadableTlsContext context,
                               final Reloadable<TlsMaterial> source,
                               final TlsMaterial initial) {
        this(context, source, initial, RenewalPolicy.defaults(), DIRECT);
    }

    public CertRotationManager(final ReloadableTlsContext context,
                               final Reloadable<TlsMaterial> source,
                               final TlsMaterial initial,
                               final RenewalPolicy policy) {
        this(context, source, initial, policy, DIRECT);
    }

    /**
     * @param apply the executor that applies rotated material -- pass the node's event
     *              loop ({@code loop::execute}) in production; defaults to synchronous.
     */
    public CertRotationManager(final ReloadableTlsContext context,
                               final Reloadable<TlsMaterial> source,
                               final TlsMaterial initial,
                               final RenewalPolicy policy,
                               final Executor apply) {
        this(context, source, initial, policy, apply, ReloadObserver.NONE);
    }

    /**
     * @param observer receives the near-expiry warning. The only signal that renewal has stopped
     *                 working: rotation is silent by nature, so without it an issuer that quit
     *                 delivering material is indistinguishable from one with nothing to deliver,
     *                 until the certificate expires and connections start failing.
     */
    public CertRotationManager(final ReloadableTlsContext context,
                               final Reloadable<TlsMaterial> source,
                               final TlsMaterial initial,
                               final RenewalPolicy policy,
                               final Executor apply,
                               final ReloadObserver observer) {
        this.observer = observer == null ? ReloadObserver.NONE : observer;
        this.context = context;
        this.source = source;
        this.policy = policy;
        this.apply = apply;
        this.currentLeaf = initial.leafCertificate();
    }

    /**
     * Begin: subscribe to the source (replay-on-subscribe applies the current material).
     * A second call is a no-op -- without the guard it would add a second listener and a
     * second daemon registration, doubling every reload and leaking the first timer.
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        // Source pushes on the daemon; we marshal the apply onto the loop.
        source.addListener(m -> apply.execute(() -> applyMaterial(m)));
        periodic = FileWatchDaemon.shared().register(
                List.of(), policy.checkInterval(), this::maybeWarnNearExpiry, observer);
    }

    private void applyMaterial(final TlsMaterial next) {
        if (closed) {
            return;
        }
        context.reload(next);
        final X509Certificate leaf = next.leafCertificate();
        if (leaf != null) {
            currentLeaf = leaf;
        }
        warnedNearExpiry = false;
    }

    private void maybeWarnNearExpiry() {
        final X509Certificate leaf = currentLeaf;
        if (leaf == null || warnedNearExpiry || closed) {
            return;
        }
        final boolean past = pastWarnDeadline(
                leaf.getNotBefore().getTime(), leaf.getNotAfter().getTime(),
                policy.warnBeforeExpiry(), System.currentTimeMillis());
        if (past) {
            warnedNearExpiry = true;
            observer.materialExpiring("cert-rotation: leaf certificate",
                    leaf.getNotAfter().toInstant());
        }
    }

    /**
     * Whether {@code now} is within {@code warnBeforeExpiry} of the leaf's {@code notAfter}
     * -- i.e. the near-expiry warning should fire. The lead time is clamped to at least the
     * cert half-life so the threshold is sane for any lifetime (a {@code warnBeforeExpiry}
     * larger than the whole lifetime does not warn from issuance).
     */
    public static boolean pastWarnDeadline(final long notBeforeMs, final long notAfterMs,
                                           final Duration warnBeforeExpiry, final long nowMs) {
        final long lead = Math.min(warnBeforeExpiry.toMillis(), (notAfterMs - notBeforeMs) / 2);
        return nowMs >= notAfterMs - lead;
    }

    @Override
    public void close() {
        closed = true;
        if (periodic != null) {
            periodic.close();
        }
    }
}
