/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.transport.security.PeerChannelSecurity;
import io.github.green4j.discas.common.transport.security.PeerCredential;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * {@link PeerChannelSecurity} backed by an {@link SSLEngine}: TLS 1.3 mutual
 * authentication and record encryption sitting beneath the frame codec.
 * <p>
 * The engine's wrap/unwrap state machine is driven by the peer transport's I/O
 * loop: {@link #unwrap} feeds inbound network records and yields decrypted
 * application bytes, {@link #wrap} encrypts outbound application frames, and
 * {@link #pendingOutbound} surfaces handshake/alert records the engine needs to
 * send. Decrypted application bytes that don't fit the caller's buffer are
 * retained in {@link #plainInbound} across calls.
 */
public final class TlsPeerSecurity implements PeerChannelSecurity {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private final SSLEngine engine;
    private final Deque<ByteBuffer> outbound = new ArrayDeque<>();
    private ByteBuffer plainInbound;   // decrypted app bytes not yet delivered (read mode)

    private boolean started;
    private boolean finished;
    private PeerCredential credential = PeerCredential.NONE;

    TlsPeerSecurity(final SSLEngine engine) {
        this.engine = engine;
        this.plainInbound = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
        this.plainInbound.flip(); // start empty in read mode
    }

    private void ensureStarted() {
        if (started) {
            return;
        }
        started = true;
        try {
            engine.beginHandshake();
        } catch (final SSLException e) {
            throw new RuntimeException("TLS beginHandshake failed", e);
        }
        pumpHandshake();
    }

    @Override
    public boolean handshakeFinished() {
        return finished;
    }

    @Override
    public ByteBuffer pendingOutbound() {
        ensureStarted();
        return outbound.poll();
    }

    @Override
    public void unwrap(final ByteBuffer net, final ByteBuffer app) {
        ensureStarted();
        drainPlain(app);
        while (net.hasRemaining()) {
            plainInbound.compact();                 // -> write mode, keep undelivered bytes
            final SSLEngineResult result;
            try {
                result = engine.unwrap(net, plainInbound);
            } catch (final SSLException e) {
                plainInbound.flip();
                throw new RuntimeException("TLS unwrap failed", e);
            }
            plainInbound.flip();                    // -> read mode
            drainPlain(app);
            pumpHandshake();
            checkFinished();

            final SSLEngineResult.Status status = result.getStatus();
            if (status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return;                             // need more network bytes
            }
            if (status == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                growPlainInbound();
                continue;
            }
            if (status == SSLEngineResult.Status.CLOSED) {
                return;
            }
            // OK: only stop early if the caller's buffer is full and we still
            // have handshake work pending on more network input.
            if (!app.hasRemaining() && plainInbound.hasRemaining()) {
                return;
            }
        }
    }

    @Override
    public ByteBuffer wrap(final ByteBuffer app) {
        ensureStarted();
        ByteBuffer net = ByteBuffer.allocate(
                engine.getSession().getPacketBufferSize() + app.remaining());
        while (app.hasRemaining()) {
            final SSLEngineResult result;
            try {
                result = engine.wrap(app, net);
            } catch (final SSLException e) {
                throw new RuntimeException("TLS wrap failed", e);
            }
            if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                net = grow(net, engine.getSession().getPacketBufferSize());
                continue;
            }
            if (result.getStatus() == SSLEngineResult.Status.CLOSED) {
                break;
            }
        }
        net.flip();
        return net;
    }

    @Override
    public PeerCredential peerCredential() {
        return credential;
    }

    @Override
    public void close() {
        try {
            engine.closeOutbound();
        } catch (final Exception ignored) {
            // best effort
        }
    }

    private void drainPlain(final ByteBuffer app) {
        if (!plainInbound.hasRemaining() || !app.hasRemaining()) {
            return;
        }
        final int n = Math.min(plainInbound.remaining(), app.remaining());
        final int savedLimit = plainInbound.limit();
        plainInbound.limit(plainInbound.position() + n);
        app.put(plainInbound);
        plainInbound.limit(savedLimit);
    }

    /** Run delegated tasks and emit any handshake records the engine wants to send. */
    private void pumpHandshake() {
        while (true) {
            final HandshakeStatus hs = engine.getHandshakeStatus();
            if (hs == HandshakeStatus.NEED_TASK) {
                Runnable task;
                while ((task = engine.getDelegatedTask()) != null) {
                    task.run();
                }
                continue;
            }
            if (hs == HandshakeStatus.NEED_WRAP) {
                ByteBuffer net = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
                final SSLEngineResult result;
                try {
                    result = engine.wrap(EMPTY, net);
                } catch (final SSLException e) {
                    throw new RuntimeException("TLS handshake wrap failed", e);
                }
                if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                    net = grow(net, engine.getSession().getPacketBufferSize());
                    // retry on next loop iteration by re-checking status
                }
                net.flip();
                if (net.hasRemaining()) {
                    outbound.add(net);
                }
                if (result.getStatus() == SSLEngineResult.Status.CLOSED) {
                    return;
                }
                continue;
            }
            // NEED_UNWRAP / NOT_HANDSHAKING / FINISHED -- no more we can do now.
            return;
        }
    }

    private void checkFinished() {
        if (finished) {
            return;
        }
        if (engine.getHandshakeStatus() == HandshakeStatus.NOT_HANDSHAKING) {
            finished = true;
            try {
                final Certificate[] chain = engine.getSession().getPeerCertificates();
                if (chain.length > 0 && chain[0] instanceof X509Certificate) {
                    credential = CertPeerCredential.fromCert((X509Certificate) chain[0]);
                }
            } catch (final Exception ignored) {
                // No verified peer cert -- credential stays NONE. With
                // needClientAuth the handshake would already have failed.
            }
        }
    }

    private void growPlainInbound() {
        final int newSize = plainInbound.capacity() + engine.getSession().getApplicationBufferSize();
        final ByteBuffer grown = ByteBuffer.allocate(newSize);
        grown.put(plainInbound);          // copy undelivered bytes (read mode)
        grown.flip();
        plainInbound = grown;
    }

    private static ByteBuffer grow(final ByteBuffer buf, final int extra) {
        final ByteBuffer grown = ByteBuffer.allocate(buf.capacity() + extra);
        buf.flip();
        grown.put(buf);
        return grown;
    }
}
