/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.Hex;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.identity.ClientDescription;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClientIdentity;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.node.acl.ClientOp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("LoggingAuditLog -- what a line shows of a key, a value and a digest")
class LoggingAuditLogTest {

    private static final ClientIdentity CLIENT = ClientIdentity.of(
            ClientId.of("web-1"), ClientDescription.of("Jenkins euc1-blue"));

    private final AuditRing ring = new AuditRing(1 << 16);
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final AuditEvent event = new AuditEvent();

    private static AuditConfigSnapshot settings(final AuditHashAlgorithm algorithm,
                                                final byte[] salt, final int saltId,
                                                final List<String> full,
                                                final List<String> text) {
        return new AuditConfigSnapshot(AuditConfigSnapshot.SINK_LOG, 1 << 16, AuditOverflow.DROP,
                16, 16, 0.25, algorithm, salt, saltId, 64, 4096, full, text);
    }

    private static AuditConfigSnapshot plain() {
        return settings(AuditHashAlgorithm.NONE, null, 0, List.of(), List.of());
    }

    private static ByteBuffer bytes(final int size, final int first) {
        final byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (first + i);
        }
        return ByteBuffer.wrap(data);
    }

    private List<String> render(final AuditConfigSnapshot cfg,
                                final java.util.function.Consumer<RingAuditRecorder> action) {
        final RingAuditRecorder recorder = new RingAuditRecorder(ring, cfg);
        action.accept(recorder);
        final LoggingAuditLog log = new LoggingAuditLog(
                new Log("n1", new PrintStream(out, true), new PrintStream(out, true)));
        final List<String> lines = new ArrayList<>();
        ring.drain((buffer, offset, length) -> {
            event.wrap(buffer, offset, length);
            log.record(event);
        }, 64);
        for (final String line : out.toString(StandardCharsets.UTF_8).split("\n")) {
            if (!line.isEmpty()) {
                lines.add(line);
            }
        }
        return lines;
    }

    @Test
    @DisplayName("The client id and its description are on every line")
    void sessionLinesCarryTheIdentity() {
        final List<String> lines = render(plain(), recorder -> {
            recorder.sessionOpened(CLIENT);
            recorder.sessionRefused(CLIENT, ClientHelloRespStatus.ACCESS_DENIED);
            recorder.sessionClosed(CLIENT);
        });

        assertEquals(3, lines.size());
        assertTrue(lines.get(0).contains("SESSION_OPENED client=web-1 (Jenkins euc1-blue)"));
        assertTrue(lines.get(1).contains("status=ACCESS_DENIED"));
        assertTrue(lines.get(2).contains("SESSION_CLOSED"));
    }

    @Test
    @DisplayName("A head and a tail, and never more than a share of the whole")
    void showsHeadAndTailBoundedByAShareOfTheValue() {
        final List<String> lines = render(plain(), recorder ->
                recorder.requestReceived(CLIENT, ClientOp.PUT, 42L, bytes(4, 1), bytes(100, 0),
                        0, false));

        final String line = lines.get(0);
        assertTrue(line.contains("op=PUT"), line);
        // A quarter of 100 bytes, so 25 of them: 13 of head and 12 of tail, as hex.
        assertTrue(line.contains("value=100:"), line);
        final String value = line.substring(line.indexOf("value=100:") + "value=100:".length());
        final String[] halves = value.split("\\.\\.");
        assertEquals(13 * 2, halves[0].length(), line);
        assertEquals(12 * 2, halves[1].trim().length(), line);
    }

    @Test
    @DisplayName("A short value shows almost nothing: the share is what keeps it from being quoted")
    void shortValueShowsAlmostNothing() {
        final List<String> lines = render(plain(), recorder ->
                recorder.requestReceived(CLIENT, ClientOp.PUT, 1L, bytes(4, 1), bytes(8, 0),
                        0, false));

        final String line = lines.get(0);
        // Two of eight bytes: one at each end.
        assertTrue(line.contains("value=8:00..07"), line);
    }

    @Test
    void prefixesMayAskForTheWholeValueAndForText() {
        final ByteBuffer key = ByteBuffer.wrap("lock/eod".getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap("owner=euc1-blue".getBytes(StandardCharsets.UTF_8));
        final List<String> lines = render(
                settings(AuditHashAlgorithm.NONE, null, 0, List.of("lock/"), List.of("lock/")),
                recorder -> recorder.requestReceived(CLIENT, ClientOp.PUT, 5L, key, value, 0, false));

        final String line = lines.get(0);
        assertTrue(line.contains("key=8:\"lock/eod\""), line);
        assertTrue(line.contains("value=15:\"owner=euc1-blue\""), line);
        assertFalse(line.contains(".."), line);
    }

    @Test
    @DisplayName("The digest is reproducible from outside: the line names the algorithm and salt")
    void digestIsReproducible() throws Exception {
        final ByteBuffer value = bytes(32, 1);
        final byte[] expected = MessageDigest.getInstance("SHA-256")
                .digest(value.duplicate().array());

        final List<String> unsalted = render(
                settings(AuditHashAlgorithm.SHA_256, null, 0, List.of(), List.of()),
                recorder -> recorder.requestReceived(CLIENT, ClientOp.PUT, 6L, bytes(4, 1),
                        value.duplicate(), 0, false));
        assertTrue(unsalted.get(0).contains("valueHash=sha-256:" + Hex.encode(expected)),
                unsalted.get(0));
    }

    @Test
    void saltedDigestNamesItsSaltAndMatchesSaltThenBytes() throws Exception {
        final byte[] salt = "pepper".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer value = bytes(32, 1);
        final MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(salt);
        final byte[] expected = md.digest(value.duplicate().array());

        final List<String> lines = render(
                settings(AuditHashAlgorithm.SHA_256, salt, 0x9f21abcd, List.of(), List.of()),
                recorder -> recorder.requestReceived(CLIENT, ClientOp.PUT, 7L, bytes(4, 1),
                        value.duplicate(), 0, false));

        assertTrue(lines.get(0).contains("valueHash=sha-256+salt/9f21abcd:" + Hex.encode(expected)),
                lines.get(0));
    }

    @Test
    @DisplayName("Above the limit nothing is digested, and the line says so")
    void oversizedFieldIsNotDigested() {
        final List<String> lines = render(
                settings(AuditHashAlgorithm.SHA_256, null, 0, List.of(), List.of()),
                recorder -> recorder.requestReceived(CLIENT, ClientOp.PUT, 8L, bytes(4, 1),
                        bytes(65, 0), 0, false));

        assertTrue(lines.get(0).contains("valueHash=skipped"), lines.get(0));
    }

    @Test
    void resultCarriesOutcomeAndVersion() {
        final List<String> lines = render(plain(), recorder ->
                recorder.requestCompleted(CLIENT, ClientOp.CAS, 9L, true, ClientErrorCode.NONE,
                        new Ballot(17L, NodeId.of("n2")), 0));

        final String line = lines.get(0);
        assertTrue(line.contains("RESULT client=web-1 (Jenkins euc1-blue) corr=9 op=CAS ok"), line);
        assertTrue(line.contains("error=NONE"), line);
        assertTrue(line.contains("version=17@n2"), line);
    }
}
