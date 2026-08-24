/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.transport.ClientTransport;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How the client reacts to a node that <em>answers</em> with a failure, as opposed to timing out.
 * <p>
 * Two codes may be retried elsewhere, and they share a shape: both are <em>node-local</em> and
 * both are refused cheaply rather than after a round is run to exhaustion.
 * {@link ClientErrorCode#NOT_READY} says that node is still replaying its log;
 * {@link ClientErrorCode#NO_QUORUM_AT_COORDINATOR} says that node cannot see a majority, which
 * under an asymmetric partition tells you nothing about the coordinator standing next to it.
 * Without this, a node answering promptly would be <em>worse</em> than one staying silent --
 * silence at least failed over after the per-attempt timeout, whereas an immediate error would
 * fail the whole operation.
 * <p>
 * {@link ClientErrorCode#UNAVAILABLE} stays in the no-failover set: it covers the
 * failures that say nothing about connectivity, which are as likely on the next node and cost a
 * whole round timeout each to discover. It now carries a second meaning that matters more --
 * <em>the outcome is unknown</em> -- so re-sending an unfenced write on it would be the
 * duplicate-apply hazard rather than merely wasteful.
 * <p>
 * A <b>version-fenced</b> write is the one exception, and the tests below pin both halves of it:
 * a fenced CAS keeps walking coordinators on {@code UNAVAILABLE} because a stale duplicate is
 * provably rejected, while an unfenced write stops and reports.
 * <p>
 * A fake transport is used so the responses are exact and no timer has to elapse: every assertion
 * here completes in milliseconds, and a regression to timeout-driven failover would blow the
 * (deliberately short) waits.
 */
@DisplayName("DisCasClient -- failover on a retryable node failure")
class UnavailableFailoverTest {

    private static final ClientId CLIENT = ClientId.of("c1");
    private static final List<NodeId> PEERS =
            List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));

    private DisCasClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    /** Answers each request according to {@code responder}, recording which peer was addressed. */
    private static final class ScriptedTransport implements ClientTransport {
        private final List<NodeId> addressed = new ArrayList<>();
        private final BiFunction<NodeId, ClientMessage, ClientMessage> responder;
        private Consumer<ClientMessage> handler;

        ScriptedTransport(final BiFunction<NodeId, ClientMessage, ClientMessage> responder) {
            this.responder = responder;
        }

        @Override
        public void send(final NodeId target, final ClientMessage message) {
            addressed.add(target);
            final ClientMessage reply = responder.apply(target, message);
            if (reply != null && handler != null) {
                handler.accept(reply);
            }
        }

        @Override
        public void register(final Consumer<ClientMessage> h) {
            this.handler = h;
        }

        @Override
        public List<NodeId> peers() {
            return PEERS;
        }

        @Override
        public int clusterSize() {
            return PEERS.size();
        }
    }

    @Test
    @DisplayName("NOT_READY moves to the next peer immediately and succeeds there")
    void notReadyFailsOverAndSucceeds() throws Exception {
        // Whichever peer is addressed first is still recovering; the next one serves the read.
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            final ClientMessage.ClientGetReq get = (ClientMessage.ClientGetReq) message;
            if (sends.getAndIncrement() == 0) {
                return new ClientMessage.ClientGetResp(target.value(), get.correlationId(),
                        false, null, "recovering", ClientErrorCode.NOT_READY);
            }
            return new ClientMessage.ClientGetResp(target.value(), get.correlationId(),
                    true, ByteBuffer.wrap("v".getBytes()), null, ClientErrorCode.NONE);
        });
        client = new DisCasClient(CLIENT, transport);

        // Completes well inside a single 5s per-attempt timeout: failover was immediate, not
        // driven by the retry timer.
        final ByteBuffer value = client.get(TestBytes.utf8("k")).get(2, TimeUnit.SECONDS).value();

        assertEquals("v", new String(toBytes(value)));
        assertTrue(transport.addressed.size() >= 2,
                "Expected a second peer to be addressed, saw " + transport.addressed);
    }

    @Test
    @DisplayName("NO_QUORUM_AT_COORDINATOR moves to the next peer -- the partition may be one-sided")
    void noQuorumAtCoordinatorFailsOverAndSucceeds() throws Exception {
        // The first coordinator sits on the minority side of an asymmetric partition. The client
        // can still reach the healthy majority through someone else, so it must not give up here.
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
            if (sends.getAndIncrement() == 0) {
                return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                        false, "cannot see a majority", ClientErrorCode.NO_QUORUM_AT_COORDINATOR);
            }
            return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                    true, null, ClientErrorCode.NONE);
        });
        client = new DisCasClient(CLIENT, transport);

        // Inside a single per-attempt timeout: the switch is driven by the answer, not the timer.
        client.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(2, TimeUnit.SECONDS);

        assertTrue(transport.addressed.size() >= 2,
                "Expected a second coordinator to be addressed, saw " + transport.addressed);
    }

    @Test
    @DisplayName("A version-fenced write does fail over on UNAVAILABLE -- a duplicate cannot apply")
    void fencedWriteFailsOverOnUnavailable() throws Exception {
        // The general rule keeps UNAVAILABLE out of the failover set because the outcome is
        // indeterminate: re-sending an unfenced write there is the duplicate-apply hazard. A
        // fenced write is the exception for a reason rather than by exemption -- a stale attempt
        // carries a ballot the register has overtaken, so a duplicate is provably a no-op.
        final Ballot fence = new Ballot(4L, NodeId.of("2"));
        final AtomicInteger sends = new AtomicInteger();
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            final ClientMessage.ClientCasReq cas =
                    (ClientMessage.ClientCasReq) message;
            if (sends.getAndIncrement() == 0) {
                return new ClientMessage.ClientCasResp(target.value(),
                        cas.correlationId(), false, false, null, Ballot.ZERO,
                        "accept stalled", ClientErrorCode.UNAVAILABLE);
            }
            return new ClientMessage.ClientCasResp(target.value(), cas.correlationId(),
                    true, true, ByteBuffer.wrap("v".getBytes()), new Ballot(5L, NodeId.of("3")),
                    null, ClientErrorCode.NONE);
        });
        client = new DisCasClient(CLIENT, transport);

        final CasResult result = client
                .cas(TestBytes.utf8("k"), new Version(fence), TestBytes.utf8("v"))
                .get(2, TimeUnit.SECONDS);

        assertTrue(result.swapped());
        assertTrue(transport.addressed.size() >= 2,
                "A fenced write must keep walking coordinators, saw " + transport.addressed);
    }

    static Stream<Arguments> nonFailoverCodes() {
        return Stream.of(
                // Indeterminate: re-sending an unfenced write is the duplicate-apply hazard, which
                // is exactly what the fenced case above is allowed to do and this one is not.
                Arguments.of("an unfenced write on UNAVAILABLE", true, ClientErrorCode.UNAVAILABLE, false),
                // Determinate (nothing was accepted), but another coordinator adds a competitor for
                // the same register rather than removing one.
                Arguments.of("a write on BALLOT_LOST", true, ClientErrorCode.BALLOT_LOST, false),
                // A policy refusal repeats on every node.
                Arguments.of("a read on ACCESS_DENIED", false, ClientErrorCode.ACCESS_DENIED, true),
                Arguments.of("a read on INVALID_ARGUMENT", false, ClientErrorCode.INVALID_ARGUMENT, true),
                // No quorum is cluster-wide, and each extra attempt costs a full proposer round
                // timeout on the node side: report after one, do not multiply by N.
                Arguments.of("a read on UNAVAILABLE", false, ClientErrorCode.UNAVAILABLE, false));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonFailoverCodes")
    @DisplayName("A code outside the failover set is reported after a single attempt")
    void codeIsNotFailedOver(final String name, final boolean write,
                             final ClientErrorCode code, final boolean callerError) {
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            if (message instanceof ClientMessage.ClientPutReq) {
                final ClientMessage.ClientPutReq put = (ClientMessage.ClientPutReq) message;
                return new ClientMessage.ClientPutResp(target.value(), put.correlationId(),
                        false, "refused", code);
            }
            final ClientMessage.ClientGetReq get = (ClientMessage.ClientGetReq) message;
            return new ClientMessage.ClientGetResp(target.value(), get.correlationId(),
                    false, null, "refused", code);
        });
        client = new DisCasClient(CLIENT, transport);

        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> (write
                        ? client.put(TestBytes.utf8("k"), TestBytes.utf8("v"))
                        : client.get(TestBytes.utf8("k"))).get(2, TimeUnit.SECONDS));

        final DisCasOperationException cause =
                assertInstanceOf(DisCasOperationException.class, thrown.getCause());
        assertEquals(code, cause.code());
        assertEquals(callerError, cause.isCallerError());
        assertEquals(1, transport.addressed.size(),
                name + " must be reported after one attempt, saw " + transport.addressed);
    }

    @Test
    @DisplayName("Every peer NOT_READY reports the last node's verdict, not an endless retry")
    void allPeersNotReadyReportsTheFailure() {
        final ScriptedTransport transport = new ScriptedTransport((target, message) -> {
            final ClientMessage.ClientGetReq get = (ClientMessage.ClientGetReq) message;
            return new ClientMessage.ClientGetResp(target.value(), get.correlationId(),
                    false, null, "recovering", ClientErrorCode.NOT_READY);
        });
        client = new DisCasClient(CLIENT, transport);

        final ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> client.get(TestBytes.utf8("k")).get(2, TimeUnit.SECONDS));

        assertEquals(ClientErrorCode.NOT_READY,
                assertInstanceOf(DisCasOperationException.class, thrown.getCause()).code());
        assertEquals(PEERS.size(), transport.addressed.size(),
                "Each peer tried exactly once before giving up");
    }

    private static byte[] toBytes(final ByteBuffer buffer) {
        final byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return out;
    }
}
