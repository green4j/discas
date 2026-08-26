/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.agent.starter.DisCasAgentConfig;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test: a 3-node TCP cluster + a {@link DisCasAgent} in front of it, driven purely over
 * HTTP. In the agent's package so it can reach the package-private nodes-file test hook.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Agent -- KV/lock/health HTTP surface end-to-end")
// One cluster and one agent for the whole file: three FileWal nodes, three peer sockets and an
// agent per test was the most expensive fixture in the suite, and every test below works on keys
// of its own. The one test that takes nodes away runs last, by @Order, for that reason.
class AgentHttpIntegrationTest {

    private static final List<NodeId> NODE_IDS = List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));

    // Static so it is created once, before the class-level @BeforeAll below.
    @TempDir
    static Path baseDir;

    private final List<DisCasNode> nodes = new ArrayList<>();
    private final Map<NodeId, InetSocketAddress> peerAddresses = new LinkedHashMap<>();
    private final Map<NodeId, InetSocketAddress> clientAddresses = new LinkedHashMap<>();

    private DisCasAgent agent;
    private String baseUrl;

    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    void setup() throws Exception {
        final Map<NodeId, ListenSocket> peerSockets = new LinkedHashMap<>();
        final TcpTransportConfig peerConfig = TcpTransportConfig.defaults();
        final ClientTransportConfig clientConfig = ClientTransportConfig.defaults();

        // Phase 1: bind every peer listener at :0 and learn the address each one got.
        // Phase 2 (below) hands each node the members map built from those addresses,
        // which is the map every node validates at construction. Nothing is reserved and
        // then bound later, so no port can be taken in between.
        for (final NodeId nodeId : NODE_IDS) {
            final ListenSocket peerSocket =
                    ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            peerSockets.put(nodeId, peerSocket);
            peerAddresses.put(nodeId, peerSocket.address());
        }

        for (final NodeId nodeId : NODE_IDS) {
            final Path walBase = baseDir.resolve("node-" + nodeId.value());
            Files.createDirectories(walBase);
            final FileWal wal = new FileWal(StorageConfig.builder()
                    .baseDirectory(walBase)
                    .walMaxFileBytes(16 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build());
            wal.initialize();

            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, ClusterId.of("agent-it-cluster"), peerAddresses.size()),
                    new TcpPeerBootstrap(peerSockets.get(nodeId),
                            InMemoryMembers.ofTcp(peerAddresses), peerConfig),
                    wal);

            final TcpClientServerTransport clientServer =
                    DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                            new InetSocketAddress("127.0.0.1", 0), clientConfig));
            clientAddresses.put(nodeId,
                    new InetSocketAddress("127.0.0.1", clientServer.boundPort()));

            nodes.add(node);
        }
        for (final DisCasNode node : nodes) {
            node.start();
        }

        // The request timeout is raised well above the default because it also caps how long a
        // blocking query may park (KvHandler.watchWait). At the default it capped the one test that
        // parks at 9s, which made that test's outcome depend on the machine finishing a write inside
        // a fixed window -- see blockingQueryWakesOnConcurrentWrite.
        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--nodes", nodesSpec(), "--http-bind", "127.0.0.1:0",
                        "--observability-bind", "127.0.0.1:0",
                        "--request-timeout-seconds", "60"}, Map.of());
        agent = DisCasAgent.start(cfg);
        baseUrl = "http://127.0.0.1:" + agent.port();
    }

    @AfterAll
    void tearDown() {
        if (agent != null) {
            agent.close();
        }
        for (final DisCasNode node : nodes) {
            try {
                node.close();
            } catch (final Exception ignored) {
            }
        }
    }

    @Test
    void kvAndLockRoundTripOverHttp() throws Exception {
        awaitClusterReady("__ready_probe__");

        HttpResponse<String> r = put("/v1/kv/foo/bar", "hello");
        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"ok\":true"), r.body());

        // GET as JSON envelope (base64 of "hello" is aGVsbG8=).
        r = get("/v1/kv/foo/bar");
        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"Value\":\"aGVsbG8=\""), r.body());

        r = get("/v1/kv/foo/bar?raw");
        assertEquals(200, r.statusCode());
        assertEquals("hello", r.body());

        r = get("/v1/kv/?keys");
        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"foo/bar\""), r.body());

        // CAS hello -> world, fenced on the cursor the blocking-query read hands out.
        final String helloVersion = version(get("/v1/kv/foo/bar?version=&raw"));
        r = put("/v1/kv/foo/bar?cas=" + helloVersion, "world");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"swapped\":true"), r.body());
        assertEquals("world", get("/v1/kv/foo/bar?raw").body());

        r = delete("/v1/kv/foo/bar", null);
        assertEquals(200, r.statusCode());
        r = get("/v1/kv/foo/bar");
        assertEquals(404, r.statusCode());

        r = put("/v1/lock/job1?ttl=30&owner=w1", "");
        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"acquired\":true"), r.body());
        final String token = extract(r.body(), "\"token\":\"", "\"");
        assertTrue(token.length() > 0, "Expected a hex token in " + r.body());

        r = get("/v1/lock/job1");
        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"status\":\"LOCKED\""), r.body());

        r = delete("/v1/lock/job1", Map.of("X-DisCas-Lock-Token", token));
        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"released\":true"), r.body());

        r = get("/v1/agent/health");
        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"status\":\"ok\""), r.body());
    }

    /**
     * The wait budget is generous on purpose: with a short one, a loaded runner that simply could
     * not apply the write in time fails looking exactly like a missed wake. At {@code wait=30} an
     * unchanged reply means the wake was genuinely missed.
     */
    @Test
    void blockingQueryWakesOnConcurrentWrite() throws Exception {
        awaitClusterReady("__ready_probe__");

        assertEquals(200, put("/v1/kv/watched", "v0").statusCode());

        // Bootstrap the cursor via a wait=0 blocking query: it fires immediately for an existing
        // key. A plain GET would do as well -- every read carries the cursor -- but this is the
        // blocking form's own zero-budget idiom, and this test is about the blocking form.
        final HttpResponse<String> seed = get("/v1/kv/watched?wait=0");
        assertEquals(200, seed.statusCode());
        final String version = seed.headers().firstValue("X-DisCas-Version").orElse(null);
        assertNotNull(version, "A blocking-query read must carry an X-DisCas-Version cursor");

        // A blocking query at the current cursor parks until something changes past it.
        final CompletableFuture<HttpResponse<String>> blocking = http.sendAsync(
                HttpRequest.newBuilder(
                                URI.create(baseUrl + "/v1/kv/watched?version=" + version + "&wait=30"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());

        // Not a race: the cursor makes the outcome the same whether or not the query has parked
        // yet -- a write past the supplied version wakes it, and one that arrived first returns
        // immediately. The pause is only so the parking path is the one usually exercised.
        Thread.sleep(300L);
        assertEquals(200, put("/v1/kv/watched", "v1").statusCode());

        final HttpResponse<String> woken = blocking.get(45, TimeUnit.SECONDS);
        assertEquals(200, woken.statusCode());
        // base64("v1") == "djE=". Still holding v0 means the query parked through a write that had
        // already been acknowledged and came back on its own deadline -- a missed wake, not a slow
        // machine, because the deadline is thirty seconds away from the write.
        assertTrue(woken.body().contains("\"Value\":\"djE=\""),
                "Blocking query did not observe the concurrent write: " + woken.body());
        final String newVersion = woken.headers().firstValue("X-DisCas-Version").orElse(null);
        assertNotNull(newVersion);
        assertNotEquals(version, newVersion, "The cursor must advance after a write");

        // With the new cursor and no writer, a short blocking query times out unchanged: the cursor
        // it hands back equals the one supplied (client detects "no change").
        final HttpResponse<String> unchanged = get("/v1/kv/watched?version=" + newVersion + "&wait=1");
        assertEquals(200, unchanged.statusCode());
        assertEquals(newVersion, unchanged.headers().firstValue("X-DisCas-Version").orElse(null));
    }

    @Test
    void malformedVersionReturns400() throws Exception {
        // A present-but-malformed ?version is a client error, surfaced as a 400 JSON envelope (thrown in
        // the handler before any cluster call) rather than being silently reset to the initial cursor.
        final HttpResponse<String> r = get("/v1/kv/watched?version=nothex&wait=0");
        assertEquals(400, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"error\":"), r.body());
        assertTrue(r.body().contains("version"), r.body());
        // An HTTP-level 400 keeps the connection alive: a follow-up request on the reused client works.
        assertEquals(400, get("/v1/kv/watched?version=alsobad&wait=0").statusCode());
    }

    /**
     * {@code ?stale} downgrades a read to {@link ReadConsistency#SERIALIZABLE}.
     * <p>
     * Losing quorum is what makes the two levels tell themselves apart over HTTP. A linearizable
     * read is a Paxos round, so without a majority it cannot complete at all; a serializable read
     * is answered from the coordinator's own committed state and still works. Anything weaker
     * (comparing values while the cluster is healthy) would pass just as happily if {@code ?stale}
     * were ignored entirely, since both levels return the same bytes when nothing is in flight.
     */
    @Test
    @Order(Integer.MAX_VALUE)   // takes two nodes away: nothing in this file may run after it
    void staleReadIsServedWithoutQuorumWhileAStrictReadIsNot() throws Exception {
        awaitClusterReady("__ready_probe__");
        assertEquals(200, put("/v1/kv/frozen", "v1").statusCode());
        assertEquals("v1", get("/v1/kv/frozen?raw").body(), "Sanity: strict read works with quorum");

        // 1 of 3 left: no majority, so no round can commit.
        nodes.get(1).close();
        nodes.get(2).close();

        final HttpResponse<String> strict = get("/v1/kv/frozen?raw");
        assertNotEquals(200, strict.statusCode(),
                "A linearizable read cannot be served without a quorum, got " + strict.body());

        final HttpResponse<String> stale = get("/v1/kv/frozen?raw&stale");
        assertEquals(200, stale.statusCode(), stale.body());
        assertEquals("v1", stale.body(),
                "A serializable read is answered from committed state and survives quorum loss");

        // The blocking-query path takes the same downgrade: watch polls with a versioned read, so
        // without this it would still be running rounds it cannot complete.
        final HttpResponse<String> staleWatch = get("/v1/kv/frozen?raw&wait=0&stale");
        assertEquals(200, staleWatch.statusCode(), staleWatch.body());
        assertNotNull(staleWatch.headers().firstValue("X-DisCas-Version").orElse(null),
                "A stale watch must still hand back a cursor");
        assertNotEquals(200, get("/v1/kv/frozen?raw&wait=0").statusCode(),
                "The same watch without ?stale cannot be served without a quorum");

        // Locks must not be downgradable the same way: a lock decision taken on a possibly-stale
        // read is a lock two holders can each believe they own. The parameter is ignored there.
        assertNotEquals(200, get("/v1/lock/job1?stale").statusCode(),
                "?stale must not weaken a lock read");
    }

    @Test
    void oversizedBodyIsRejectedWith413() throws Exception {
        awaitClusterReady("__ready_probe__");

        // One byte past the limit. The agent stops copying once it overflows, so this bounds its
        // memory too -- the body is refused, not buffered and then refused.
        final byte[] tooBig = new byte[DisCasClient.MAX_VALUE_BYTES + 1];
        final HttpResponse<String> over = putBytes("/v1/kv/oversized", tooBig);
        assertEquals(413, over.statusCode(), over.body());

        // The connection must survive it: a rejected body cannot poison the keep-alive stream.
        assertEquals(200, put("/v1/kv/after-413", "ok").statusCode());
        assertEquals(404, get("/v1/kv/oversized").statusCode(),
                "A refused body must not have been written");
    }

    /**
     * The round trip is the point: a version this agent handed out must be usable as the next
     * write's precondition, and a stale one must lose without destroying the write that overtook it.
     */
    @Test
    @DisplayName("?cas=<version> fences a write on the X-DisCas-Version cursor it handed out")
    void versionedCasOverHttp() throws Exception {
        awaitClusterReady("__ready_probe__");

        // cas=0 is create-if-absent -- Consul's spelling, and a real version rather than a wildcard.
        final HttpResponse<String> created = put("/v1/kv/vcas/key?cas=0", "v1");
        assertEquals(200, created.statusCode(), created.body());
        assertTrue(created.body().contains("\"swapped\":true"), created.body());
        final String v1Version = version(created);

        // The same key, now present: create-if-absent must lose.
        final HttpResponse<String> recreate = put("/v1/kv/vcas/key?cas=0", "nope");
        assertEquals(200, recreate.statusCode(), recreate.body());
        assertTrue(recreate.body().contains("\"swapped\":false"), recreate.body());
        assertEquals("v1", get("/v1/kv/vcas/key?raw").body());

        // The cursor from the first write is the precondition for the second.
        final HttpResponse<String> updated = put("/v1/kv/vcas/key?cas=" + v1Version, "v2");
        assertEquals(200, updated.statusCode(), updated.body());
        assertTrue(updated.body().contains("\"swapped\":true"), updated.body());
        final String v2Version = version(updated);
        assertNotEquals(v1Version, v2Version, "A committed write must advance the cursor");

        // Replaying the stale cursor is the case the whole design exists for: it must not apply,
        // and it must report what won so the caller can recompute.
        final HttpResponse<String> stale = put("/v1/kv/vcas/key?cas=" + v1Version, "v3");
        assertEquals(200, stale.statusCode(), stale.body());
        assertTrue(stale.body().contains("\"swapped\":false"), stale.body());
        // base64("v2") == "djI="
        assertTrue(stale.body().contains("\"value\":\"djI=\""), stale.body());
        assertEquals("v2", get("/v1/kv/vcas/key?raw").body(), "V2 must survive the stale replay");

        // The fenced delete, same shape.
        final HttpResponse<String> staleDelete = delete("/v1/kv/vcas/key?cas=" + v1Version, null);
        assertEquals(200, staleDelete.statusCode(), staleDelete.body());
        assertTrue(staleDelete.body().contains("\"swapped\":false"), staleDelete.body());
        assertEquals("v2", get("/v1/kv/vcas/key?raw").body(), "A stale delete must not tombstone");

        final HttpResponse<String> deleted = delete("/v1/kv/vcas/key?cas=" + v2Version, null);
        assertEquals(200, deleted.statusCode(), deleted.body());
        assertTrue(deleted.body().contains("\"swapped\":true"), deleted.body());
        assertEquals(404, get("/v1/kv/vcas/key").statusCode());

        // A malformed cursor is the caller's error and must not be silently reset to "no version",
        // which would turn a fenced write into a create-if-absent.
        assertEquals(400, put("/v1/kv/vcas/key?cas=zz", "x").statusCode());

        // A bare ?cas with no version is refused rather than quietly demoted to an unconditional
        // put: the caller asked for a precondition, and there is no value-compared form to fall
        // back on.
        final HttpResponse<String> bareCas = put("/v1/kv/vcas/key?cas", "x");
        assertEquals(400, bareCas.statusCode(), bareCas.body());

    }

    /**
     * The cursor is not a property of the blocking form. Every read already knows the version --
     * it comes back on the wire whether or not anybody asked -- so withholding it from a plain GET
     * would force a second call whose only purpose is to be told something the first one had.
     */
    @Test
    @DisplayName("A plain GET carries the cursor, and it is the one ?cas= accepts")
    void plainGetCarriesTheCursor() throws Exception {
        awaitClusterReady("__ready_probe__");
        assertEquals(200, put("/v1/kv/plain-cursor", "v1").statusCode());

        final HttpResponse<String> read = get("/v1/kv/plain-cursor?raw");
        assertEquals(200, read.statusCode(), read.body());
        final String cursor = version(read);

        // Load-bearing: the cursor from a plain read must fence a write, or it is decoration.
        final HttpResponse<String> swapped = put("/v1/kv/plain-cursor?cas=" + cursor, "v2");
        assertEquals(200, swapped.statusCode(), swapped.body());
        assertTrue(swapped.body().contains("\"swapped\":true"), swapped.body());

        // A miss carries one too, so a caller can watch for a key that does not exist yet.
        assertNotNull(version(get("/v1/kv/never-written")),
                "A 404 must still hand back the cursor to watch from");
    }

    /**
     * An unfenced write commits at a version like any other. Withholding it would mean a writer
     * that wants to fence its next write, or watch what it just wrote, has to read the key back to
     * learn something the write already knew.
     */
    @Test
    @DisplayName("An unfenced PUT and DELETE hand back the version they committed at")
    void unfencedWritesCarryTheirVersion() throws Exception {
        awaitClusterReady("__ready_probe__");

        final HttpResponse<String> written = put("/v1/kv/unfenced", "v1");
        assertEquals(200, written.statusCode(), written.body());
        final String afterPut = version(written);

        // Load-bearing, not decoration: it must be the version a fence accepts.
        assertTrue(put("/v1/kv/unfenced?cas=" + afterPut, "v2").body().contains("\"swapped\":true"));

        final HttpResponse<String> removed = delete("/v1/kv/unfenced", null);
        assertEquals(200, removed.statusCode(), removed.body());
        assertNotEquals(afterPut, version(removed),
                "A tombstone is a commit and advances the cursor");
    }

    /**
     * The blocking query states whether it woke or timed out, rather than leaving the caller to
     * compare cursors -- the same answer {@code WatchResult.changed()} gives in the Java client.
     */
    @Test
    @DisplayName("A blocking query says whether it changed")
    void blockingQueryStatesWhetherItChanged() throws Exception {
        awaitClusterReady("__ready_probe__");
        assertEquals(200, put("/v1/kv/changed-flag", "v0").statusCode());

        // From the initial cursor an existing key has already moved on: changed.
        final HttpResponse<String> fired = get("/v1/kv/changed-flag?version=303a&wait=0");
        assertEquals(200, fired.statusCode(), fired.body());
        assertEquals("true", fired.headers().firstValue("X-DisCas-Changed").orElse(null));

        // From its own current cursor, with nobody writing, the wait simply elapses.
        final HttpResponse<String> quiet =
                get("/v1/kv/changed-flag?version=" + version(fired) + "&wait=1");
        assertEquals(200, quiet.statusCode(), quiet.body());
        assertEquals("false", quiet.headers().firstValue("X-DisCas-Changed").orElse(null));
        assertEquals(version(fired), version(quiet),
                "and the cursor it hands back is the one it was given");
    }

    /**
     * A listing that cannot say it is short is the failure mode {@code ?stale} would otherwise
     * introduce silently, so the completeness of every listing is stated rather than implied.
     */
    @Test
    @DisplayName("A listing states how far it can be trusted")
    void listingStatesItsCompleteness() throws Exception {
        awaitClusterReady("__ready_probe__");
        assertEquals(200, put("/v1/kv/listed/one", "v").statusCode());

        final HttpResponse<String> listed = get("/v1/kv/listed?keys");
        assertEquals(200, listed.statusCode(), listed.body());
        assertEquals("true", listed.headers().firstValue("X-DisCas-Complete").orElse(null),
                "A listing that came back at all under the default coverage reached a quorum");

        final int responded = Integer.parseInt(
                listed.headers().firstValue("X-DisCas-Responded").orElseThrow());
        final int clusterSize = Integer.parseInt(
                listed.headers().firstValue("X-DisCas-Cluster-Size").orElseThrow());
        assertTrue(responded >= clusterSize / 2 + 1,
                "The counts must back the claim: " + responded + " of " + clusterSize);
    }

    private static String version(final HttpResponse<String> response) {
        return response.headers().firstValue("X-DisCas-Version")
                .orElseThrow(() -> new AssertionError("No X-DisCas-Version on " + response.body()));
    }

    @Test
    void unsupportedMethodsAreRejectedWith405() throws Exception {
        awaitClusterReady("__ready_probe__");

        assertEquals(405, method("PATCH", "/v1/kv/foo").statusCode());
        assertEquals(405, method("PATCH", "/v1/lock/foo").statusCode());
        // Health is read-only and is served by a different handler, so it needs its own check.
        assertEquals(405, method("DELETE", "/v1/agent/health").statusCode());
    }

    @Test
    void renewExtendsTheLeaseOverHttp() throws Exception {
        awaitClusterReady("__ready_probe__");

        final HttpResponse<String> acquired = put("/v1/lock/renewable?ttl=1&owner=w1", "");
        assertEquals(200, acquired.statusCode(), acquired.body());
        final String token = extract(acquired.body(), "\"token\":\"", "\"");

        final HttpResponse<String> renewed = putWithHeader(
                "/v1/lock/renewable?renew&ttl=30", "", "X-DisCas-Lock-Token", token);
        assertEquals(200, renewed.statusCode(), renewed.body());
        assertTrue(renewed.body().contains("\"renewed\":true"), renewed.body());

        // Past the original 1s lease. Without the renew this would read EXPIRED, so the status here
        // is what proves the new lease took effect rather than the call merely returning true. The
        // elapsed time is the substance of the assertion, which is why this one is a sleep: there is
        // no condition to wait for, only a deadline to outlive.
        Thread.sleep(1500L);
        final HttpResponse<String> info = get("/v1/lock/renewable");
        assertEquals(200, info.statusCode());
        assertTrue(info.body().contains("\"status\":\"LOCKED\""),
                "The renewed lease must outlive the original ttl: " + info.body());
    }

    @Test
    void renewIsRefusedWithoutAValidToken() throws Exception {
        awaitClusterReady("__ready_probe__");

        final HttpResponse<String> acquired = put("/v1/lock/renew-guard?ttl=30&owner=w1", "");
        assertEquals(200, acquired.statusCode(), acquired.body());
        final String token = extract(acquired.body(), "\"token\":\"", "\"");

        // No token at all is a malformed request; a wrong one is a well-formed request that is
        // simply refused -- different answers, and the agent must not conflate them.
        assertEquals(400, put("/v1/lock/renew-guard?renew&ttl=30", "").statusCode());

        final char flipped = token.charAt(0) == 'a' ? 'b' : 'a';
        final String wrong = flipped + token.substring(1);
        final HttpResponse<String> refused = putWithHeader(
                "/v1/lock/renew-guard?renew&ttl=30", "", "X-DisCas-Lock-Token", wrong);
        assertEquals(200, refused.statusCode(), refused.body());
        assertTrue(refused.body().contains("\"renewed\":false"), refused.body());

        // A bad ttl is rejected before the token is even used.
        assertEquals(400, putWithHeader(
                "/v1/lock/renew-guard?renew&ttl=0", "", "X-DisCas-Lock-Token", token).statusCode());
    }

    @Test
    void acquireRequiresAnOwner() throws Exception {
        awaitClusterReady("__ready_probe__");

        // Rejected rather than filled in with a generated id: over HTTP the response to an acquire
        // is exactly what a dropped connection takes away, and a name the caller never chose is no
        // way back to the lock afterwards.
        final HttpResponse<String> noOwner = put("/v1/lock/needs-owner?ttl=30", "");
        assertEquals(400, noOwner.statusCode(), noOwner.body());
        assertEquals(400, put("/v1/lock/needs-owner?ttl=30&owner=", "").statusCode());
        assertEquals(400, put("/v1/lock/needs-owner?recover", "").statusCode());
    }

    @Test
    void recoverHandsBackTheLockAnAcquireMayHaveTakenSilently() throws Exception {
        awaitClusterReady("__ready_probe__");

        // Stands in for an acquire whose response was lost: the record is committed, and the
        // caller is holding no token.
        final HttpResponse<String> acquired = put("/v1/lock/recoverable?ttl=30&owner=w1", "");
        assertEquals(200, acquired.statusCode(), acquired.body());
        final String token = extract(acquired.body(), "\"token\":\"", "\"");

        final HttpResponse<String> recovered = put("/v1/lock/recoverable?recover&owner=w1", "");
        assertEquals(200, recovered.statusCode(), recovered.body());
        assertTrue(recovered.body().contains("\"acquired\":true"), recovered.body());
        assertEquals(token, extract(recovered.body(), "\"token\":\"", "\""),
                "recovery must return the standing lease, not mint a new one");

        // A second acquire under the same name takes nothing and is told so precisely.
        final HttpResponse<String> again = put("/v1/lock/recoverable?ttl=30&owner=w1", "");
        assertTrue(again.body().contains("\"status\":\"HELD_BY_SELF\""), again.body());
        assertTrue(again.body().contains("\"acquired\":false"), again.body());

        final HttpResponse<String> other = put("/v1/lock/recoverable?recover&owner=w2", "");
        assertTrue(other.body().contains("\"status\":\"HELD_BY_OTHER\""), other.body());

        final HttpResponse<String> nothing = put("/v1/lock/never-acquired?recover&owner=w1", "");
        assertTrue(nothing.body().contains("\"status\":\"NOT_HELD\""), nothing.body());
    }

    @Test
    void aLockReadingNeverCarriesTheHoldersToken() throws Exception {
        awaitClusterReady("__ready_probe__");

        final HttpResponse<String> acquired = put("/v1/lock/private-token?ttl=30&owner=w1", "");
        assertEquals(200, acquired.statusCode(), acquired.body());

        // Anyone may ask these two, so neither may answer with the thing release and renew are
        // conditioned on. The owner and the generation are the readable part; the way back to a
        // token is ?recover, which at least asks the caller to claim the name.
        final HttpResponse<String> info = get("/v1/lock/private-token");
        assertTrue(info.body().contains("\"owner\":\"w1\""), info.body());
        assertFalse(info.body().contains("\"token\""), info.body());

        final HttpResponse<String> refused = put("/v1/lock/private-token?ttl=30&owner=w2", "");
        assertTrue(refused.body().contains("\"status\":\"HELD_BY_OTHER\""), refused.body());
        assertFalse(refused.body().contains("\"token\""), refused.body());
    }

    /**
     * @param probeKey a key the agent's client is authorized to write. A cluster with client
     *                 authorization enabled denies a probe outside the granted prefix, which
     *                 never resolves into readiness.
     */
    private void awaitClusterReady(final String probeKey) throws Exception {
        TestAwait.until("the agent to answer a write to " + probeKey, () -> {
            final HttpResponse<String> response = put("/v1/kv/" + probeKey, "1");
            if (response.statusCode() != 200 || !response.body().contains("\"ok\":true")) {
                throw new IllegalStateException(
                        "Not ready: " + response.statusCode() + " " + response.body());
            }
        });
    }

    private String nodesSpec() {
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (final Map.Entry<NodeId, InetSocketAddress> e : clientAddresses.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append(e.getKey().value()).append('=')
                    .append(e.getValue().getHostString()).append(':').append(e.getValue().getPort());
            first = false;
        }
        return sb.toString();
    }

    private HttpResponse<String> get(final String path) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build());
    }

    private HttpResponse<String> put(final String path, final String body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .PUT(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    private HttpResponse<String> method(final String verb, final String path) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .method(verb, HttpRequest.BodyPublishers.noBody()).build());
    }

    private HttpResponse<String> putBytes(final String path, final byte[] body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body)).build());
    }

    private HttpResponse<String> putWithHeader(final String path, final String body,
                                               final String headerName, final String headerValue)
            throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header(headerName, headerValue)
                .PUT(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    private HttpResponse<String> delete(final String path, final Map<String, String> headers)
            throws Exception {
        final HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path)).DELETE();
        if (headers != null) {
            for (final Map.Entry<String, String> h : headers.entrySet()) {
                b.header(h.getKey(), h.getValue());
            }
        }
        return send(b.build());
    }

    private HttpResponse<String> send(final HttpRequest request) throws Exception {
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String extract(final String s, final String startMarker, final String endMarker) {
        final int i = s.indexOf(startMarker);
        if (i < 0) {
            return "";
        }
        final int from = i + startMarker.length();
        final int j = s.indexOf(endMarker, from);
        return j < 0 ? "" : s.substring(from, j);
    }
}
