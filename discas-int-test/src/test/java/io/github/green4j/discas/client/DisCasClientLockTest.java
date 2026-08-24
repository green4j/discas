/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockAcquireStatus;
import io.github.green4j.discas.client.lock.LockInfoResult;
import io.github.green4j.discas.client.lock.LockInfoStatus;
import io.github.green4j.discas.client.lock.LockToken;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestCluster;

import io.github.green4j.discas.client.lock.LockWriteResult;
import io.github.green4j.discas.client.lock.LockWriteStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CAS client -- distributed lock API")
// One cluster for the whole file: every test below works on a lock key of its own, so a fresh
// three-node cluster per test bought isolation nothing needed and cost twenty-five start-ups.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DisCasClientLockTest {
    private TestCluster cluster;

    @BeforeAll
    void setUp() throws Exception {
        cluster = new TestCluster(3, 5);
        cluster.start();
        cluster.awaitReady();
    }

    @AfterAll
    void tearDown() {
        cluster.close();
    }


    @Nested
    @DisplayName("tryLock")
    class TryLockTests {
        @Test
        @DisplayName("Acquires on an empty key")
        void acquiresOnEmptyKey() throws Exception {
            final DisCasClient client = cluster.client(0);
            final String owner = "test-owner-1";
            final LockAcquireResult result = client
                    .tryLock(TestBytes.utf8("lock-1"), Duration.ofSeconds(2), owner)
                    .get(8, TimeUnit.SECONDS);

            assertTrue(result.acquired());
            assertEquals(LockAcquireStatus.ACQUIRED, result.status());
            assertNotNull(result.lock());
            assertNotNull(result.lock().token());
            assertNotNull(result.lock().lockInfo());
            assertEquals(owner, result.lock().lockInfo().snapshot().ownerId());
        }

        @Test
        @DisplayName("Fails when an active lease is held by another owner")
        void failsWhenLeaseActive() throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);
            final String ownerA = "owner-A";

            final LockAcquireResult first = clientA
                    .tryLock(TestBytes.utf8("lock-2"), Duration.ofSeconds(2), ownerA)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(first.acquired());

            final LockAcquireResult second = clientB
                    .tryLock(TestBytes.utf8("lock-2"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.HELD_BY_OTHER, second.status());
            assertNotNull(second.lockInfo());
            assertEquals(ownerA, second.lockInfo().ownerId());
        }

        @Test
        @DisplayName("Steals the lock after the previous lease expires")
        void stealsAfterLeaseExpiry() throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);
            final String ownerB = "owner-B";

            final LockAcquireResult first = clientA
                    .tryLock(TestBytes.utf8("lock-3"), Duration.ofMillis(150))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(first.acquired());

            Thread.sleep(250L);

            final LockAcquireResult second = clientB
                    .tryLock(TestBytes.utf8("lock-3"), Duration.ofSeconds(1), ownerB)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(second.acquired());
            assertEquals(ownerB, second.lock().lockInfo().snapshot().ownerId());
        }

        @Test
        @DisplayName("Returns NOT_LOCK_RECORD when key already holds a plain value")
        void notLockRecordOnPlainValueKey() throws Exception {
            final DisCasClient client = cluster.client(0);
            client.put(TestBytes.utf8("plain-1"), TestBytes.utf8("plain-value")).get(8, TimeUnit.SECONDS);

            final LockAcquireResult result = client
                    .tryLock(TestBytes.utf8("plain-1"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.NOT_LOCK_RECORD, result.status());
            assertFalse(result.acquired());
        }

        @Test
        @DisplayName("Is not reentrant -- a second tryLock by the same owner fails")
        void notReentrantForSameOwner() throws Exception {
            final DisCasClient client = cluster.client(0);
            final String owner = "alice";

            final LockAcquireResult first = client
                    .tryLock(TestBytes.utf8("lock-reentrancy"), Duration.ofSeconds(2), owner)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(first.acquired());

            final LockAcquireResult second = client
                    .tryLock(TestBytes.utf8("lock-reentrancy"), Duration.ofSeconds(2), owner)
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.HELD_BY_OTHER, second.status());
            assertEquals(owner, second.lockInfo().ownerId());
        }
    }

    @Nested
    @DisplayName("Lock (waits)")
    class LockWithWaitTests {
        @Test
        @DisplayName("Waits then acquires when the previous lease expires inside the wait window")
        void waitsThenAcquires() throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);
            final String ownerB = "owner-B-wait";

            final LockAcquireResult first = clientA
                    .tryLock(TestBytes.utf8("lock-4"), Duration.ofMillis(300))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(first.acquired());

            final LockAcquireResult waited = clientB
                    .lock(TestBytes.utf8("lock-4"), Duration.ofSeconds(1), Duration.ofSeconds(2), ownerB)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(waited.acquired());
            assertEquals(ownerB, waited.lock().lockInfo().snapshot().ownerId());
        }

        @Test
        @DisplayName("Times out when the wait window expires before the holder releases")
        void timesOut() throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);

            final LockAcquireResult first = clientA
                    .tryLock(TestBytes.utf8("lock-5"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(first.acquired());

            final LockAcquireResult timedOut = clientB
                    .lock(TestBytes.utf8("lock-5"), Duration.ofSeconds(1), Duration.ofMillis(150))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.TIMED_OUT, timedOut.status());
        }

        @Test
        @DisplayName("With zero wait timeout returns quickly without sleeping")
        void zeroWaitTimeoutDoesNotSleep() throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);

            final LockAcquireResult held = clientA
                    .tryLock(TestBytes.utf8("lock-zero-wait"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(held.acquired());

            final long beforeNanos = System.nanoTime();
            final LockAcquireResult contended = clientB
                    .lock(TestBytes.utf8("lock-zero-wait"), Duration.ofSeconds(1), Duration.ZERO)
                    .get(8, TimeUnit.SECONDS);
            final long elapsedMs = (System.nanoTime() - beforeNanos) / 1_000_000L;
            assertEquals(LockAcquireStatus.TIMED_OUT, contended.status());
            assertTrue(elapsedMs < 500L,
                    "lock() with zero wait must return quickly, elapsedMs=" + elapsedMs);

            assertEquals(LockWriteStatus.APPLIED,
                    held.lock().release().get(8, TimeUnit.SECONDS).status());
            final LockAcquireResult freshTake = clientB
                    .lock(TestBytes.utf8("lock-zero-wait"), Duration.ofSeconds(1), Duration.ZERO)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(freshTake.acquired(), "Uncontended lock with zero wait must succeed");
        }

        @Test
        @DisplayName("Re-acquirable after release")
        void reacquirableAfterRelease() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockAcquireResult first = client
                    .tryLock(TestBytes.utf8("lock-reacquire"), Duration.ofSeconds(5))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(first.acquired());
            assertEquals(LockWriteStatus.APPLIED,
                    first.lock().release().get(8, TimeUnit.SECONDS).status());

            final LockAcquireResult second = client
                    .tryLock(TestBytes.utf8("lock-reacquire"), Duration.ofSeconds(5))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(second.acquired());
            assertEquals(LockWriteStatus.APPLIED,
                    second.lock().release().get(8, TimeUnit.SECONDS).status());
        }
    }

    @Nested
    @DisplayName("Release")
    class ReleaseTests {
        @Test
        @DisplayName("Via DistributedLock handle clears the lease")
        void viaHandle() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockAcquireResult result = client
                    .tryLock(TestBytes.utf8("lock-6"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(result.acquired());

            final LockWriteResult released = result.lock().release().get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.APPLIED, released.status());

            final LockInfoResult info = client.getLockInfo(TestBytes.utf8("lock-6"))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.UNLOCKED, info.status());
        }

        @Test
        @DisplayName("Fails with a wrong token; correct token still works")
        void failsWithWrongToken() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockAcquireResult result = client
                    .tryLock(TestBytes.utf8("lock-7"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(result.acquired());

            final ByteBuffer view = result.token().bytes();
            final byte[] wrong = new byte[view.remaining()];
            view.get(wrong);
            wrong[0] ^= 0x7F;
            final LockWriteResult wrongRelease = client
                    .release(TestBytes.utf8("lock-7"), new LockToken(wrong))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.HELD_BY_OTHER, wrongRelease.status());

            final LockWriteResult okRelease = result.lock().release().get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.APPLIED, okRelease.status());
        }

        @Test
        @DisplayName("Fails on a key that was never locked")
        void failsOnNeverLockedKey() throws Exception {
            final DisCasClient client = cluster.client(0);
            final byte[] bogusToken = new byte[16];
            for (int i = 0; i < bogusToken.length; i++) {
                bogusToken[i] = (byte) i;
            }
            final LockWriteResult released = client
                    .release(TestBytes.utf8("never-locked"), new LockToken(bogusToken))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.NOT_HELD, released.status());
        }

        @Test
        @DisplayName("Fails when the key holds a non-lock plain value")
        void failsOnNonLockRecordValue() throws Exception {
            final DisCasClient client = cluster.client(0);
            client.put(TestBytes.utf8("plain-3"), TestBytes.utf8("something")).get(8, TimeUnit.SECONDS);

            final byte[] bogusToken = new byte[16];
            final LockWriteResult released = client
                    .release(TestBytes.utf8("plain-3"), new LockToken(bogusToken))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.NOT_LOCK_RECORD, released.status());
        }

        @Test
        @DisplayName("With correct token still swaps after lease has expired")
        void onExpiredLeaseWithCorrectTokenStillSwaps() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockAcquireResult acquired = client
                    .tryLock(TestBytes.utf8("lock-expired-release"), Duration.ofMillis(150))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(acquired.acquired());

            Thread.sleep(250L);

            final LockInfoResult expired = client
                    .getLockInfo(TestBytes.utf8("lock-expired-release")).get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.EXPIRED, expired.status());

            final LockWriteResult released = acquired.lock().release().get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.APPLIED, released.status(),
                    "an expired lease is still this holder's to clean up");

            final LockInfoResult after = client
                    .getLockInfo(TestBytes.utf8("lock-expired-release")).get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.UNLOCKED, after.status());
        }
    }

    @Nested
    @DisplayName("Renew")
    class RenewTests {
        @Test
        @DisplayName("Extends the lease so a contender still sees the holder")
        void extendsLease() throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);
            final String owner = "renew-owner";

            final LockAcquireResult first = clientA
                    .tryLock(TestBytes.utf8("lock-renew-extend"), Duration.ofMillis(150), owner)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(first.acquired());

            assertEquals(LockWriteStatus.APPLIED,
                    first.lock().renew(Duration.ofSeconds(2)).get(8, TimeUnit.SECONDS).status());

            Thread.sleep(250L);

            final LockAcquireResult contended = clientB
                    .tryLock(TestBytes.utf8("lock-renew-extend"), Duration.ofSeconds(1))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.HELD_BY_OTHER, contended.status());
            assertEquals(owner, contended.lockInfo().ownerId());

            assertEquals(LockWriteStatus.APPLIED,
                    first.lock().release().get(8, TimeUnit.SECONDS).status());
        }

        @Test
        @DisplayName("Fails with a wrong token")
        void failsWithWrongToken() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockAcquireResult acquired = client
                    .tryLock(TestBytes.utf8("lock-renew-wrong-token"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(acquired.acquired());

            final ByteBuffer view = acquired.token().bytes();
            final byte[] wrong = new byte[view.remaining()];
            view.get(wrong);
            wrong[0] ^= (byte) 0x55;

            final LockWriteResult renewed = client
                    .renewLock(TestBytes.utf8("lock-renew-wrong-token"), new LockToken(wrong), Duration.ofSeconds(5))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.HELD_BY_OTHER, renewed.status());

            assertEquals(LockWriteStatus.APPLIED,
                    acquired.lock().release().get(8, TimeUnit.SECONDS).status());
        }

        @Test
        @DisplayName("Updates lockInfo().current() but not .snapshot()")
        void updatesCurrentLeaseLocally() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockAcquireResult acquired = client
                    .tryLock(TestBytes.utf8("lock-renew-currentlease"), Duration.ofMillis(500))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(acquired.acquired());

            final long snapshotLease = acquired.lock().lockInfo().snapshot().leaseUntilEpochMs();

            assertEquals(LockWriteStatus.APPLIED,
                    acquired.lock().renew(Duration.ofSeconds(10)).get(8, TimeUnit.SECONDS).status());

            final long currentLease = acquired.lock().lockInfo().current().leaseUntilEpochMs();
            final long snapshotLeaseAfter = acquired.lock().lockInfo().snapshot().leaseUntilEpochMs();

            // snapshot is immutable
            assertEquals(snapshotLease, snapshotLeaseAfter);
            // current advanced past snapshot by approximately the new TTL
            assertTrue(currentLease > snapshotLease + 5_000L,
                    "currentLease should be ~10s past the original 500ms lease");

            assertEquals(LockWriteStatus.APPLIED,
                    acquired.lock().release().get(8, TimeUnit.SECONDS).status());
        }

        @Test
        @DisplayName("Fails after the lock has been released")
        void failsAfterRelease() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockAcquireResult acquired = client
                    .tryLock(TestBytes.utf8("lock-renew-after-release"), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(acquired.acquired());
            final LockToken token = acquired.lock().token();

            assertEquals(LockWriteStatus.APPLIED,
                    acquired.lock().release().get(8, TimeUnit.SECONDS).status());

            final LockWriteResult renewed = client
                    .renewLock(TestBytes.utf8("lock-renew-after-release"), token, Duration.ofSeconds(5))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.NOT_HELD, renewed.status(),
                    "a released key holds a marker, not a lock -- the token is beside the point");
        }
    }

    @Nested
    @DisplayName("Fencing -- a superseded token")
    class FencingTests {

        /**
         * The scenario the token exists for: A's lease expires, B steals the lock, and A -- which
         * still believes it holds it -- carries on. A's token was genuinely valid once, which is
         * what separates this from the wrong-token cases above; nothing about it looks malformed.
         * If it still worked, A and B would both believe they held the lock, and A would be able to
         * unlock B.
         * <p>
         * The mechanism is that release and renew both compare against the record as it is *now*,
         * then CAS on that exact value -- so a steal either loses the token comparison or, if it
         * lands in between, invalidates the CAS.
         */
        @ParameterizedTest(name = "{0}")
        @CsvSource({"release, fence-1", "renew, fence-2"})
        @DisplayName("A token superseded by a steal can neither release nor renew the new lease")
        void supersededTokenCannotAct(final String action, final String key) throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);

            final LockAcquireResult a = clientA
                    .tryLock(TestBytes.utf8(key), Duration.ofMillis(150), "owner-A")
                    .get(8, TimeUnit.SECONDS);
            assertTrue(a.acquired());
            final LockToken aToken = a.token();

            Thread.sleep(250L);
            final LockAcquireResult b = clientB
                    .tryLock(TestBytes.utf8(key), Duration.ofSeconds(30), "owner-B")
                    .get(8, TimeUnit.SECONDS);
            assertTrue(b.acquired(), "B must be able to steal an expired lease");

            // A stray renew would be worse than a stray release: A would extend a lease it does not
            // hold, and B's ownership would silently survive under A's clock.
            final LockWriteResult attempt = "release".equals(action)
                    ? clientA.release(TestBytes.utf8(key), aToken).get(8, TimeUnit.SECONDS)
                    : clientA.renewLock(TestBytes.utf8(key), aToken, Duration.ofSeconds(30))
                            .get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.HELD_BY_OTHER, attempt.status(),
                    "A's superseded token must not act on the lock B now holds");
            assertEquals("owner-B", attempt.observed().ownerId(),
                    "and the refusal must name who displaced it, without a second read");

            // The failed attempt must also leave B's lease intact, not merely be reported as failed.
            final LockInfoResult info =
                    clientB.getLockInfo(TestBytes.utf8(key)).get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.LOCKED, info.status());
            assertEquals("owner-B", info.info().ownerId());
        }

        /**
         * The same token, one step further on: B releases, so the lock is free again. A's token is
         * now not merely superseded but pointing at a generation that has been closed out. It must
         * not re-lock the key by releasing into it.
         */
        @Test
        @DisplayName("Cannot act on a lock that was stolen and then released")
        void supersededTokenCannotActAfterFullCycle() throws Exception {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);

            final LockAcquireResult a = clientA
                    .tryLock(TestBytes.utf8("fence-3"), Duration.ofMillis(150), "owner-A")
                    .get(8, TimeUnit.SECONDS);
            assertTrue(a.acquired());
            final LockToken aToken = a.token();

            Thread.sleep(250L);
            final LockAcquireResult b = clientB
                    .tryLock(TestBytes.utf8("fence-3"), Duration.ofSeconds(30), "owner-B")
                    .get(8, TimeUnit.SECONDS);
            assertTrue(b.acquired());
            assertEquals(LockWriteStatus.APPLIED,
                    b.lock().release().get(8, TimeUnit.SECONDS).status());

            assertEquals(LockWriteStatus.NOT_HELD,
                    clientA.release(TestBytes.utf8("fence-3"), aToken).get(8, TimeUnit.SECONDS).status());
            assertEquals(LockWriteStatus.NOT_HELD,
                    clientA.renewLock(TestBytes.utf8("fence-3"), aToken, Duration.ofSeconds(30))
                            .get(8, TimeUnit.SECONDS).status());

            // Still free -- A's attempts must not have resurrected a lease in its own name.
            final LockInfoResult info =
                    clientA.getLockInfo(TestBytes.utf8("fence-3")).get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.UNLOCKED, info.status());
        }

        /**
         * Not a fault scenario: the token's own arithmetic. Each generation of a lock must carry a
         * strictly higher token than the one it displaced, and a handle from a displaced
         * generation must know it is stale. Monotonicity <em>under load</em> is a chaos case.
         */
        @Test
        @DisplayName("The fencing token strictly increases across acquire / release / re-acquire")
        void fencingTokenMonotonic() throws Exception {
            final String key = "fence-monotonic";
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);

            final LockAcquireResult first = clientA
                    .lock(key, Duration.ofSeconds(20), Duration.ofSeconds(20), "owner-A")
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.ACQUIRED, first.status());
            final long firstToken = first.lock().fencingToken();
            assertTrue(firstToken >= 1L, "First acquire must produce a positive fencing token");
            assertTrue(first.lock().validate().get(8, TimeUnit.SECONDS),
                    "validate() must succeed while the lock is still held by us");

            assertEquals(LockWriteStatus.APPLIED,
                    first.lock().release().get(8, TimeUnit.SECONDS).status(),
                    "release() must succeed for the token we hold");

            final LockAcquireResult second = clientB
                    .lock(key, Duration.ofSeconds(20), Duration.ofSeconds(20), "owner-B")
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.ACQUIRED, second.status());
            assertTrue(second.lock().fencingToken() > firstToken,
                    "Second acquire's token must strictly exceed the first");

            // The first handle is still held in memory by A; it must know it has been displaced.
            assertFalse(first.lock().validate().get(8, TimeUnit.SECONDS),
                    "validate() on a stale lock handle must report invalid");

            second.lock().release().get(8, TimeUnit.SECONDS);
        }
    }

    @Nested
    @DisplayName("getLockInfo")
    class GetLockInfoTests {
        @Test
        @DisplayName("Classifies UNLOCKED, LOCKED, EXPIRED and NOT_LOCK_RECORD")
        void classifiesStatuses() throws Exception {
            final DisCasClient client = cluster.client(0);

            final LockInfoResult unlocked = client.getLockInfo(TestBytes.utf8("lock-8"))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.UNLOCKED, unlocked.status());

            final LockAcquireResult acquired = client
                    .tryLock(TestBytes.utf8("lock-8"), Duration.ofMillis(200))
                    .get(8, TimeUnit.SECONDS);
            assertTrue(acquired.acquired());

            final LockInfoResult locked = client.getLockInfo(TestBytes.utf8("lock-8"))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.LOCKED, locked.status());

            Thread.sleep(300L);
            final LockInfoResult expired = client.getLockInfo(TestBytes.utf8("lock-8"))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.EXPIRED, expired.status());

            client.put(TestBytes.utf8("plain-value-key"), TestBytes.utf8("hello")).get(8, TimeUnit.SECONDS);
            final LockInfoResult nonLock = client.getLockInfo(TestBytes.utf8("plain-value-key"))
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.NOT_LOCK_RECORD, nonLock.status());
        }

        @Test
        @DisplayName("Via DistributedLock.info() delegates to the same protocol")
        void viaDistributedLockHandle() throws Exception {
            final DisCasClient client = cluster.client(0);
            final String owner = "info-owner";

            final LockAcquireResult result = client
                    .tryLock(TestBytes.utf8("lock-9"), Duration.ofSeconds(2), owner)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(result.acquired());

            final LockInfoResult info = result.lock().info().get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.LOCKED, info.status());
            assertEquals(owner, info.info().ownerId());
        }
    }

    @Nested
    @DisplayName("Contention and key-encoding overloads")
    class ContentionAndOverloads {
        @Test
        @DisplayName("Concurrent tryLock from 5 clients elects exactly one winner")
        void concurrentTryLockHasExactlyOneWinner() throws Exception {
            final int contenders = 5;
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(contenders);
            final List<LockAcquireResult> results = new ArrayList<>();
            final ExecutorService pool = Executors.newFixedThreadPool(contenders);

            for (int i = 0; i < contenders; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        final LockAcquireResult r = cluster.client(idx)
                                .tryLock(TestBytes.utf8("lock-contention"), Duration.ofSeconds(2),
                                        "owner-" + idx)
                                .get(8, TimeUnit.SECONDS);
                        synchronized (results) {
                            results.add(r);
                        }
                    } catch (final Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(15, TimeUnit.SECONDS));
            pool.shutdownNow();

            assertEquals(contenders, results.size());
            int winners = 0;
            String winnerOwner = null;
            for (final LockAcquireResult r : results) {
                if (r.status() == LockAcquireStatus.ACQUIRED) {
                    winners++;
                    winnerOwner = r.lock().lockInfo().snapshot().ownerId();
                }
            }
            assertEquals(1, winners, "Exactly one client must win the lock");
            assertNotNull(winnerOwner);
            for (final LockAcquireResult r : results) {
                if (r.status() == LockAcquireStatus.HELD_BY_OTHER) {
                    assertEquals(winnerOwner, r.lockInfo().ownerId(),
                            "Losers must observe the winner's ownerId");
                }
            }
        }

        @Test
        @DisplayName("String-key overloads route to the same lock as ByteBuffer overloads")
        void stringKeyOverloadsRouteToSameLock() throws Exception {
            final DisCasClient client = cluster.client(0);
            final String key = "shared-key";
            final String owner = "owner-string";

            final LockAcquireResult acquired = client
                    .tryLock(key, Duration.ofSeconds(2), owner)
                    .get(8, TimeUnit.SECONDS);
            assertTrue(acquired.acquired());

            final ByteBuffer bufKey = ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
            final LockInfoResult viaBuffer = client.getLockInfo(bufKey)
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.LOCKED, viaBuffer.status());
            assertEquals(owner, viaBuffer.info().ownerId());

            final LockInfoResult viaString = client.getLockInfo(key)
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.LOCKED, viaString.status());
            assertEquals(owner, viaString.info().ownerId());

            final LockWriteResult released = client.release(key, acquired.lock().token())
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.APPLIED, released.status());

            final LockInfoResult finalState = client.getLockInfo(key)
                    .get(8, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.UNLOCKED, finalState.status());
        }
    }
}
