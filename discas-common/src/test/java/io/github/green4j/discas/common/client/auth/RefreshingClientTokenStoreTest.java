/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("RefreshingClientTokenStore -- fetch-backed refresh, replay, change-gate")
class RefreshingClientTokenStoreTest {

    private static final ClientId WEB = ClientId.of("web-1");
    private static final long FAR_FUTURE = Long.MAX_VALUE;

    private static ClientTokens tokens(final String token) {
        return InMemoryClientTokenStore.builder().add(WEB, token, FAR_FUTURE).build().snapshot();
    }

    private static boolean accepts(final ClientTokenStore store, final String token) {
        return new TokenClientAuthenticator(store, () -> 0L).authenticate(WEB, token).authenticated();
    }

    @Test
    @DisplayName("Serves the initial fetch, then a forced refresh picks up a changed source")
    void refreshPicksUpChange() {
        final AtomicReference<ClientTokens> source = new AtomicReference<>(tokens("t1"));
        try (RefreshingClientTokenStore store =
                     new RefreshingClientTokenStore(source::get, Duration.ofSeconds(30))) {
            assertTrue(accepts(store, "t1"));

            source.set(tokens("t2"));
            store.reloadNow();

            assertTrue(accepts(store, "t2"), "New token accepted after refresh");
            assertFalse(accepts(store, "t1"), "Old token no longer in the source");
        }
    }

    @Test
    @DisplayName("Subscribe replays the current value, then a change notifies once")
    void replayThenNotify() {
        final AtomicReference<ClientTokens> source = new AtomicReference<>(tokens("t1"));
        try (RefreshingClientTokenStore store =
                     new RefreshingClientTokenStore(source::get, Duration.ofSeconds(30))) {
            final AtomicInteger count = new AtomicInteger();
            store.addListener(s -> count.incrementAndGet());
            assertEquals(1, count.get(), "Replay-on-subscribe delivers the current value");

            source.set(tokens("t2"));
            store.reloadNow();
            assertEquals(2, count.get(), "One further notify for the change");

            store.reloadNow(); // no change -> value gate, no notify
            assertEquals(2, count.get());
        }
    }
}
