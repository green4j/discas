/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * The three cluster operations a held {@link Lock} needs, narrowed to exactly those.
 * <p>
 * This is what {@link DistributedLock} is constructed with instead of the whole client: a lock
 * handed to application code can then only release, renew and read its own key, and cannot be
 * used as a back door to arbitrary KV access.
 */
public interface LockClientOps {
    /** CAS a release marker onto {@code key}, conditioned on {@code token} still being the holder. */
    CompletableFuture<LockWriteResult> release(ByteBuffer key, LockToken token);

    /** CAS a later lease end onto {@code key}, conditioned on {@code token} still being the holder. */
    CompletableFuture<LockWriteResult> renewLock(ByteBuffer key, LockToken token, Duration newLeaseTtl);

    /** Read {@code key}'s current lock state from the cluster. */
    CompletableFuture<LockInfoResult> getLockInfo(ByteBuffer key);
}
