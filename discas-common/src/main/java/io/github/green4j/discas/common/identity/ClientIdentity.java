/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.identity;

import java.nio.charset.StandardCharsets;

/**
 * Everything a node knows about who is on the other end of a client connection: the trusted
 * {@link ClientId} bound at CLIENT_HELLO and the optional {@link ClientDescription} presented with
 * it. Created once per connection and carried with every request that arrives on it.
 * <p>
 * Both fields are nullable. A {@code null} id is a connection nothing authenticated -- the
 * in-process transports and the AllowAll path -- and authorization treats it exactly as it treated
 * a {@code null} {@link ClientId} before.
 * <p>
 * The label is rendered and encoded in the constructor rather than per use. An audit record copies
 * it into the ring buffer on the event loop, and encoding a string there would allocate once per
 * request; here it costs one small array per connection.
 */
public final class ClientIdentity {

    /** A connection whose identity nothing established. */
    public static final ClientIdentity UNAUTHENTICATED = new ClientIdentity(null, null);

    private final ClientId id;
    private final ClientDescription description;
    private final String label;
    private final byte[] labelBytes;

    private ClientIdentity(final ClientId id, final ClientDescription description) {
        this.id = id;
        this.description = description;
        this.label = render(id, description);
        this.labelBytes = label.getBytes(StandardCharsets.UTF_8);
    }

    public static ClientIdentity of(final ClientId id) {
        return of(id, null);
    }

    public static ClientIdentity of(final ClientId id, final ClientDescription description) {
        if (id == null && description == null) {
            return UNAUTHENTICATED;
        }
        return new ClientIdentity(id, description);
    }

    public ClientId id() {
        return id;
    }

    public ClientDescription description() {
        return description;
    }

    /** {@code clientId}, or {@code clientId (description)} when one was presented. */
    public String label() {
        return label;
    }

    /** UTF-8 length of {@link #label()}. */
    public int labelLength() {
        return labelBytes.length;
    }

    /**
     * Copies the UTF-8 of {@link #label()} into {@code dst} at {@code offset}.
     * <p>
     * A copy rather than the array, so the encoding cannot be mutated by a caller that holds it,
     * and so the audit ring writes straight into its own storage.
     *
     * @return the number of bytes written
     */
    public int copyLabelTo(final byte[] dst, final int offset) {
        System.arraycopy(labelBytes, 0, dst, offset, labelBytes.length);
        return labelBytes.length;
    }

    private static String render(final ClientId id, final ClientDescription description) {
        final String name = id == null ? "(unauthenticated)" : id.value();
        return description == null ? name : name + " (" + description.value() + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClientIdentity other = (ClientIdentity) o;
        return (id == null ? other.id == null : id.equals(other.id))
                && (description == null ? other.description == null
                        : description.equals(other.description));
    }

    @Override
    public int hashCode() {
        return 31 * (id == null ? 0 : id.hashCode()) + (description == null ? 0 : description.hashCode());
    }

    @Override
    public String toString() {
        return label;
    }
}
