/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;

import java.nio.ByteBuffer;

/**
 * Codec for the {@code TYPE_CLIENT_HELLO} frame payload.
 * <p>
 * Layout: {@code [int32 version][string clientId][string credential]}. {@code version} leads
 * the payload, and must keep that position across every future revision, so a server can read
 * and reject a mismatch before parsing fields whose layout it may not know.
 * <p>
 * {@code credential} is an opaque client-supplied secret (a token, or absent -- encoded with
 * the {@code -1} length every other nullable field on the wire uses). There is no
 * scheme discriminator: the server runs a single configured {@code ClientAuthenticator}
 * (AllowAll / Token / mTLS) and interprets the credential accordingly -- AllowAll ignores
 * it, Token treats it as the shared secret, mTLS takes identity from the certificate CN
 * and ignores it.
 */
public final class ClientHello {
    /**
     * Maximum UTF-8 length of the opaque {@code credential}. Not a key or a value, so it takes
     * no bound from {@link KvLimits}: a shared-secret token an order of magnitude larger than
     * this is a malformed or hostile hello, not a legitimate one.
     */
    public static final int MAX_CREDENTIAL_BYTES = 4096;

    /** Upper bound on an encoded hello payload -- see the layout above. */
    public static final int MAX_PAYLOAD_BYTES =
            Integer.BYTES                                            // version
                    + Integer.BYTES + KvLimits.MAX_CLIENT_ID_BYTES   // putString(clientId)
                    + Integer.BYTES + MAX_CREDENTIAL_BYTES;          // writeNullableString(credential)

    private ClientHello() {
    }

    /** Encode a hello payload (returned buffer is flipped, ready to read/frame). */
    public static ByteBuffer encode(final int version, final ClientId clientId,
                                    final String credential) {
        final MessageBufferWriter out = new MessageBufferWriter(64, MAX_PAYLOAD_BYTES);
        out.writeInt(version);
        out.writeString(clientId.value());
        out.writeNullableString(credential);
        return out.toByteBuffer();
    }

    /**
     * Read only the leading protocol version, leaving the caller's buffer untouched.
     * <p>
     * The rest of the payload's layout may differ across protocol versions, so a receiver that
     * has not yet confirmed the version must not decode past this field.
     */
    public static int peekVersion(final ByteBuffer payload) {
        return new MessageBufferReader(payload.duplicate()).readInt();
    }

    /**
     * Decode a hello payload. Reads from a duplicate so the caller's buffer position is
     * left untouched (the server peeks the version from the same buffer separately).
     *
     * @throws IllegalArgumentException if the payload is malformed
     */
    public static Decoded decode(final ByteBuffer payload) {
        final MessageBufferReader in = new MessageBufferReader(payload.duplicate());
        final int version = in.readInt();
        final ClientId clientId = ClientId.of(in.readString());
        final String credential = in.readNullableString();
        if (in.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after CLIENT_HELLO decode");
        }
        return new Decoded(version, clientId, credential);
    }

    /** A decoded CLIENT_HELLO. */
    public static final class Decoded {
        public final int version;
        public final ClientId clientId;
        public final String credential;

        Decoded(final int version, final ClientId clientId, final String credential) {
            this.version = version;
            this.clientId = clientId;
            this.credential = credential;
        }
    }
}
