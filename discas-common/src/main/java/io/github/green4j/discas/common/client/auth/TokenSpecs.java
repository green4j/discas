/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * The textual token-record grammar shared by the file- and directory-backed
 * {@link ClientTokenStore}s, in both directions.
 * <p>
 * A single record is {@code pbkdf2$<iterations>$<saltB64>$<hashB64>$<notAfterEpochMs>}.
 * A client may hold several records for overlap rotation, written as {@code ;}-separated
 * records on one line.
 * <p>
 * Reading and writing live together on purpose. A grammar with a parser and no writer is a format
 * the system can consume but nothing can produce, which leaves provisioning to whoever assembles
 * the string correctly by hand.
 */
public final class TokenSpecs {

    private static final String PREFIX = "pbkdf2";

    private TokenSpecs() {
    }

    /**
     * One record in the text form a token file holds. The inverse of {@link #parse(String)}.
     *
     * @return {@code pbkdf2$<iterations>$<saltB64>$<hashB64>$<notAfterEpochMs>}
     */
    public static String format(final TokenRecord record) {
        final Base64.Encoder base64 = Base64.getEncoder();
        return PREFIX
                + '$' + record.iterations()
                + '$' + base64.encodeToString(record.salt())
                + '$' + base64.encodeToString(record.hash())
                + '$' + record.notAfterEpochMs();
    }

    /** Parse one {@code ;}-separated line into zero or more records. */
    static List<TokenRecord> parseLine(final String line) {
        final List<TokenRecord> records = new ArrayList<>();
        for (final String spec : line.split(";")) {
            final String trimmed = spec.trim();
            if (!trimmed.isEmpty()) {
                records.add(parse(trimmed));
            }
        }
        return records;
    }

    /** Parse a single {@code pbkdf2$...} record. */
    static TokenRecord parse(final String spec) {
        final String[] parts = spec.split("\\$");
        if (parts.length != 5 || !PREFIX.equals(parts[0])) {
            throw new IllegalArgumentException("Bad token spec '" + spec + "'");
        }
        final int iterations = Integer.parseInt(parts[1]);
        final byte[] salt = Base64.getDecoder().decode(parts[2]);
        final byte[] hash = Base64.getDecoder().decode(parts[3]);
        final long notAfter = Long.parseLong(parts[4]);
        return new TokenRecord(salt, iterations, hash, notAfter);
    }
}
