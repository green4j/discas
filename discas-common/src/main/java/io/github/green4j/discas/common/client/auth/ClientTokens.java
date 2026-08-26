/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client.auth;

import io.github.green4j.discas.common.identity.ClientId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An immutable point-in-time view of provisioned client tokens, keyed by {@link ClientId}
 * (each client may have several records for overlap rotation). Value-based
 * {@link #equals(Object)} lets a file-backed store skip re-publishing an unchanged reload.
 * The token analogue of {@code MembersSnapshot}.
 */
public final class ClientTokens {

    private final Map<ClientId, List<TokenRecord>> byClient;

    public ClientTokens(final Map<ClientId, List<TokenRecord>> byClient) {
        final Map<ClientId, List<TokenRecord>> copy = new LinkedHashMap<>();
        for (final Map.Entry<ClientId, List<TokenRecord>> e : byClient.entrySet()) {
            copy.put(e.getKey(), List.copyOf(e.getValue()));
        }
        this.byClient = Collections.unmodifiableMap(copy);
    }

    /** The records for {@code clientId}, or an empty list if none. */
    public List<TokenRecord> records(final ClientId clientId) {
        final List<TokenRecord> records = byClient.get(clientId);
        return records == null ? List.of() : records;
    }

    /**
     * One line for the reload report: which clients have credentials, how many each, and when the
     * last one of each runs out. Sorted by client, so two reports can be compared.
     *
     * <p><b>Never the credential.</b> A token file is the secret itself -- salt, hash, iterations --
     * and a log is read by more people than the file is. What is here is what an operator needs to
     * confirm a rotation landed: the client is present, it now has two records rather than one, and
     * the far one expires when they expect. Anyone who can already see the log learns nothing that
     * lets them authenticate as anybody.
     */
    public String summary() {
        if (byClient.isEmpty()) {
            return "no clients: every client is refused";
        }
        final List<ClientId> ids = new ArrayList<>(byClient.keySet());
        Collections.sort(ids);
        final StringBuilder sb = new StringBuilder();
        sb.append(ids.size()).append(ids.size() == 1 ? " client: " : " clients: ");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            final List<TokenRecord> records = byClient.get(ids.get(i));
            sb.append(ids.get(i).value())
                    .append(" (").append(records.size())
                    .append(records.size() == 1 ? " token, " : " tokens, ")
                    .append("last expires ").append(lastExpiry(records)).append(')');
        }
        return sb.toString();
    }

    /** The furthest expiry among {@code records}: the moment this client stops being able to connect. */
    private static String lastExpiry(final List<TokenRecord> records) {
        long latest = Long.MIN_VALUE;
        for (final TokenRecord record : records) {
            latest = Math.max(latest, record.notAfterEpochMs());
        }
        return records.isEmpty() ? "never" : Instant.ofEpochMilli(latest).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientTokens)) {
            return false;
        }
        return byClient.equals(((ClientTokens) o).byClient);
    }

    @Override
    public int hashCode() {
        return byClient.hashCode();
    }
}
