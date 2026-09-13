/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.common.io.ReloadReport;
import io.github.green4j.discas.common.io.ReloadableFileSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * The audit settings, in one java properties file, re-read on request:
 * <pre>
 *   audit.sink                 = log
 *   audit.buffer-bytes         = 8388608
 *   audit.overflow             = drop
 *   audit.preview.head-bytes   = 16
 *   audit.preview.tail-bytes   = 16
 *   audit.preview.max-fraction = 0.25
 *   audit.hash.algorithm       = sha-256
 *   audit.hash.salt            = a-secret
 *   audit.hash.max-bytes       = 65536
 *   audit.full.prefixes        = lock/ ; meta/
 *   audit.full.max-bytes       = 4096
 *   audit.text.prefixes        = config/
 * </pre>
 * {@code sink} and {@code buffer-bytes} are read once, when the node is built: a reload that
 * changes either is refused, because the ring cannot be resized under a live producer. Everything
 * else takes effect on the next record.
 * <p>
 * The salt never leaves this file. What a log line carries is the first four bytes of its digest,
 * which says which salt reproduces a line without saying what it is.
 */
public final class FileAuditConfig implements AutoCloseable {

    private static final String DEFAULT_SINK = AuditConfigSnapshot.SINK_LOG;
    private static final int DEFAULT_BUFFER_BYTES = 8 * 1024 * 1024;
    private static final int DEFAULT_HEAD_BYTES = 16;
    private static final int DEFAULT_TAIL_BYTES = 16;
    private static final double DEFAULT_MAX_FRACTION = 0.25;
    private static final int DEFAULT_HASH_MAX_BYTES = 64 * 1024;
    private static final int DEFAULT_FULL_MAX_BYTES = 4096;

    private final ReloadableFileSource<AuditConfigSnapshot> source;
    private volatile AuditConfigSnapshot applied;

    public FileAuditConfig(final Path file) {
        this(file, ReloadObserver.NONE);
    }

    public FileAuditConfig(final Path file, final ReloadObserver observer) {
        final Path abs = file.toAbsolutePath();
        this.source = new ReloadableFileSource<>(
                List.of(abs), contents -> accept(parse(abs, contents.get(0))),
                AuditConfigSnapshot::summary, observer);
    }

    public AuditConfigSnapshot snapshot() {
        return source.snapshot();
    }

    public void addListener(final Consumer<AuditConfigSnapshot> listener) {
        source.addListener(listener);
    }

    /** Re-read this file alone and apply what it says. */
    public ReloadReport.Entry reloadNow() {
        return source.reloadNow();
    }

    @Override
    public void close() {
        source.close();
    }

    /**
     * The buffer size this file asks for, read on its own: the node's heap budget is split between
     * the store and the ring before either exists, and this is that number.
     */
    public static int bufferBytesOf(final Path file) {
        try {
            return parse(file, java.nio.file.Files.readAllBytes(file)).bufferBytes();
        } catch (final java.io.IOException e) {
            throw new IllegalArgumentException("Cannot read audit config file " + file, e);
        }
    }

    private AuditConfigSnapshot accept(final AuditConfigSnapshot candidate) {
        final AuditConfigSnapshot current = applied;
        if (current != null) {
            if (!current.sink().equals(candidate.sink())) {
                throw new IllegalArgumentException("Audit sink cannot be changed by a reload: "
                        + current.sink() + " is in force, the file now says " + candidate.sink());
            }
            if (current.bufferBytes() != candidate.bufferBytes()) {
                throw new IllegalArgumentException("Audit buffer size cannot be changed by a "
                        + "reload: " + current.bufferBytes() + " bytes are allocated and the store "
                        + "is sized around them, the file now says " + candidate.bufferBytes());
            }
        }
        applied = candidate;
        return candidate;
    }

    private static AuditConfigSnapshot parse(final Path file, final byte[] bytes) {
        final Properties props = new Properties();
        try {
            props.load(new ByteArrayInputStream(bytes));
        } catch (final Exception e) {
            throw new RuntimeException("Cannot read audit config file " + file, e);
        }
        final String sink = text(props, "audit.sink", DEFAULT_SINK);
        if (!AuditConfigSnapshot.SINK_LOG.equals(sink)) {
            throw new IllegalArgumentException("Unknown audit sink '" + sink + "' in " + file);
        }
        final byte[] salt = saltOf(text(props, "audit.hash.salt", null));
        return new AuditConfigSnapshot(
                sink,
                bufferBytes(number(props, "audit.buffer-bytes", DEFAULT_BUFFER_BYTES)),
                AuditOverflow.fromConfigName(
                        text(props, "audit.overflow", AuditOverflow.DROP.configName())),
                number(props, "audit.preview.head-bytes", DEFAULT_HEAD_BYTES),
                number(props, "audit.preview.tail-bytes", DEFAULT_TAIL_BYTES),
                fraction(props, "audit.preview.max-fraction", DEFAULT_MAX_FRACTION),
                AuditHashAlgorithm.fromConfigName(
                        text(props, "audit.hash.algorithm", AuditHashAlgorithm.NONE.configName())),
                salt,
                saltId(salt),
                number(props, "audit.hash.max-bytes", DEFAULT_HASH_MAX_BYTES),
                number(props, "audit.full.max-bytes", DEFAULT_FULL_MAX_BYTES),
                prefixes(props, "audit.full.prefixes"),
                prefixes(props, "audit.text.prefixes"));
    }

    /** Rounded up to a power of two, which is what the ring's masking needs. */
    private static int bufferBytes(final int requested) {
        if (requested < 1024) {
            throw new IllegalArgumentException("audit.buffer-bytes must be at least 1024, got "
                    + requested);
        }
        return Integer.highestOneBit(requested) == requested
                ? requested
                : Integer.highestOneBit(requested) << 1;
    }

    private static byte[] saltOf(final String salt) {
        return salt == null || salt.isEmpty() ? null : salt.getBytes(StandardCharsets.UTF_8);
    }

    private static int saltId(final byte[] salt) {
        if (salt == null) {
            return 0;
        }
        try {
            final byte[] digest = MessageDigest.getInstance("SHA-256").digest(salt);
            return ((digest[0] & 0xff) << 24) | ((digest[1] & 0xff) << 16)
                    | ((digest[2] & 0xff) << 8) | (digest[3] & 0xff);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required to name an audit salt", e);
        }
    }

    private static List<String> prefixes(final Properties props, final String name) {
        final List<String> parsed = new ArrayList<>();
        final String value = props.getProperty(name);
        if (value == null) {
            return parsed;
        }
        for (final String prefix : value.split(";")) {
            final String trimmed = prefix.trim();
            if (!trimmed.isEmpty()) {
                parsed.add(trimmed);
            }
        }
        return parsed;
    }

    private static String text(final Properties props, final String name, final String fallback) {
        final String value = props.getProperty(name);
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    private static int number(final Properties props, final String name, final int fallback) {
        final String value = text(props, name, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Bad " + name + " '" + value + "'", e);
        }
    }

    private static double fraction(final Properties props, final String name,
                                   final double fallback) {
        final String value = text(props, name, null);
        if (value == null) {
            return fallback;
        }
        final double parsed;
        try {
            parsed = Double.parseDouble(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Bad " + name + " '" + value + "'", e);
        }
        if (parsed <= 0.0 || parsed > 1.0) {
            throw new IllegalArgumentException(name + " must be in (0, 1], got " + parsed);
        }
        return parsed;
    }
}
