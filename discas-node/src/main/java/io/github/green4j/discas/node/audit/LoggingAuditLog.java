/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.Hex;
import io.github.green4j.discas.common.logging.Log;

/**
 * The default sink: one line per event through the node's {@link Log}, rendered into a builder it
 * reuses, so a trail costs no garbage.
 * <p>
 * A value is never printed whole unless a prefix rule says it may be. A line carries its size, its
 * head and its tail, and -- when digests are on -- a digest naming its own algorithm and salt, so
 * a reader holding a candidate value can check it against the line.
 */
public final class LoggingAuditLog implements AuditLog {

    private final Log log;
    private final StringBuilder line = new StringBuilder(256);

    public LoggingAuditLog(final Log log) {
        this.log = log;
    }

    @Override
    public void record(final AuditEvent event) {
        final AuditEventKind kind = event.kind();
        line.setLength(0);
        line.append("AUDIT ").append(kind);
        switch (kind) {
            case RECORDS_LOST:
                line.append(" lost=").append(event.lostRecords());
                break;
            case SESSION_REFUSED:
                client(event);
                line.append(" status=").append(event.helloStatus());
                break;
            case SESSION_OPENED:
            case SESSION_CLOSED:
                client(event);
                break;
            case REQUEST:
                request(event);
                break;
            case RESULT:
            default:
                result(event);
                break;
        }
        log.info(line);
    }

    private void request(final AuditEvent event) {
        client(event);
        line.append(" corr=").append(event.correlationId()).append(" op=").append(event.op());
        if (event.serializable()) {
            line.append(" consistency=SERIALIZABLE");
        }
        if (event.versionCounter() != 0) {
            line.append(" expected=").append(event.versionCounter());
        }
        field(" key=", event, event.keySize(), event.keyOffset(), event.keyLength(),
                event.keyHeadBytes(), event.keyTruncated(), event.keyText());
        field(" value=", event, event.valueSize(), event.valueOffset(), event.valueLength(),
                event.valueHeadBytes(), event.valueTruncated(), event.valueText());
        digest(" keyHash=", event, event.keyHashOffset(), event.keyHashLength(),
                event.keyHashSkipped());
        digest(" valueHash=", event, event.valueHashOffset(), event.valueHashLength(),
                event.valueHashSkipped());
    }

    private void result(final AuditEvent event) {
        client(event);
        line.append(" corr=").append(event.correlationId())
                .append(" op=").append(event.op())
                .append(event.ok() ? " ok" : " failed")
                .append(" error=").append(event.errorCode());
        if (event.versionCounter() != 0) {
            line.append(" version=").append(event.versionCounter()).append('@');
            event.appendVersionNode(line);
        }
        if (event.count() > 0) {
            line.append(" entries=").append(event.count());
        }
    }

    private void client(final AuditEvent event) {
        line.append(" client=");
        event.appendLabel(line);
    }

    /** {@code <size>:<head>..<tail>}: the two halves of what was kept, or nothing when absent. */
    private void field(final String name, final AuditEvent event, final int size, final int offset,
                       final int length, final int head, final boolean truncated,
                       final boolean text) {
        if (size < 0) {
            return;
        }
        line.append(name).append(size).append(':');
        final int headBytes = Math.min(head, length);
        if (text) {
            line.append('"');
        }
        append(event, offset, headBytes, text);
        if (truncated) {
            line.append("..");
        }
        append(event, offset + headBytes, length - headBytes, text);
        if (text) {
            line.append('"');
        }
    }

    private void append(final AuditEvent event, final int offset, final int length,
                        final boolean text) {
        if (length <= 0) {
            return;
        }
        if (text) {
            AuditText.decode(event.recordBytes(), offset, length, line);
            return;
        }
        Hex.appendTo(line, event.recordBytes(), offset, length);
    }

    private void digest(final String name, final AuditEvent event, final int offset,
                        final int length, final boolean skipped) {
        if (skipped) {
            line.append(name).append("skipped");
            return;
        }
        if (length <= 0) {
            return;
        }
        line.append(name).append(event.hashAlgorithm().configName());
        final int saltId = event.hashSaltId();
        if (saltId != 0) {
            line.append("+salt/");
            AuditConfigSnapshot.appendSaltId(line, saltId);
        }
        line.append(':');
        Hex.appendTo(line, event.recordBytes(), offset, length);
    }
}
