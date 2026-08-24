/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import java.time.Duration;
import java.util.Locale;

/**
 * Wall-clock / concurrency intensity profile shared by every stress, chaos and
 * concurrency test. Selected via {@code -Ddiscas.test.profile=quick|standard|long}.
 *
 * <p>QUICK targets ~30 s of chaos for pre-commit and IDE feedback, STANDARD ~1 min for CI, and
 * LONG ~10 min for a pre-release soak.
 */
public enum TestProfile {
    QUICK(0.5),
    STANDARD(1.0),
    LONG(3.0);

    public static final String PROPERTY = "discas.test.profile";

    private static final long MIN_DURATION_MS = 500L;
    private static final int MIN_COUNT = 1;

    private final double scale;

    TestProfile(final double scale) {
        this.scale = scale;
    }

    public static TestProfile current() {
        final String raw = System.getProperty(PROPERTY, "standard").trim();
        if (raw.isEmpty()) {
            return STANDARD;
        }
        try {
            return TestProfile.valueOf(raw.toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            return STANDARD;
        }
    }

    public double scale() {
        return scale;
    }

    public Duration scale(final Duration base) {
        final long scaled = (long) Math.ceil(base.toMillis() * scale);
        return Duration.ofMillis(Math.max(MIN_DURATION_MS, scaled));
    }

    public int scale(final int base) {
        final int scaled = (int) Math.ceil(base * scale);
        return Math.max(MIN_COUNT, scaled);
    }

    public long scale(final long base) {
        final long scaled = (long) Math.ceil(base * scale);
        return Math.max(MIN_COUNT, scaled);
    }

    public boolean atLeast(final TestProfile other) {
        return ordinal() >= other.ordinal();
    }
}
