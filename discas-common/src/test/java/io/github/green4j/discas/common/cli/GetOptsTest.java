/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.cli;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("GetOpts - choices, optional-arg, grouped/aligned help, parse regressions")
class GetOptsTest {

    /** A schema exercising flags, value options, choices, optional-arg, custom metavar, groups. */
    private static GetOpts opts() {
        return new GetOpts("backup-tool", "Backs up files.")
                .flag("verbose", 'v', "Enable verbose output.")
                .stringOpt("dest", 'd', "Destination directory.").required()
                .intOpt("threads", 't', 4, "Worker threads.")
                .stringOpt("cleanup", null, "strip", "Clean the staging area.")
                .metavar("<mode>").choices("strip", "verbatim", "keep")
                .stringOpt("checksum", 'C', null, "Write a checksum sidecar.")
                .metavar("<algo>").choices("md5", "sha256").optionalArg("sha256")
                .stringOpt("log-file", 'l', null, "Append a log here.")
                .metavar("<file>")
                .group("Extra")
                .flag("grouped", 'g', "A grouped flag.");
    }

    @Test
    void choiceAcceptsAMember() {
        final GetOpts o = opts();
        o.parseOrThrow(new String[] {"-d", "x", "--cleanup", "verbatim"});
        assertEquals("verbatim", o.getString("cleanup"));
    }

    @Test
    void choiceRejectsNonMember() {
        final GetOpts o = opts();
        assertThrows(GetOpts.ParseException.class,
                () -> o.parseOrThrow(new String[] {"-d", "x", "--cleanup", "bogus"}));
    }

    @Test
    void choiceRejectsDefaultNotInSetAtRegistration() {
        assertThrows(IllegalArgumentException.class,
                () -> new GetOpts("p", "d").stringOpt("c", null, "nope", "h").choices("a", "b"));
    }

    @Test
    void optionalArgBareUsesImpliedAndDoesNotConsumeNext() {
        final GetOpts o = opts();
        // '--checksum' bare must NOT swallow '-d' as its value.
        o.parseOrThrow(new String[] {"--checksum", "-d", "x"});
        assertEquals("sha256", o.getString("checksum"));
        assertEquals("x", o.getString("dest"));
    }

    @Test
    void optionalArgInlineAndAttachedShort() {
        final GetOpts a = opts();
        a.parseOrThrow(new String[] {"-d", "x", "--checksum=md5"});
        assertEquals("md5", a.getString("checksum"));

        final GetOpts b = opts();
        b.parseOrThrow(new String[] {"-d", "x", "-Cmd5"});
        assertEquals("md5", b.getString("checksum"));
    }

    @Test
    void optionalArgIsNotRequired() {
        final GetOpts o = new GetOpts("p", "d")
                .stringOpt("key", 'k', null, "a key").optionalArg("default-key");
        o.parseOrThrow(new String[] {}); // absent optional-value option must not fail the required-check
        assertNull(o.getString("key")); // absent => its default (null); implied value is only for a bare flag
        assertFalse(o.isPresent("key"));
    }

    @Test
    void requiredAndDefaultAreMutuallyExclusive() {
        // required() forbids a default value (with-default overload)...
        assertThrows(IllegalStateException.class,
                () -> new GetOpts("p", "d").stringOpt("x", 'x', "def", "h").required());
        // ...and is incompatible with an optional value, in either order (no-default overload).
        assertThrows(IllegalStateException.class,
                () -> new GetOpts("p", "d").stringOpt("x", 'x', "h").required().optionalArg("v"));
        assertThrows(IllegalStateException.class,
                () -> new GetOpts("p", "d").stringOpt("x", 'x', "h").optionalArg("v").required());
    }

    @Test
    void noDefaultOverloadWithRequiredParsesAndEnforcesPresence() {
        final GetOpts a = new GetOpts("p", "d").stringOpt("dest", 'd', "Destination.").required();
        a.parseOrThrow(new String[] {"-d", "/b"});
        assertEquals("/b", a.getString("dest"));

        final GetOpts b = new GetOpts("p", "d").stringOpt("dest", 'd', "Destination.").required();
        assertThrows(GetOpts.ParseException.class, () -> b.parseOrThrow(new String[] {}));
    }

    @Test
    void presenceBasedOptionsCannotBeRequired() {
        assertThrows(IllegalStateException.class,
                () -> new GetOpts("p", "d").flag("f", 'f', "h").required());
        assertThrows(IllegalStateException.class,
                () -> new GetOpts("p", "d").booleanOpt("b", 'b', false, "h").required());
    }

    @Test
    void parseRegressions() {
        // short cluster of flags
        GetOpts o = opts();
        o.parseOrThrow(new String[] {"-d", "x", "-vg"});
        assertTrue(o.getBool("verbose"));
        assertTrue(o.getBool("grouped"));

        // -t 8 and -t8
        o = opts();
        o.parseOrThrow(new String[] {"-d", "x", "-t", "8"});
        assertEquals(8, o.getInt("threads"));
        o = opts();
        o.parseOrThrow(new String[] {"-d", "x", "-t8"});
        assertEquals(8, o.getInt("threads"));

        // long value: '=' and space forms
        o = opts();
        o.parseOrThrow(new String[] {"--dest=/b"});
        assertEquals("/b", o.getString("dest"));
        o = opts();
        o.parseOrThrow(new String[] {"--dest", "/b"});
        assertEquals("/b", o.getString("dest"));

        // '--' ends options
        o = opts();
        o.parseOrThrow(new String[] {"-d", "x", "--", "-v"});
        assertFalse(o.getBool("verbose"));
        assertEquals("[-v]", o.positionals().toString());

        // unambiguous long prefix
        o = opts();
        o.parseOrThrow(new String[] {"-d", "x", "--verb"});
        assertTrue(o.getBool("verbose"));
    }

    @Test
    void missingRequiredThrows() {
        final GetOpts o = opts();
        assertThrows(GetOpts.ParseException.class, () -> o.parseOrThrow(new String[] {"-v"}));
    }

    @Test
    void absentValueOptionKeepsDefault() {
        final GetOpts o = opts();
        o.parseOrThrow(new String[] {"-d", "x"});
        assertEquals("strip", o.getString("cleanup")); // default
        assertNull(o.getString("checksum"));           // no default
        assertEquals(4, o.getInt("threads"));
    }
}
