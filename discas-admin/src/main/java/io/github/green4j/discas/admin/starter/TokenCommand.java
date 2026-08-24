/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin.starter;

import io.github.green4j.discas.common.cli.GetOpts;
import io.github.green4j.discas.common.cli.config.ConfigResolver;
import io.github.green4j.discas.common.cli.config.ConfigSupport;
import io.github.green4j.discas.common.client.auth.Pbkdf2;
import io.github.green4j.discas.common.client.auth.TokenRecord;
import io.github.green4j.discas.common.client.auth.TokenSpecs;
import io.github.green4j.discas.common.identity.ClientId;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

/**
 * {@code discas-admin token} -- mints a client token and prints the line a node's token file needs.
 *
 * <p>The only command here that neither connects to a cluster nor writes a file. It exists because
 * a node under {@code --client-auth token} reads a grammar nothing else could produce: the file
 * holds a PBKDF2 hash, not the token, so provisioning means choosing a work factor, salting,
 * deriving, base64-ing and assembling a {@code $}-separated record. That is a program, and an
 * operator should not have to write one to add a client.
 *
 * <p><b>Two streams, on purpose.</b> The record goes to standard output, alone and unprefixed, so
 * {@code >>} appends exactly what belongs in the file. The token itself and everything explaining
 * it go to standard error, where a redirect cannot swallow them -- a secret that scrolls past into a
 * config file is a secret nobody read.
 *
 * <p>The token is shown once and kept nowhere. There is no command to recover it, by construction:
 * what the cluster stores cannot be reversed, which is the point of storing a hash.
 */
final class TokenCommand extends AbstractCommand {

    /** Bytes of randomness in a generated token. 32 is a 256-bit secret. */
    private static final int TOKEN_BYTES = 32;

    private static final int DEFAULT_TTL_DAYS = 90;

    private static final SecureRandom RANDOM = new SecureRandom();

    TokenCommand() {
        super("token", "mint a client token and print the record a token file needs");
    }

    @Override
    protected void run(final GetOpts opts,
                       final ConfigResolver config,
                       final ProgressLine progressLine) throws Exception {
        final ClientId clientId = ClientId.of(config.required("client-id"));
        final int ttlDays = config.integerAtLeast("ttl-days", DEFAULT_TTL_DAYS, 1);
        // Only upward. The count is in the record so a cluster can be moved to a stronger one
        // without invalidating the tokens already issued; lowering it below the default would
        // weaken every token minted from here on, and no operational need reads like that.
        final int iterations =
                config.integerAtLeast("iterations", Pbkdf2.DEFAULT_ITERATIONS, Pbkdf2.DEFAULT_ITERATIONS);

        final String supplied = config.secret("token");
        final String token = supplied != null ? supplied : generateToken();

        final Instant notAfter = Instant.now().plus(Duration.ofDays(ttlDays));
        final byte[] salt = Pbkdf2.newSalt();
        final TokenRecord record = new TokenRecord(
                salt, iterations, Pbkdf2.hash(token, salt, iterations), notAfter.toEpochMilli());

        // stdout: the file's line, and nothing else that could end up in the file.
        System.out.println("client." + clientId.value() + " = " + TokenSpecs.format(record));

        note("client " + clientId.value() + ", " + iterations + " iterations, expires "
                + notAfter + " (" + ttlDays + " days)");
        if (supplied == null) {
            note("token (shown once, store it now): " + token);
        } else {
            note("token supplied by you; this command did not keep it");
        }
        note("append the line above to the node's --client-token-file, or write it as "
                + clientId.value() + ".token under --client-token-dir. Both are hot-reloaded.");
    }

    /** Commentary, on standard error, so a redirect of the record cannot swallow it. */
    private void note(final String message) {
        System.err.println(program + ": " + message);
    }

    private static String generateToken() {
        final byte[] bytes = new byte[TOKEN_BYTES];
        RANDOM.nextBytes(bytes);
        // URL-safe and unpadded: a token travels in a CLIENT_HELLO, a config file and often an
        // environment variable, and none of those wants '+', '/' or '='.
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    @Override
    protected GetOpts options() {
        return new GetOpts(program,
                "Mint a client token and print the record a node's token file needs. The record "
                        + "goes to standard output and everything else to standard error, so "
                        + "'discas-admin token -c web-1 >> tokens.conf' appends exactly the right "
                        + "line while you still see the token. Every option can also be set by its "
                        + "DISCAS_* environment variable.")
                .stringOpt("client-id", 'c', ConfigSupport.helpWithEnv("client-id",
                        "Client the token is for. It is the id that client presents, and the id "
                                + "the ACL file grants to [required].")).metavar("<id>")
                .stringOpt("ttl-days", null, ConfigSupport.helpWithEnv("ttl-days",
                        "Days until the record expires [default: " + DEFAULT_TTL_DAYS + "]."))
                        .metavar("<days>")
                .stringOpt("token", null, ConfigSupport.helpWithEnv("token",
                        "Use this secret instead of generating one, for a token that comes from a "
                                + "secret manager [default: generate a 256-bit one]."))
                        .metavar("<secret>")
                .stringOpt("iterations", null, ConfigSupport.helpWithEnv("iterations",
                        "PBKDF2 work factor: how many derivations one verification costs, and so "
                                + "how expensive guessing is against a stolen file. Carried in the "
                                + "record, so raising it applies to new tokens without "
                                + "invalidating old ones. Cannot be set below the default ["
                                + Pbkdf2.DEFAULT_ITERATIONS + "].")).metavar("<n>")
                .epilogue("  " + program + " --client-id web-1 --ttl-days 30\n"
                        + "  " + program + " -c reporter >> /etc/discas/tokens.conf\n"
                        + "The token is shown once and stored nowhere: the file holds a PBKDF2 hash, "
                        + "which is not reversible, so a lost token is re-issued rather than "
                        + "recovered. To rotate without an outage, mint a second record and put it "
                        + "on the same line separated by ' ; ' -- both are valid until the old one's "
                        + "expiry passes. To revoke now, delete the record; the node reloads it.")
                .width(100);
    }
}
