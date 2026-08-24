/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.ScanPage;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.common.client.ReadConsistency;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

/**
 * The shortest useful program: a 3-node cluster, one client, and every KV operation once --
 * <b>with each call's failure branches present rather than assumed away</b>.
 * <p>
 * Every result goes through {@code ExampleOutcome}, which sorts an outcome into applied, refused
 * (determinate: it did not happen) or unknown. That is not ceremony: the difference between the
 * last two is the difference between "retry freely" and "retrying may destroy a write you never
 * saw".
 * <p>
 * There is exactly one compare-and-set and it is fenced on the key's {@link Version}: under ABA a
 * value-compared failure cannot be handled by any caller, which makes it the wrong primitive for a
 * store whose selling point is that you can reason about failure.
 */
public final class QuickstartExample {

    private static final long CALL_MS = 8_000L;

    private QuickstartExample() {
    }

    public static void main(final String[] args) throws Exception {
        try (ExampleCluster.RunningCluster cluster =
                     ExampleCluster.startInProcessCluster(1)) {
            final DisCasClient client = cluster.client(0);
            final ByteBuffer key = ExampleBytes.encode("user:1");

            System.out.println("=== Quickstart: 3 in-process nodes, 1 client ===");

            // put -- unconditional, last-write-wins. An unknown outcome here is terminal for the
            // call: re-putting converges on our value, but a late duplicate can overwrite a write
            // that happened in between. That is what last-write-wins means, so it is in contract.
            report("put(user:1, v1)",
                    ExampleOutcome.of(client.put(key.duplicate(), ExampleBytes.encode("v1")), CALL_MS));

            // get -- a read costs a round and returns the value together with the version the
            // write below needs. A read that fails changes nothing, so there is no unknown case.
            final ExampleOutcome<GetResult> read = ExampleOutcome.of(
                    client.get(key.duplicate(), ReadConsistency.LINEARIZABLE), CALL_MS);
            final GetResult current = read.require("get(user:1)");
            System.out.println("  get(user:1) -> " + ExampleBytes.decode(current.value())
                    + " @ " + current.version().token());

            // cas fenced on that version. A duplicate carries a ballot the register has moved
            // past, so this is the write that stays safe when a coordinator stops answering --
            // and the only one whose unknown outcome is resolved by simply re-sending it.
            final ExampleOutcome<CasResult> swap = ExampleOutcome.of(
                    client.cas(key.duplicate(), current.version(), ExampleBytes.encode("v2")), CALL_MS);
            if (swap.applied()) {
                final CasResult r = swap.value();
                System.out.println("  cas(user:1, @" + current.version().token() + " -> v2) swapped="
                        + r.swapped()
                        + (r.swapped() ? "" : ", lost to @" + r.version().token()));
            } else {
                System.out.println("  " + swap.describe("cas(user:1, @version -> v2)"));
            }

            // A serializable read: cheaper, no round, and possibly behind. Staleness here is a
            // property of what we asked for, not of a failure -- which is why it needs no
            // recovery branch, only an informed caller.
            final ExampleOutcome<GetResult> stale = ExampleOutcome.of(
                    client.get(key.duplicate(), ReadConsistency.SERIALIZABLE), CALL_MS);
            System.out.println("  get(user:1, SERIALIZABLE) -> "
                    + ExampleBytes.decode(stale.require("serializable get").value()));

            // scan enumerates keys, not values, and completes only on a quorum. Its failure mode
            // is "the listing cannot be trusted", never a silently short answer.
            final ExampleOutcome<ScanPage> scanned =
                    ExampleOutcome.of(client.scan(), CALL_MS);
            System.out.println("  scan() -> " + scanned.require("scan").results().stream()
                    .map(s -> ExampleBytes.decode(s.key())).collect(Collectors.toList()));

            // putIfAbsent -- cas against Version.INITIAL, under the name that says what it is
            // for. Losing here is final rather than a reason to read again: the key exists, and no
            // retry makes it not exist. The result carries the winner, so a loser already knows
            // who took the key without a second round trip.
            final ExampleOutcome<CasResult> claimed = ExampleOutcome.of(
                    client.putIfAbsent(key.duplicate(), ExampleBytes.encode("v3")), CALL_MS);
            final CasResult claim = claimed.require("putIfAbsent(user:1, v3)");
            System.out.println("  putIfAbsent(user:1, v3) swapped=" + claim.swapped()
                    + (claim.swapped() ? "" : ", already held by "
                    + ExampleBytes.decode(claim.value()) + " @" + claim.version().token()));

            // update -- the read-transform-write loop of ContentionExample, built in. It retries a
            // lost compare and nothing else: a timeout is the absence of a verdict, and re-running
            // the transform against one would apply it twice. The transform sees null when the key
            // is absent and returns null to tombstone.
            final ExampleOutcome<GetResult> updated = ExampleOutcome.of(
                    client.update(key.duplicate(),
                            v -> ExampleBytes.encode(ExampleBytes.decode(v) + "+suffix")), CALL_MS);
            final GetResult after = updated.require("update(user:1, v -> v + suffix)");
            System.out.println("  update(user:1) -> " + ExampleBytes.decode(after.value())
                    + " @ " + after.version().token());

            // A version-fenced delete: same fence, committing a tombstone. Fenced because an
            // unfenced delete left unknown can land after somebody recreated the key.
            final Version live = ExampleOutcome
                    .of(client.get(key.duplicate(), ReadConsistency.LINEARIZABLE), CALL_MS)
                    .require("get before delete")
                    .version();
            final ExampleOutcome<CasResult> deleted =
                    ExampleOutcome.of(client.delete(key.duplicate(), live), CALL_MS);
            System.out.println("  delete(user:1, @" + live.token() + ") swapped="
                    + deleted.require("fenced delete").swapped());

            System.out.println("  get(user:1) after delete -> " + ExampleBytes.decode(
                    ExampleOutcome.of(client.get(key.duplicate()), CALL_MS).require("get").value()));
        }
    }

    private static void report(final String what, final ExampleOutcome<?> outcome) {
        System.out.println("  " + outcome.describe(what));
        if (outcome.unknown()) {
            // Shown rather than swallowed: for an unfenced write this is where a real caller
            // decides, and the decision cannot be made by the client on its behalf.
            System.out.println("    ^ an unfenced write: re-issue converges on our value, but may "
                    + "overwrite a concurrent one. Use cas(key, Version, value) when that matters.");
        }
    }
}
