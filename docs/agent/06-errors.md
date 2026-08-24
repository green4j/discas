# 6. Errors

Every failure comes back as JSON with the same shape, whatever the status:

```json
{"error":"this coordinator cannot see a majority"}
```

**Branch on the status code, never on the message text.** The messages come from several layers and
are not a contract.

The connection stays open on a failure -- a failed request is a normal outcome, not a broken
connection.

## The statuses

| Status | What happened | Retry? |
|---|---|---|
| `400` | your request is wrong: bad `ttl`, `wait`, `version`, `token`, a bare `?cas`, a missing key, or a key past the size limit | no -- it fails identically every time |
| `403` | the nodes refused this agent's identity | no -- fix the credentials |
| `404` | the key does not exist, or is tombstoned | -- |
| `405` | wrong method for that route | no |
| `413` | the body is larger than the cluster's maximum value size (16 MiB) | no |
| `500` | something that never reached a node, **and a strict listing that could not reach quorum** | see below |
| `503` | the cluster could not complete it | **read the next section** |
| `504` | the agent's own `--request-timeout-seconds` elapsed | outcome unknown, like `503` |
| `507` | every replica is full | only after something is deleted |

`507` is not a server fault and not your fault: every replica holds the same keys, so a node that is
full is a cluster that is full. A delete is never refused for want of room, which is what gives a
full store a way back.

**`500` on a listing is a rough edge worth knowing.** A `?keys` that cannot reach a majority fails
with `500`, not `503`, even though the condition is transient and worth re-issuing:

```json
{"error":"Scan did not reach quorum (1 of 3 configured nodes responded, quorum is 2)"} [500]
```

Either retry it, or use [`?keys&stale`](02-kv.md#listing-keys) if a partial listing is acceptable.

## The one that needs care

**`503` collapses five cluster conditions into one status, and they do not all mean the same thing.**

Four of them are **determinate** -- the write provably did not happen:

- the coordinator is still replaying its log (`{"error":"node 1 is recovering"}`);
- the coordinator cannot see a majority (`{"error":"this coordinator cannot see a majority"}`);
- another writer won the ballot duel for that key;
- the write outlived the coordinator's proposal expiry before anything was proposed.

The fifth is **indeterminate**: the round reached the accept phase and did not complete in time.
Some acceptor may hold the proposal and a later proposer may finish it -- *without you*. `504` is the
same situation seen from the agent's side.

You cannot tell them apart from the status alone, and re-reading does not help: "not applied yet"
and "will be applied in a moment" look identical.

**So the rule is about the request, not the response:**

| You sent | On `503`/`504` |
|---|---|
| `PUT ?cas=...` or `DELETE ?cas=...` | **retry as written.** A duplicate carries a version the register has already overtaken, so it provably cannot apply twice |
| plain `PUT` / `DELETE` | **do not blindly retry.** It may already have been applied |
| `GET` | retry freely -- a read changes nothing |
| lock acquire | do not retry; ask [who holds it](04-locks.md#when-you-do-not-know-whether-you-got-it) |

This is the whole practical argument for [`?cas`](02-kv.md#the-conditional-write-cas): it turns the
one genuinely dangerous failure into one you can handle by doing nothing special.

If you must use an unfenced write and still need recovery, make the write identify its author and
read it back:

```bash
marker="writer-$(hostname):$payload"
if ! curl -sf -X PUT --data-binary "$marker" localhost:8500/v1/kv/thekey > /dev/null; then
  # Unknown outcome: is the value that landed mine?
  [ "$(curl -s -X GET 'localhost:8500/v1/kv/thekey?raw')" = "$marker" ] && echo "applied after all"
fi
```

If you find yourself writing that often, the answer is to switch to a fenced write.

## Reading a status quickly

```bash
curl -s -X GET -o /dev/null -w '%{http_code}\n' localhost:8500/v1/kv/somekey
```

Avoid `curl -f` here. It turns any 4xx/5xx into a non-zero exit, but it **discards the body** --
which is where the reason is -- and `--show-error` only adds curl's own message
(`curl: (22) The requested URL returned error: 404`), not the server's. Capture the status with
`-w` instead and keep the body.

The same distinction, argued from the client's side rather than HTTP's, is in
[the client documentation](../user/02-key-value.md#what-to-do-with-a-failure).
