# 5. Access: TLS, tokens and ACLs

There are **two independent trust domains** in a discas deployment, and conflating them is the
first mistake to avoid. The **peer mesh** (node <-> node) carries consensus traffic and is configured
by the `--tls-*` options; the **client port** (client or agent <-> node) carries requests and is
configured by the `--client-*` options. They may use different CAs, different stores and different
rotation settings, because a member of the cluster and a caller of the cluster are not the same kind
of thing.

**Both default to open.** Peers run plaintext unless `--tls` is set; clients run `allowall` -- any
caller may claim any client id -- and with no `--client-acl-file`, every authenticated client may
read and write every key. Neither default is a bug: a private network is a supported deployment. But
both are silent if you assumed otherwise, which is why the node prints a warning at boot for each.

One consequence worth stating plainly: **an ACL on top of `allowall` is advisory, not enforced.** A
client can simply claim a better-privileged id. Pair an ACL with `token` or `mtls` for it to mean
anything.

---

## How to check

```sh
# What the node is actually running with -- the startup table, secrets masked.
journalctl -u discas-node | grep -A40 'effective configuration'

# Material approaching expiry, and reload health.
curl -s localhost:9600/metrics | grep -E 'reload|material'
```

| Metric | Says |
|---|---|
| `discas_reload_material_expires_seconds` | how long the loaded TLS material has left |
| `discas_reload_material_expiring_total` | it crossed the warning threshold |
| `discas_reloads_total` / `discas_reloads_unchanged_total` / `discas_reload_failures_total` | whether your last edit was applied, meant nothing new, or was refused |
| `discas_node_peer_handshakes_rejected_total` | peers being refused -- the log line says why |

A certificate's expiry is not something to discover from a handshake failure. Alert on
`discas_reload_material_expires_seconds` well ahead of the renewal cadence you chose.

---

## Operations

### Peer mesh

| Option | Meaning |
|---|---|
| `--tls` | mTLS between members. Off by default |
| `--tls-keystore`, `--tls-keystore-password` | this node's leaf certificate and private key (chain `[leaf, intermediate]`) |
| `--tls-truststore`, `--tls-truststore-password` | the cluster CA. During a rotation, **both** old and new |
| `--tls-cert-rotation` | let a reload pick up rotated material, and warn before it expires (default on) |

Identity on the mesh is the certificate's URI SAN, `discas://<cluster_id>/<node_id>`, and it has to
agree with the membership file. When it does not, the handshake fails with `IDENTITY_MISMATCH` and
the cause says which of the four disagreements it was.

Passwords belong in the environment -- every option has a `DISCAS_*` variable
([7. Configuration](07-configuration.md#secrets)). A password on a command line is visible in the
process table.

### Client port: the four modes

| Mode | Channel | Identity from | Wire secrecy |
|---|---|---|---|
| **AllowAll** (default) | plaintext | the claimed client id, unchecked | none |
| **Token** | plaintext | client id + token verified against a store | none |
| **Server-TLS + token** | server-authenticated TLS | client id + token; the client presents no cert | encrypted |
| **mTLS** | mutual TLS | the **client certificate CN** -- the claimed id is ignored | encrypted |

```sh
# (b) token over plaintext, on a trusted network
discas-node ... --client-auth token \
  --client-token-file /etc/discas/tokens.conf \
  --client-acl-file   /etc/discas/acl.conf

# (c) token over server-authenticated TLS: a keystore, and no truststore -- the client is not verified
discas-node ... --client-auth token --client-token-dir /etc/discas/tokens.d \
  --client-tls --client-tls-keystore /etc/discas/client-server.p12 \
  --client-acl-file /etc/discas/acl.conf

# (d) mTLS: identity is the cert CN, and --client-tls is implied
discas-node ... --client-auth mtls \
  --client-tls-keystore   /etc/discas/client-server.p12 \
  --client-tls-truststore /etc/discas/client-ca.p12 \
  --client-acl-file /etc/discas/acl.conf
```

Or, without the start scripts -- same arguments, same `DISCAS_*` variables:
`java -cp "$CP" io.github.green4j.discas.node.starter.DisCasNodeStarter` in place of `discas-node`. See [1. Context](01-context.md#two-ways-to-invoke-each-of-them).

**Server-side TLS on the client port authenticates the CA, not which node answered.** The client
verifies the certificate chain against its trust store; there is no endpoint identification and no
node identity in the client handshake. Anything holding a leaf signed by that CA can present itself
as a member of this cluster to a client. The cryptographic node-identity binding -- the
`discas://<cluster>/<node>` SAN -- exists on the **peer mesh**, where it is checked. If your threat
model needs a client to know *which* node it reached, that is a property the client port does not
offer today.

The node refuses these combinations at startup rather than running in a quietly weaker posture:
`token` without exactly one of `--client-token-file` / `--client-token-dir`; a token store set when
the mode is not `token`; `mtls` without a trust store; `--client-tls` without a keystore; a client
TLS store set while `--client-tls` is off.

### Token file

Java properties, one line per client, re-read on `POST /reload`. Records are `;`-separated:

```
client.web-1   = pbkdf2$210000$<saltB64>$<hashB64>$<notAfterEpochMs>
client.reporter = pbkdf2$...$<notAfter> ; pbkdf2$...$<notAfter>
```

**The file holds a hash, not the token**, so you do not write these by hand --
[`discas-admin token`](08-admin.md#token) mints one and prints the line:

```sh
discas-admin token --client-id web-1 --ttl-days 90 >> /etc/discas/tokens.conf
```
```
discas-admin token: client web-1, 210000 iterations, expires 2026-11-22T09:14:03Z (90 days)
discas-admin token: token (shown once, store it now): YWLLmWOtrH3dnqLXuxTE8zKOIRe38N7WXLkUPwnbl_I
```

The record goes to standard output and everything else to standard error, so the redirect above
appends exactly the right line while the token still reaches your terminal. **It is shown once and
stored nowhere** -- a hash cannot be reversed, so a lost token is re-issued, never recovered.

Two records on one line is how a token is rotated without an outage: the old one keeps working until
its `notAfter`. The directory form (`--client-token-dir`) is one `<clientId>.token` file per client,
same grammar -- add, rotate or revoke a client by dropping, editing or deleting one file.

A node in token mode with no token file refuses every client. Create the file before starting it.

### ACL file

Java properties, one line per client, re-read on `POST /reload`. Grants are `;`-separated, each
`<keyPrefix>:<ops>`, where the ops are the letters `G`et, `P`ut, `C`ompare-and-set, `D`elete, `S`can.
The split is on the **last** colon, so a prefix may itself contain one:

```
acl.web-1    = app/:GPCD ; session/:GPCDS
acl.reporter = report/:GS
```

**Default-deny and allow-only**: a request is allowed if some grant's prefix matches the key *and*
includes the op. A client with no line in the file may do nothing, which is the point of having the
file at all.

A file that has properties in it but not one `acl.*` among them -- `alc.web-1`, say -- is **refused**
rather than applied, and the revision in force stays in force. Taken at face value it would say that
no client is granted anything, so a mis-typed prefix would deny every client at once and look like a
clean reload while doing it. An *empty* file is not refused: that one really does mean nobody may do
anything, and it is applied like any other revision.

Every node loads the same file through the same distribution mechanism, so there is no per-node
lag window to reason about.

### Rotating a token

Zero disruption, and no restart:

1. Mint the new one -- `discas-admin token --client-id web-1` -- and put its record on the same line
   as the old, separated by ` ; ` (or add a second line to that client's `<id>.token` file).
2. `POST /reload` on every node. Let the clients switch over; both are valid meanwhile.
3. Drop the old record -- or let its `notAfter` retire it -- and reload again.

### Rotating a CA

The same shape for both domains, and the key is **overlap: trust the new CA before anyone presents
leaves from it, and retire the old one only after everyone does.**

1. **Stand up the new CA.** Commands for every issuer are in
   [15. PKI recipes](15-pki-recipes.md).
2. **Trust first.** Update every trust store to contain **both** CAs, then `POST /reload` on every
   node to swap the material in. Everyone now accepts old- and new-CA leaves; nobody presents a new
   one, so nothing changes on the wire.
3. **Re-issue leaves** from the new CA, same identity and same SAN, write the new key stores, and
   reload. Roll this out at your own pace -- the overlap makes any mix valid. A key store and the
   trust store beside it are read in one call and applied together or not at all, so a mismatched
   pair is never in force.
4. **Retire the old CA** once every party presents new-CA leaves. Drop it from the trust stores and
   reload.

No established connection is dropped at any step: the `SSLContext` is built once over reloadable
managers, so a rotation swaps what is underneath it rather than rebuilding it. `node_id` and
`incarnation_id` never change.

---

## Troubleshooting

| State | Raised when | Action |
|---|---|---|
| `ACCESS_DENIED` | a node refused a client's or agent's `CLIENT_HELLO` | check the token or client certificate against that node's `--client-auth` and `--client-acl-file` |
| `MATERIAL_EXPIRING` | loaded TLS material is approaching expiry | rotate it. A reload picks the new files up with no restart |
| `RELOAD_FAILED` | a file did not parse on a reload | fix the file and reload again. **Not transient** -- a reload that caught a file mid-write reports it as `unreadable` and never reaches here, so what is on disk is malformed and will stay malformed |
| `CLOCK_SKEW` | a peer's clock differs by 1 s or more at the handshake | run NTP on both. This spread is the accuracy ceiling of every lock lease in the system |
| `CLOCK_UNUSABLE` | a peer is beyond the handshake's five-minute replay bound | fix NTP on that node; it cannot join at all until then |

**A failed reload is not an outage.** The last good version stays in force, so a malformed ACL file
does not lock every client out -- it leaves the previous policy running until you fix it.

**A peer certificate that expired** is not an access problem so much as a membership one: the
handshake fails, the peer drops out, and the node may fall below quorum. It shows up as `PEER_DOWN`
and `/ready` 503. Rotation needs no restart, so this should never be how you learn about an expiry.

---

## Impact

| Action | Effect |
|---|---|
| Edit the token or ACL file | applies on reload, no restart, no dropped connections |
| Rotate material with `--tls-cert-rotation` on | established sessions survive; new handshakes use the new material |
| Turn on `--tls` or `--client-tls` | a restart, and every party must be ready for it -- see [10. Upgrades](10-upgrade.md) |
| Remove a client's ACL line | once reloaded, that client may do nothing at the next request. Default-deny is immediate |
| Retire a CA before every leaf has rolled | handshakes fail and members drop out. This is why step 4 is last |

---

## Recovery

- **Locked yourself out with a bad ACL** -- the previous policy is still in force if the new file
  does not parse. If it *does* parse and is simply wrong, edit it again and reload.
- **A rotation that went wrong mid-way** -- go back to trusting both CAs. Overlap is the safe state,
  and every step of the procedure is reversible while both are trusted.
- **A compromised member** -- evict it with a constant-`N` seat swap and stop renewing its
  certificate ([3. Cluster](03-cluster.md#evicting-a-compromised-member-now)). Dropping it from the
  member file outright would change `N` and be refused.

---

## Related

- [15. PKI recipes](15-pki-recipes.md) -- issuing the material this page configures
- [3. Cluster and members](03-cluster.md) -- the member file the SANs must agree with
- [7. Configuration](07-configuration.md) -- where secrets go, and what reloads
- [11. The HTTP agent](11-http-agent.md) -- the agent is a client, and authenticates as one
- Runbooks: [RB-13 rotate the peer CA](13-runbooks.md#rb-13-rotate-the-peer-ca),
  [RB-14 rotate client credentials](13-runbooks.md#rb-14-rotate-client-credentials)
