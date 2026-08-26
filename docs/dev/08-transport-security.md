# 8. Transport and security

## Context

Two independent wires. The **peer mesh** carries consensus and anti-entropy between members; the
**client wire** carries the client protocol. They share their framing, chunking and security seams
(all in `discas-common`) and nothing else -- different message families, different handshakes,
different authentication.

Both are non-blocking `EventLoop.IoDriver`s, so a node's two transports and its timers share one
thread. Both also have an in-process implementation with no sockets at all, which is what makes an
embedded deployment and most of the test suite possible.

## Framing

```
[int32 frameLen][int32 frameCrc32][byte frameType][payload...]
```

Big-endian, stated at every allocation rather than left to the `ByteBuffer` default. `frameLen`
counts the CRC, the type and the payload; the CRC covers the type and the payload. A mismatch throws
and the transport drops the connection.

Frame types: `PEER_HELLO`, `PEER_HELLO_RESP`, `PEER_MESSAGE`, `CLIENT_HELLO`, `CLIENT_HELLO_RESP`,
`CLIENT_MESSAGE`, `BYTES`, and `CHUNK_START` / `CHUNK_PART` / `CHUNK_END`.

A payload larger than `maxFrameBytes` is split by `ChunkingEngine` into a chunk stream -- one active
chunked message per connection, so a `CHUNK_START` while another assembly is live is a protocol
violation. Budgets (`maxQueuedOutBytes`, `maxInflightBytes`, `maxRxBufferBytes`, `maxConnections`)
live on `TransportConfig`, shared by both sides; each side only supplies the largest message its
framing must carry, and the defaults are derived from that so they cannot disagree.

## The peer handshake

One connection per pair, dialled by the lower node id, so two members never end up with two. Nothing
protocol-level crosses it until `PEER_HELLO` has settled five things: the protocol version, the
cluster id, both identities, the frozen `N`, and each side's **promise ceiling claim**.

Every rejection is a `PeerHelloRespStatus`, and the set is the vocabulary of everything that can be
wrong between two members:

| Status | Means |
|---|---|
| `PROTOCOL_MISMATCH` | incompatible builds |
| `CLUSTER_MISMATCH` | that node belongs to another cluster |
| `IDENTITY_MISMATCH` | the member list and the presented identity disagree (the cause says which of four) |
| `UNKNOWN_PEER` | not in this node's member list |
| `CLUSTER_SIZE_MISMATCH` | the two disagree about `N` -- refused, because a mixed-`N` cluster forms two quorums over one key |
| `HELLO_TIMESTAMP_SKEW` | clocks more than five minutes apart (a replay guard; usually a wrong clock) |
| `NOT_REPLAYED` | one end cannot state a ceiling yet -- transient, the dialer retries |
| `CEILING_ROLLED_BACK` | the same storage came back holding *less* than it left with |
| `INCARNATION_DUPLICATED` | one member's data directory copied onto another |
| `INCARNATION_CHANGED` | retired, never sent -- a replaced disk is safe, it recovers a floor |

The last three are the storage-identity guards from [3. CASPaxos](03-caspaxos.md).
`PromiseCeilingHistory` remembers, per process, the highest ceiling each incarnation claimed -- a
detector, not a repair, and it narrows the window rather than closing it: memory is per process, so
a rollback coinciding with a restart of every peer that remembered passes unseen.

## Security

Pluggable at one seam per side -- `PeerChannelSecurity` and `ClientChannelSecurity` -- which sit
*beneath* the frame codec and transform network bytes to application bytes. The plaintext
implementations are identity passthroughs that copy nothing.

**Peer mesh**: plaintext, or mTLS with a shared cluster CA. Identity is bound cryptographically by a
URI SAN, `discas://<clusterId>/<nodeId>` (`CertIdentities`), and the handshake cross-checks it
against the `PEER_HELLO` fields -- so a valid certificate for the wrong node is still refused. The
mesh is symmetric: both roles require a peer certificate.

**Client wire**: three authentication modes, one per server, selected by configuration --
`AllowAllClientAuthenticator` (takes the claimed id at face value), `TokenClientAuthenticator`
(PBKDF2-hashed shared secret, constant-time compare, overlap rotation via multiple live records) or
mTLS, where the id comes from the certificate CN (`ClientCertIdentities`).

**Authorization** is separate from authentication and is **default-deny**: a `ClientPolicy` is an
ordered list of grants binding a key prefix to a set of `ClientOp` (`GET`, `PUT`, `CAS`, `DELETE`,
`SCAN`). It runs at ingress, *before* the round, so a denied request leaves the store untouched. An
unbound authorizer is permissive -- that is what preserves the AllowAll contract for tests.

The critical rule: authorization keys off the **authenticated** `ClientId` bound to the connection at
hello time, never the `senderId` carried in each message, which is self-declared.

### Rotation

Everything file-backed reloads through one mechanism (`ReloadableFileSource` registered in the
process-wide `ReloadableFiles`): the member list, client tokens, the client ACL, and TLS material.
Nothing polls -- `ReloadableFiles.reloadAll()`, reached from `DisCasNode.reloadFiles()` and the
`POST /reload` endpoint, is the only thing that reads a file after startup, which is what makes an
in-place edit safe. It is a two-phase commit across every registered source: all of them parse
before any of them publishes, so a single unparseable file leaves the whole set as it was. A reload
that fails to parse keeps the last good value and reports it; TLS material swaps trust first, then
key, so new handshakes accept both the old and the new CA during a rotation and established
connections are undisturbed.

## Source map

| Concern | File |
|---|---|
| Frames | `discas-common/src/main/java/io/github/green4j/discas/common/transport/FrameCodec.java` |
| Chunking | `discas-common/src/main/java/io/github/green4j/discas/common/transport/ChunkingEngine.java` |
| Shared budgets | `discas-common/src/main/java/io/github/green4j/discas/common/transport/TransportConfig.java` |
| Peer mesh | `discas-node/src/main/java/io/github/green4j/discas/node/transport/TcpPeerTransport.java` |
| Peer handshake | `discas-node/src/main/java/io/github/green4j/discas/node/transport/PeerHelloCodec.java`, `PeerHelloRespStatus.java` |
| Rollback detector | `discas-node/src/main/java/io/github/green4j/discas/node/transport/PromiseCeilingHistory.java` |
| Client server / client side | `discas-node/src/main/java/io/github/green4j/discas/node/transport/TcpClientServerTransport.java`, `discas-client/src/main/java/io/github/green4j/discas/client/transport/TcpClientTransport.java` |
| Security seams | `discas-common/src/main/java/io/github/green4j/discas/common/transport/security/PeerChannelSecurity.java`, `ClientChannelSecurity.java` |
| TLS | `discas-common/src/main/java/io/github/green4j/discas/common/transport/tls/TlsContexts.java`, `ReloadableTlsContext.java`, `CertRotationManager.java` |
| Cert identity | `discas-common/src/main/java/io/github/green4j/discas/common/transport/tls/CertIdentities.java`, `ClientCertIdentities.java` |
| Client auth | `discas-common/src/main/java/io/github/green4j/discas/common/client/auth/ClientAuthenticator.java` |
| Authorization | `discas-node/src/main/java/io/github/green4j/discas/node/acl/ClientAuthorizer.java`, `ClientPolicy.java`, `ClientOp.java` |
| Membership | `discas-node/src/main/java/io/github/green4j/discas/node/membership/Members.java`, `FileMembers.java` |
| Reload | `discas-common/src/main/java/io/github/green4j/discas/common/io/ReloadableFileSource.java`, `ReloadableFiles.java`, `ReloadReport.java` |

## Tests

| Question | Suite |
|---|---|
| mTLS handshake and delivery | `discas-int-test/src/test/java/io/github/green4j/discas/tls/MtlsTransportTest.java` |
| Cert rotation with no disruption | `discas-int-test/src/test/java/io/github/green4j/discas/tls/MtlsRotationClusterTest.java` |
| Every membership edit, applied or refused | `discas-node/src/test/java/io/github/green4j/discas/node/transport/MembershipReloadMatrixTest.java` |
| Every answer a `PEER_HELLO` can get | `discas-node/src/test/java/io/github/green4j/discas/node/transport/PeerHandshakeRefusalMatrixTest.java` |
| Rollback detection, and not refusing a legitimate wipe | `discas-node/src/test/java/io/github/green4j/discas/node/transport/PromiseCeilingHistoryTest.java` |
| Authorization at ingress, including the store untouched | `discas-int-test/src/test/java/io/github/green4j/discas/client/ClientAclIntegrationTest.java` |
| ACL edits while serving | `discas-node/src/test/java/io/github/green4j/discas/node/acl/ClientAclRevisionMatrixTest.java` |
| Malformed input against every decoder | `discas-int-test/src/test/java/io/github/green4j/discas/codec/CodecFuzzTest.java` |

## How to change it safely

- **Wire changes are compatibility changes.** `TransportProtocol.PROTOCOL_VERSION` (currently 1) is
  exchanged once per connection and is what lets the codecs resolve enums strictly -- an unrecognised
  byte cannot have come from a legitimate counterpart. Adding a field means bumping it.
- **Never authorize on the `senderId` in a message.** It is self-declared; the bound `ClientId` is
  the authenticated one.
- **A new `PeerHelloRespStatus` carries an explicit wire byte**, never the `ordinal()` -- reordering
  the enum must not change what travels.
- **A new refusal must decide whether it is transient.** `NOT_REPLAYED` resolves itself and the
  dialer retries; a permanent one needs an operator, so it needs an `OperatorState` too.
