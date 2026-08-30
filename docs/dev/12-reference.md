# 12. Reference

## Repository map

| Module | Depends on | Holds |
|---|---|---|
| `discas-common` | -- | wire types, event loop, HTTP server, CLI, metrics, logging, TLS, dump format |
| `discas-node` | common | a cluster member: consensus, store, WAL, anti-entropy, peer transport, ACL |
| `discas-client` | common | `DisCasClient`, locks, client transports, dump/load over a client |
| `discas-agent` | common, client | HTTP/JSON front end |
| `discas-admin` | common, client, node | the operator CLI: `dump`, `load`, `init`, `token` |
| `discas-example` | common, node, client | runnable examples; some assert their claims |
| `discas-performance` | common, node, client | the performance envelope and its instruments |
| `discas-int-test` | all | cross-module, cluster, chaos and crash suites |
| `discas-all` | common, node, client, agent, admin | no sources: the deployable modules bundled into one jar |

`discas-node` and `discas-client` do not depend on each other; anything they share lives in
`discas-common`.

## Package map

Under `io.github.green4j.discas`:

| Package | What lives there |
|---|---|
| `common` | `EventLoop`, `Ballot`, `KvLimits`, `KeyHash`, `ByteBuffers`, `Hex` |
| `common/client` | `ClientMessage`, its codec, `ClientErrorCode`, `ReadConsistency`, `ClientIngress`, `ResponseSink` |
| `common/client/auth` | client authenticators, token stores, PBKDF2 |
| `common/transport` | `FrameCodec`, `ChunkingEngine`, `TransportConfig`, buffer reader/writer, `ConnectionState` |
| `common/transport/tls` | contexts, reloadable managers, cert identities, rotation |
| `common/transport/security` | the two channel-security seams and their plaintext implementations |
| `common/http/server` | `HttpServer` (its javadoc is the manual), routers, query parser |
| `common/io` | `ReloadableFiles`, `ReloadableFileSource`, `PeriodicDaemon`, reload observers |
| `common/dump` | the dump file format |
| `common/operator` | `OperatorState`, `OperatorAttention`, `OperatorGroup` |
| `common/metrics`, `common/observability` | registry, Prometheus text, the shared observability server |
| `common/logging`, `common/identity`, `common/cli` | logger, `NodeId`/`ClientId`/`ClusterId`/`IncarnationId`, `GetOpts` |
| `node` | `DisCasNode`, `Proposer`, `Acceptor`, `LocalStore`, `AntiEntropy`, tombstones, observers |
| `node/wal` | `FileWal`, segments, snapshots, `IncarnationMarker`, on-disk formats |
| `node/transport` | peer mesh, client server, `PEER_HELLO`, `PromiseCeilingHistory` |
| `node/acl`, `node/membership` | authorization; `Members` and its file source |
| `node/observability`, `node/seed`, `node/starter` | probes; member seeding; the standalone process |
| `client` | `DisCasClient`, `Version`, `ClusterClock`, config, observers |
| `client/lock`, `client/transport`, `client/dump` | locks; transports; `ClusterDump`/`ClusterLoad` |
| `agent`, `agent/starter` | handlers, JSON writer, `ReloadableClient` |
| `admin`, `admin/starter`, `admin/runbook` | operations; commands; the generated `RUN.md` |

## Key interfaces

| Interface | Module | Implemented by |
|---|---|---|
| `Wal` | node | `FileWal`; `InMemoryWal` in tests |
| `PeerTransport` | node | `TcpPeerTransport`; in-process and fault-injecting variants in tests |
| `ClientTransport` | client | `TcpClientTransport`, `InProcessClientTransport`, `ColocatedClientTransport` (the local member in process, the rest over TCP) |
| `ClientIngress` / `ResponseSink` | common | the node's client handler / whichever transport created it |
| `NodeObserver` / `ClientObserver` / `ReloadObserver` | node / client / common | logging, metrics, peer-state and stderr decorators |
| `Members<T>` | node | `FileMembers` (reloadable), `InMemoryMembers` |
| `ClientAuthenticator` | common | `AllowAll`, `Token`, mTLS |
| `ClientAcl` | node | `FileClientAcl`, `InMemoryClientAcl` |
| `PeerChannelSecurity` / `ClientChannelSecurity` | common | plaintext passthrough, `SSLEngine`-backed |
| `Reloadable<T>` | common | `ReloadableFileSource` -- one contract for every file re-read on request |
| `EventLoop.IoDriver` | common | both transports and the HTTP server's workers |
| `ByteContent` | common | HTTP response bodies, zero-copy |

## Wire protocols

**Framing** (both wires): `[int32 len][int32 crc32][byte type][payload]`, big-endian. Types:
`PEER_HELLO`, `PEER_HELLO_RESP`, `PEER_MESSAGE`, `CLIENT_HELLO`, `CLIENT_HELLO_RESP`,
`CLIENT_MESSAGE`, `BYTES`, `CHUNK_START`, `CHUNK_PART`, `CHUNK_END`.

**Peer messages** (`PeerMessage`): `PrepareReq`/`PrepareResp`, `AcceptReq`/`AcceptResp`,
`DigestReq`/`DigestResp`, `KeysReq`/`KeysResp`, `CeilingReq`/`CeilingResp`,
`PurgeCheckReq`/`PurgeCheckResp`, `PurgeReq`.

**Client messages** (`ClientMessage`): get, put, cas, delete, scan and their responses.

**Versions**: `TransportProtocol.PROTOCOL_VERSION`, `StorageFormat.FORMAT_VERSION` with its
`LAYOUT_VERSION`, and `DumpCodec.FORMAT_VERSION`. Each is bumped on the first incompatible change
**after** a release, so each constant is the authority on its own value -- read it there.

## The enums worth reading first

| Enum | Names |
|---|---|
| `NodeState` | `REPLAYING`, `AWAITING_FLOOR`, `SERVING`, `CLOSING`, `FAILED` |
| `RoundFailure` | `BALLOT_NACK`, `INSUFFICIENT_RESPONDERS`, `ACCEPT_TIMEOUT`, `PROPOSAL_EXPIRED` |
| `ClientErrorCode` | `NONE`, `ACCESS_DENIED`, `INVALID_ARGUMENT`, `INTERNAL`, `UNAVAILABLE`, `NOT_READY`, `NO_QUORUM_AT_COORDINATOR`, `BALLOT_LOST`, `PROPOSAL_EXPIRED`, `STORE_FULL` |
| `PeerHelloRespStatus` | `OK` plus ten refusals -- see [8](08-transport-security.md) |
| `PurgeAnswer` | `RETAINED`, `ABSENT`, `HELD` |
| `CeilingEvidence` | `NONE`, `WITNESS`, `NO_HISTORY` |
| `OperatorState` | every condition needing an operator, each with its remedy |
| `ClientOp` | `GET`, `PUT`, `CAS`, `DELETE`, `SCAN` |
| `ReadConsistency` | `LINEARIZABLE`, `SERIALIZABLE` |
| `ScanCoverage` | `QUORUM`, `ANY_AVAILABLE` |

## Glossary

| Term | Meaning here |
|---|---|
| **Ballot** | `(counter, NodeId)`, totally ordered; also a key's *version* to a client |
| **Round** | one prepare -> transform -> accept cycle for one key |
| **Coordinator** | the member driving a round; any member, per operation |
| **Quorum** | `N/2 + 1` |
| **Promise ceiling** | the highest ballot an acceptor has *forced* the right to promise up to |
| **Promise floor** | derived at recovery from the ceiling; nothing at or below it may be promised |
| **Incarnation** | one run of a node's durable storage; changes when the directory is recreated |
| **Witness** | a member whose bounds are unbroken, so its ceiling is evidence during floor recovery |
| **Tombstone** | a delete marker; the thing that out-votes a stale value on a lagging replica |
| **Purge** | dropping a tombstone once every member permits it |
| **Range** | one of 256 hash buckets anti-entropy compares by digest |
| **Fenced write** | a CAS conditioned on a version, so a late duplicate cannot apply |
| **Determinate** | the outcome is known; only `UNAVAILABLE` is not |
| **Fencing token** | the strictly increasing generation a lock hands out; the actual safety mechanism |
