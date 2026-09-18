# 16. Containers

One image, `green4j/discas`, carries all three commands. Which one runs is the first argument, so
every command line in this handbook works inside a container with `docker run green4j/discas` in
front of it:

```sh
docker run green4j/discas:0.0.5 node  --node-id n1 --cluster-id prod ...
docker run green4j/discas:0.0.5 agent --nodes 1=n1:7002,2=n2:7002 ...
docker run green4j/discas:0.0.5 admin dump ...
```

`node` is the default, so a bare `docker run green4j/discas` starts a node.

Tags are `<version>`, `<major>.<minor>` and `latest`, published for `linux/amd64` and `linux/arm64`.
A release moves all three; nothing else does.

---

## What the image changes

Two things differ from running the start scripts, and both are forced by the container:

**Binds that default to loopback are moved to `0.0.0.0`.** A node's observability endpoint defaults
to `127.0.0.1:9600` and the agent's HTTP to `127.0.0.1:8500`
([6. Monitoring](06-monitoring.md#health-and-ready)) -- correct on a host, unreachable from a kubelet
probe. The image sets them, and the node's `--client-bind` and `--wal-dir`, as *defaults*:

| | Node | Agent |
|---|---|---|
| `--observability-bind` | `0.0.0.0:9600` | `0.0.0.0:9601` |
| `--client-bind` | `0.0.0.0:7002` | -- |
| `--http-bind` | -- | `0.0.0.0:8500` |
| `--wal-dir` | `/var/lib/discas` | -- |

These sit below a flag and below a `DISCAS_*` variable, so `CLI > ENV > DEFAULT`
([7. Configuration](07-configuration.md)) is unchanged and the startup configuration table still
reports where each value came from.

**`--peer-bind` is not defaulted.** It already resolves to this node's own entry in the member list,
which is a local address inside a pod. Setting it in the image would force a port on every
deployment.

## The data directory

The image declares no `VOLUME`. That is deliberate: an unmounted `VOLUME` produces an anonymous
volume, and since a node that starts on an empty directory is a *legitimate* start -- it waits in
`AWAITING_FLOOR` and asks the cluster for a floor -- a forgotten mount would look like a working
node while its state accumulated somewhere nothing would reuse. Mount a volume deliberately, per the
three rules in [2. The node](02-node.md#on-an-orchestrated-platform).

The process runs as **uid 1000**, so the mounted directory has to be writable by it: `fsGroup: 1000`
on Kubernetes, or a pre-`chown`ed directory for a bind mount.

```yaml
securityContext:
  fsGroup: 1000
  runAsNonRoot: true
```

## Memory

The JVM would otherwise take a quarter of the container's memory limit. The image passes
`-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError`, and `--store-heap-fraction` is a fraction of
the heap that produces -- so the store's budget is a fraction of a fraction of the pod's limit.

`JAVA_OPTS` is appended after them, and a later JVM flag wins, so overriding one does not mean
restating the rest:

```sh
docker run -e JAVA_OPTS='-XX:MaxRAMPercentage=50' green4j/discas:0.0.5 node ...
```

## Probes and shutdown

`/health` and `/ready` mean exactly what [6. Monitoring](06-monitoring.md#health-and-ready) says, and
the split matters more here than anywhere: a `livenessProbe` on `/ready` turns a partition into a
cluster-wide crashloop.

```yaml
livenessProbe:
  httpGet: { path: /health, port: 9600 }
readinessProbe:
  httpGet: { path: /ready, port: 9600 }
```

The JVM is PID 1, so a container stop delivers `SIGTERM` to it directly and the node drains through
`CLOSING` within `--shutdown-await-timeout-ms` (5 s by default). Leave `terminationGracePeriodSeconds`
above that.

## Debugging a running container

The image carries the JDK rather than a JRE, so the tools are already inside -- which matters because
a node's event loop is also its timer thread, and pausing it in a debugger times out peer
connections ([2. Development](../dev/02-development.md#debugging)). A thread dump is the instrument:

```sh
docker exec <container> jcmd 1 Thread.print
kubectl exec <pod> -- jcmd 1 Thread.print
```

`jmap`, `jinfo`, `jstat`, `jfr` and `keytool` are present on the same path.

## One note on the base

The image is Alpine, so its libc is musl, whose resolver handles `search` domains differently from
glibc. discas holds peer addresses unresolved and resolves them fresh at every dial
([2. The node](02-node.md)), so peer DNS is continuously on the cluster-formation path rather than
being read once at startup. Nothing in discas links native code, so this is the only place musl is
visible. If a cluster forms by IP but not by name, look here first.
