# Majabigwaduce: Clustering Design (2.1.0)

*2026-10-26 — draft*

This document proposes how to let Majabigwaduce run its reducers (and later its mappers) on
other nodes of a Pekko cluster. It is written as a plan before any code exists, in the same
spirit as `TypedActorsMigration.md`, and should be updated to record what was actually built.

## Goals

- Run a map-reduce job with reducers spread across several JVMs, while keeping the mid-level
  (`MapReduce`) and high-level (`DataDefinition`) APIs as they are: callers still pass ordinary
  Scala functions `f` and `g` at the call site.
- Keep the core library light. Clustering lives in a new sbt subproject and artifact,
  `majabigwaduce-cluster`, which depends on `pekko-cluster-typed`. Users who don't cluster don't
  download it (2.0.3 removed the unused cluster dependencies from the core for this reason).
- Be testable in an ordinary ScalaTest suite and on CircleCI, without real machines.
- Make a good teaching example: the same "ship the closure to the workers" model that Spark uses.

Non-goals for 2.1.0: fault tolerance beyond failing the job cleanly, data locality, dynamic
rebalancing mid-job, and Pekko Cluster Sharding.

## Where the current code already helps

Two properties of the 2.0.x typed design make this tractable:

1. **Messages carry only data.** `DoMap` and `DoReduce` hold keys, values and `replyTo` refs.
   The functions `f` and `g` are captured when a behavior is *created*
   (`Mapper(f)`, `Reducer(g)`), not sent per message.
2. **The Master works from plain sequences of refs.** `Master.behavior` builds
   `mappers: Seq[ActorRef[MapperCommand[...]]]` and `reducers: Seq[ActorRef[ReducerCommand[...]]]`
   with `context.spawn`, and everything after that (`doMap`, `distributeWork`,
   `doReductionAsync`) only sends to those refs. Where the refs come from is a single seam.

So clustering is mostly about *where the reducer behaviors get spawned*, not about changing the
map → distribute → reduce → collate pipeline.

## The key question: getting `f` and `g` to the workers

Scala function values are serializable. Since Scala 2.12, and in Scala 3, the compiler emits
lambdas through `LambdaMetafactory` with the serializable flag, so Java serialization can write
and read them, together with whatever they capture. A quick check on Scala 3.3.9:

| Function | Java serialization round trip |
|---|---|
| `(_, line) => Try(line.split(" ").head -> 1)` | works |
| `w => !stop(w)` capturing a `Set[String]` | works |
| `_ + h.n` capturing an instance of a non-`Serializable` class | `NotSerializableException` |

This is exactly Spark's model, and it carries Spark's two conditions:

- **Every node needs the same classes.** A serialized lambda refers to a synthetic method in the
  class that defined it, so the worker JVM must have that class on its classpath. Spark handles
  this by shipping the application jar to executors. For 2.1.0 we require that every node runs
  the same application jar (the normal way to deploy a Pekko cluster anyway).
- **Captured values must be serializable.** Capturing `this`, a logger, a `Config`, an
  `ActorSystem` and so on fails at send time. This is Spark's familiar "Task not serializable"
  error, and it's worth turning into a clear Majabigwaduce error message (and a lesson).

The real constraint is Pekko, not Scala: **Pekko disables Java serialization by default**
(`pekko.actor.allow-java-serialization = off`), because deserializing arbitrary Java objects from
the network is a well-known remote-code-execution risk. Options:

| Option | Pros | Cons |
|---|---|---|
| **A. Ship closures with Java serialization** (recommended) | Keeps today's API; matches Spark; Throwables serialize for free | Needs Java serialization switched on; only safe on a trusted network |
| B. Named jobs: `f`/`g` defined in code on every node, messages carry only a job name | No Java serialization of code; data can use Jackson | New API (jobs registered by name); can't pass an ad-hoc lambda at the call site |
| C. Kryo (e.g. a Pekko Kryo serializer) for everything | Faster, more compact than Java serialization | Extra dependency; closure support needs checking; same trust caveat |

**Recommendation: A for 2.1.0**, documented clearly as "trusted cluster only", with the
serialization bindings limited to Majabigwaduce's own message types (a marker trait such as
`MajabigwaduceSerializable`). B or C can come later if someone needs to run on an untrusted
network or wants better throughput. This needs a spike first (see below), because it must be
confirmed exactly how Pekko 1.7 behaves when `allow-java-serialization` is on but Java
serialization is bound only to specific classes.

With Java serialization, the data side is also simple: generic `K2`, `W`, `V2` values only need
to be `Serializable` (case classes, `String`, numbers and standard collections already are), and
the `Throwable`s carried in `MapperResponse`, `ReduceResult` and `Response` serialize as they are.

## Proposed architecture

### Core refactor (in the core library, still 2.1.0)

Introduce a small abstraction for "where workers come from", and make today's behavior its
local implementation:

```scala
trait WorkerPool:
  def reducers[K2, W, V2](g: ...): Future[Seq[ActorRef[ReducerCommand[K2, W, V2]]]]
  def mappers[K1, V1, K2, W](f: ...): Future[Seq[ActorRef[MapperCommand[K1, V1, K2, W]]]]
```

`LocalWorkerPool` does what `Master.behavior` does now (`context.spawn` children). The Master
gets its refs from the pool instead of spawning directly. This is a no-op refactor for local
users and can be merged and released on its own.

### Cluster module (`majabigwaduce-cluster`)

- **Worker nodes** run a `ClusterWorker` behavior that registers with the typed `Receptionist`
  under a `ServiceKey[WorkerCommand]`. Its protocol:
  - `SpawnReducer(g, z, jobId, replyTo)`: spawn a local `Reducer(g)` / `Reducer_Fold(g, z)` and
    reply with its `ActorRef` (Pekko serializes `ActorRef`s natively).
  - `StopJob(jobId)`: stop everything spawned for that job.
- **The master node** uses a `ClusterWorkerPool`: it subscribes to the Receptionist listing,
  and when a job starts, asks each available worker to `SpawnReducer`, collecting the remote refs.
  From there the existing pipeline runs unchanged; `distributeWork` already spreads keys across
  however many reducer refs it is given.
- **Configuration**: `majabigwaduce.cluster { reducers-per-worker, min-workers, ... }` plus the
  usual `pekko.cluster` seed-node settings.

### Phasing

1. **Spike** (throwaway): two `ActorSystem`s in one JVM joined as a cluster; send a message
   carrying a lambda from one to the other with Java serialization bound to a marker trait;
   apply it on the far side. Confirms the serialization config before anything else is built.
2. **2.1.0-M1**: `WorkerPool` refactor in core (local only), released or at least merged.
3. **2.1.0**: `majabigwaduce-cluster` with remote **reducers**; mapping still runs on the master
   node. Two-node `CountWords` example.
4. **Later**: remote **mappers**, so input chunks are mapped where they're sent.

A note on expectations: with only reducers distributed, the master still does all the mapping
and then ships every intermediate value over the network. That demonstrates the mechanism well,
but it will rarely be faster than local execution. Real speed-ups need phase 4 and a reduce step
that is expensive relative to the data moved. The benchmarks subproject should measure this
rather than assume it.

## Failure handling (2.1.0)

- A worker that leaves or crashes mid-job makes its asks time out, so the job fails with a
  clear error rather than hanging. No retries in 2.1.0.
- A function that can't be serialized fails when the job starts, before any data moves, with
  an error that names the job and suggests what was captured.
- Workers stop a job's reducers on `StopJob`, or when the master node leaves the cluster.

## Testing

- **Unit/integration**: start two or three `ActorSystem`s in one JVM, join them
  programmatically (`Cluster(system).manager ! Join(address)`), with Artery on `127.0.0.1` and
  fixed test ports. Runs as an ordinary ScalaTest suite and on CircleCI.
- **Serialization**: Pekko's `serialize-messages = on` in tests forces every message through
  serialization even locally, which catches non-serializable captures early.
- Pekko's multi-node test kit can come later if needed.

## Open questions

1. Is "trusted cluster only" acceptable for the intended uses (coursework, a lab or departmental
   cluster)? If not, option B or C moves up.
2. Should a job be able to run on the master node too (master is also a worker), or only on
   dedicated worker nodes?
3. How should the cluster variant be selected: a different `Actors` / `ActorSystem` setup with
   the same `MapReduce` API, or explicit `ClusterMapReduce` constructors?
4. Which examples matter most for the course: `CountWords` over two nodes, the matrix example,
   or `DataDefinition`?
