# Majabigwaduce: Clustering Design (2.1.0)

*2026-10-26 — draft; updated after the serialization spike and the WorkerPool refactor*

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

**Recommendation: A for 2.1.0**, documented clearly as "trusted cluster only". B or C can come
later if someone needs to run on an untrusted network or wants better throughput.

### Spike results (Pekko 1.7.1, Scala 3.3.9)

Two `ActorSystem`s in one JVM, joined as a cluster over Artery TCP on `127.0.0.1`, with
`allow-java-serialization = on` and Java serialization bound to a marker trait:

1. **Discovery works.** The master node finds a worker on the other node through the typed
   `Receptionist`.
2. **Shipping a closure works.** A `SpawnFolder(g, z, replyTo)` message carrying a lambda that
   captures a `Set[String]` reaches the worker, which spawns a folding actor from it and replies
   with that actor's (remote) `ActorRef`. Folding through that ref gives the right answer.
3. **Exceptions come back intact.** A `NumberFormatException` thrown on the worker arrives on
   the master node as `Left(NumberFormatException)`.
4. **A non-serializable capture fails silently.** Pekko logs `Failed to serialize message` with
   `NotSerializableException` on the sending side and drops the message, so the caller's ask
   simply times out. Majabigwaduce must therefore check serializability itself (by serializing
   the definition locally) before sending, to fail fast with a clear message.
5. **A marker-trait binding does not restrict anything.** With `allow-java-serialization = on`,
   an unrelated `Serializable` case class that does *not* extend the marker trait was also
   accepted. Switching Java serialization on is effectively all-or-nothing, which is why this
   must be described as trusted-cluster only, not merely "limited to our message types".

With Java serialization, the data side is also simple: generic `K2`, `W`, `V2` values only need
to be `Serializable` (case classes, `String`, numbers and standard collections already are), and
the `Throwable`s carried in `MapperResponse`, `ReduceResult` and `Response` serialize as they are.

## Proposed architecture

### Core refactor (in the core library, still 2.1.0)

**Done** (`core/WorkerPool.scala`). The Master no longer spawns its workers; it asks a pool:

```scala
trait WorkerPool:
  def acquire[K1, V1, K2, W, V2](context: ActorContext[?], actors: Actors,
      mapper: MapperDef[K1, V1, K2, W], nMappers: Int,
      reducer: ReducerDef[K2, W, V2], nReducers: Int): Future[Workers[K1, V1, K2, W, V2]]
```

- `MapperDef(f, forgiving)` and `ReducerDef` (`ReduceDef(g)` or `FoldDef(g, z)`) describe the
  workers as data -- the functions plus mode -- rather than as `Behavior`s, and are
  `Serializable`, so a remote pool can ship them. Each has a `behavior` method that builds the
  actual behavior wherever it runs. `FoldDef` keeps `z` as a function so that every reduction
  still starts from a fresh zero, as `Reducer_Fold` did.
- `acquire` returns a `Future`, and the Master chains each request onto it. That lets a remote
  pool supply workers asynchronously, with no stashing or protocol change; a pool failure is
  passed back to the caller as a `Failure`.
- `LocalWorkerPool`, the default, spawns local children exactly as before. No public API change.

### Cluster module (`majabigwaduce-cluster`)

**Built** (`cluster/`), with remote reducers only so far:

- **`ClusterWorker`** (start one with `ClusterWorker.start(system)` on every node which should
  run reducers, the master node included if you like) registers with the typed `Receptionist`
  under `ClusterWorker.key`. Its one command, `SpawnReducers(reducer, n, master, replyTo)`,
  carries the `ReducerDef`, function included. For each job the worker starts a small job actor
  which spawns the `n` reducers as its children, replies with their refs (`ReducersSpawned`),
  and watches the Master: when the Master stops, or its node leaves the cluster, the job actor
  stops and takes its reducers with it. So no separate `StopJob` message is needed.
- **`ClusterWorkerPool`** is what `WorkerPool.forSystem` returns for a cluster member. In
  `acquire` it spawns the mappers locally, then checks that Java serialization is on, checks
  that the `ReducerDef` serializes (failing fast with a message that names the likely cause),
  finds at least `min-workers` workers through the Receptionist, and asks each for its share of
  the job's `reducers` (the existing setting: the total is split as evenly as possible). The
  refs are interleaved by node, so the Master's round-robin distribution alternates between
  nodes. From there the Master's pipeline runs unchanged.
- **Configuration** (`cluster/src/main/resources/reference.conf`):
  `majabigwaduce.cluster { worker-pool, min-workers = 1, discovery-timeout = 10s, spawn-timeout = 5s }`,
  plus the usual `pekko.cluster` seed-node settings and, deliberately not set by the module,
  `pekko.actor.allow-java-serialization = on`.

**Lesson learned while building it:** the fail-fast check found a real problem in core.
`MapReduceFirstFold` and `MapReducePipeFold` created their zero function as
`() => summon[Zero[V1]].zero`, and `summon` there reads a field of the enclosing object, so the
lambda captured `this`, which holds the `ActorSystem`. Core now takes the `Zero` into a local
value first, and `Zero` itself is `Serializable`. The `CountWords` example had the same problem
with its reducer functions and zeros, which were members of the class; they now live in its
companion object. Expect students (and the other examples) to hit this too.

### Two-terminal demo

`cluster/src/test/scala/.../cluster/ClusterDemo.scala` runs `CountWords` on two JVMs on one
machine, using `cluster/src/test/resources/cluster-demo.conf` (Artery on `127.0.0.1`, Java
serialization on, the worker node on port 2551 as the seed node). In the first terminal, start
the worker node:

```bash
sbt "cluster/Test/runMain com.phasmid.majabigwaduce.cluster.clusterWorkerNode"
```

Once it prints `worker started`, run `CountWords` on the master node (port 2552) in the second
terminal:

```bash
sbt "cluster/Test/runMain com.phasmid.majabigwaduce.cluster.clusterCountWords"
```

The master prints `Word count = 11`, then leaves the cluster. The worker logs
`spawned 4 reducers for pekko://CountWordsCluster@127.0.0.1:2552/system/...` for each of the
pipeline's two Masters, and then `master ... has stopped: stopping its reducers`. Run the master
again as often as you like; press RETURN in the first terminal to stop the worker. Both mains
take an optional port as their first argument, and `clusterCountWords` takes the document URLs
after it, so you can start a second worker (on, say, port 2553) and see the reducers split
between the two. If no worker is up, the master fails after `discovery-timeout` (10 seconds)
with a message asking whether `ClusterWorker.start` has been called. The Artery `WARN` lines
about an aborted connection when the master shuts down are expected.

### `DataDefinition` and `Matrix` on a cluster

`MatrixOperation` (the mid-level matrix example) needed no change: like `CountWords`, it runs
clustered when it's given a cluster `ActorSystem`. The high-level API needed two changes in core:

- **Choosing the `ActorSystem`.** `DataDefinition`'s factory methods (`apply`, `create`) used to
  build every `LazyDD` in the JVM-wide `DataDefinition.context`, so they could never use a
  cluster. They (and `Matrix`'s `product`, `product2` and `forRows`) now take a `DDContext` as a
  `using`/implicit parameter. The default, `DDContext.default`, lives in `DDContext`'s companion
  object, so it's found when there's no other `DDContext` in scope: existing code compiles and
  behaves as before. To run on a cluster, define a `given DDContext` built on a cluster
  `ActorSystem`. This is decision 3 again, applied to the high-level API: same API, with the
  appropriate `ActorSystem`. `DataDefinition.context` is still there, as a plain method returning
  the default. It's no longer a `given`, so importing `DataDefinition.*` doesn't bring a second
  candidate into scope. (Adding the parameter is source-compatible but not binary-compatible.)
- **Another captured `this`.** `LazyDD.evaluate` built its reducer as
  `summon[Monoid[W]].combine`, which captured the `LazyDD`, and through it the `DDContext` and its
  `ActorSystem`. The pre-check caught it at once. The monoid is now taken into a local value first,
  as in the earlier `MapReduceFirstFold` fix.

`ExamplesClusterSpec` runs `MatrixOperation`, a `LazyDD`, `DataDefinition.map`/`reduce` and a
25 × 25 `Matrix2.product2` (just above the default 20 × 20 cutoff below which no map-reduce is
used) on a master node with no worker. A local run would give the same answers, so each test also
checks, from the worker's log, that the worker spawned that job's reducers.

### Phasing

1. **Spike** (throwaway) -- done; see "Spike results" above.
2. **`WorkerPool` refactor in core** (local only) -- done.
3. **2.1.0**: `majabigwaduce-cluster` with remote **reducers**; mapping still runs on the master
   node. Two-node `CountWords` example -- done, both as tests (`TwoNodeSpec`,
   `MasterAsWorkerSpec`) and as a two-terminal demo (see "Two-terminal demo" above).
   The matrix and `DataDefinition` examples -- done (`ExamplesClusterSpec`: see
   "`DataDefinition` and `Matrix` on a cluster" above).
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
  an error that names the job and suggests what was captured. (The spike showed that Pekko on
  its own would just drop the message and let the ask time out.)
- Workers stop a job's reducers when its Master stops, or when the master node leaves the cluster.

## Testing

- **Unit/integration**: start two or three `ActorSystem`s in one JVM, join them
  programmatically (`Cluster(system).manager ! Join(address)`), with Artery on `127.0.0.1` and
  fixed test ports. Runs as an ordinary ScalaTest suite and on CircleCI.
- **Serialization**: Pekko's `serialize-messages = on` in tests forces every message through
  serialization even locally, which catches non-serializable captures early.
- Pekko's multi-node test kit can come later if needed.

## Decisions (2026-10-26)

The four open questions from the first draft, answered:

1. **Trusted cluster only: yes.** Java serialization (option A) it is. Because Pekko's Java
   serialization can't be restricted to our own messages (spike result 5), the cluster module
   does not switch it on for you: an application must set
   `pekko.actor.allow-java-serialization = on` itself, and the cluster pool fails with a clear
   message if it hasn't.
2. **The master node can be a worker too: yes.** Any node that starts a `ClusterWorker` offers
   reducers, including the node the Master runs on. A one-machine demo is then just a cluster of
   one; dedicated worker nodes simply don't run a Master.
3. **Same API, chosen by the `ActorSystem`.** There are no `ClusterMapReduce` constructors. A
   Master picks its pool from the `ActorSystem` it runs in: if the system is a cluster member
   (`pekko.actor.provider = cluster`) and the cluster module is on the classpath (its
   `reference.conf` names `majabigwaduce.cluster.worker-pool`), it uses that pool; otherwise
   `LocalWorkerPool`. So existing code such as `CountWords` runs clustered just by being given a
   cluster `ActorSystem`.
4. **Examples: `CountWords` first**, then the matrix example and `DataDefinition`, which will all
   be needed soon.
