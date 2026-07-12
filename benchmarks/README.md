# Benchmarking with JMH

## What JMH is and why it exists

A naive benchmark like this gives garbage numbers:

```scala
val start = System.nanoTime()
wordCount()
println(System.nanoTime() - start)
```

The problems are all JVM-specific:

- **JIT warm-up**: the JVM interprets bytecode at first, then profiles it, then compiles hot
  code to machine code. The first several calls run *much* slower than steady-state —
  measuring them mixes "cold" and "hot" performance.
- **Dead-code elimination**: if the JIT can prove a result is never used, it can optimize the
  whole computation away, and you end up benchmarking nothing.
- **GC pauses, OS scheduling noise**: a single run can be skewed by a GC pause that has
  nothing to do with your code's actual cost.

JMH (Java Microbenchmark Harness, built by the OpenJDK team) exists specifically to dodge
these traps: it runs your code for a warm-up period *before* measuring, forks a separate clean
JVM per benchmark, uses "blackholes" to prevent dead-code elimination, and runs enough
iterations to report a mean with an error margin instead of one noisy sample.

## How it's wired up in this repo

- `project/plugins.sbt` has `sbt-jmh`, the sbt integration for JMH (it handles the
  annotation-processing/code-generation step JMH needs).
- `build.sbt` defines a separate `benchmarks` subproject (`.enablePlugins(JmhPlugin)`,
  `.dependsOn(root)`) so JMH's dependencies never leak into the published library jar.
- Benchmark classes live under `benchmarks/src/main/scala/com/phasmid/majabigwaduce/benchmarks/`:
  `WordCountBenchmark.scala` (the map/pipe/reduce word-count pipeline), `MatrixBenchmark.scala`
  (`Matrix2`'s actor-vs-sequential row processing), `DataDefinitionBenchmark.scala`
  (the filter/map/reduce RDD-style pipeline), and `MapperParallelismBenchmark.scala`
  (map-side parallelism specifically, with a deliberately CPU-heavy mapper function).

## The annotations, explained

Using `WordCountBenchmark` as the reference example:

- **`@State(Scope.Benchmark)`** — one instance of this class is shared across all
  threads/iterations in a run. (Other options: `Scope.Thread` — a fresh instance per thread,
  useful if state isn't thread-safe.)
- **`@BenchmarkMode(Array(Mode.AverageTime))`** — report average time per call. Other modes:
  `Throughput` (ops/sec), `SampleTime` (percentile distribution), `SingleShotTime` (cold-start
  cost, no warm-up).
- **`@OutputTimeUnit(TimeUnit.MILLISECONDS)`** — just controls the units in the printed table.
- **`@Warmup(iterations = 5)`** / **`@Measurement(iterations = 10)`** — 5 throwaway iterations
  to let the JIT warm up, then 10 iterations that actually get recorded. Each "iteration" by
  default runs for 1 second, calling the `@Benchmark` method repeatedly and counting
  invocations.
- **`@Fork(1)`** — run in 1 freshly-spawned JVM. Bump this to 3-5 for a real baseline you
  intend to trust, since different JVM invocations can land on different JIT/GC behavior; more
  forks means your error bars reflect that variance honestly instead of hiding it.
- **`@Param(Array("10", "100", "1000"))`** — these are *application-level* configuration, not
  harness configuration. The difference matters: flags like `-i`/`-wi`/`-f`/`-t` (see below)
  control how JMH *measures* — warm-up length, fork count, thread count — and stay the same no
  matter what code you're benchmarking. `@Param` fields, by contrast, are inputs specific to
  *this* benchmark's own domain — how much data it processes, how it's shaped — defined by
  whoever wrote the benchmark. JMH just supplies each declared value to the field before a run;
  what it means is entirely up to the benchmark class.

  When a class has more than one `@Param` field, JMH runs the *entire* warmup+measurement
  cycle once for **every combination** of values across all of them — the Cartesian product,
  not a zipped/paired sweep. `WordCountBenchmark` has four: `documents` (3 values by default),
  `executors` (1), `keyCardinality` (1), `reducers` (1) — that's 3 × 1 × 1 × 1 = 3 full runs.
  Add a second value to `executors` and it becomes 3 × 2 × 1 × 1 = 6 runs; the cost of sweeping
  multiple parameters compounds fast, which is worth keeping in mind before adding another
  `@Param` on a whim.
- **`@Setup(Level.Trial)`** / **`@TearDown(Level.Trial)`** — run once before/after all
  iterations *for a given param combination* (not once per invocation — that would defeat the
  point of measuring steady-state cost). `Level.Trial` is the coarsest; `Level.Iteration` and
  `Level.Invocation` exist for setup that needs to happen more often, but `Level.Invocation` in
  particular adds real overhead of its own and is rarely what you want.
- **`@Benchmark`** — the method actually being timed. Its return value matters: returning a
  real value (rather than `Unit`) is what lets JMH's blackhole mechanism prevent the JIT from
  proving the computation's result is unused and optimizing it away.

## WordCountBenchmark's parameters

- **`documents`** — number of synthetic documents word-counted per invocation. This is the
  overall data-volume knob.
- **`executors`** — default parallelism knob (default: 4). `keyCardinality` and `reducers`
  each fall back to this value unless explicitly overridden (see below).
- **`keyCardinality`** — number of distinct grouping keys ("servers") the documents are folded
  under in stage 1. This mirrors the original `CountWords` exemplar, where real documents came
  from actual HTTP servers (`bbc.com`, `cnn.com`, `default`) and got grouped by which server
  they came from; here it's a synthetic stand-in for "how many distinct keys does the data
  have," independent of how much parallelism is applied to reduce them.
- **`reducers`** — number of reducer actors `Master` spins up to parallelize the reduce stage
  (set via the `reducers` config key that `Master` reads at construction time). This is
  actor-pool parallelism, a different concern from `keyCardinality`.
- **`reuseInstance`** — `true`: build the `Actors`/pipeline once in `@Setup` and reuse it across
  every `@Benchmark` invocation in the trial (the fixed behavior, since 2.0.1 — see "Design
  limitations" item 1 below). `false`: build a fresh `Actors`/pipeline on every single
  invocation and close it at the end (the pre-2.0.1 behavior), kept as an explicit, labeled
  "cold" comparison point in the same run/table. JMH's own warmup iterations can't distinguish
  this on their own, since they just call the same `@Benchmark` method body repeatedly — only a
  `@Param` that changes what the method body constructs can separate "JVM/JIT warm" from
  "actor-instance warm."

`keyCardinality` and `reducers` default to `-1`, a sentinel meaning "use `executors`." JMH
`@Param` default arrays must be compile-time constant literals, so a field can't directly
default to *another field's* runtime value — the `-1` sentinel is the standard workaround:
`@Setup` resolves it (`if (keyCardinality > 0) keyCardinality else executors`) before each
trial. Practically, this means:

- Don't pass `-p keyCardinality` or `-p reducers` at all, and both follow whatever `-p
  executors=N` you gave (or its default of 4) — one knob, coupled behavior.
- Pass `-p keyCardinality=8` (or `reducers`) explicitly, and that value wins regardless of
  `executors` — letting you isolate "does more data-key cardinality slow things down" from
  "does more reducer parallelism speed things up," which is the reason they're two fields
  instead of one.

## MatrixBenchmark's parameters

- **`size`** — dimension N of the two NxN matrices being multiplied. The overall data-volume
  knob, same role as `documents` in `WordCountBenchmark`.
- **`forceActors`** — `Matrix2.forRows` only takes the actor-based (`DataDefinition`/MapReduce)
  path when `this.size >= cutoff`; below that it runs sequentially in-thread. `forceActors`
  overrides the in-scope `cutoff` given directly (`Dimensions(Seq(1,1))` when `true`,
  effectively unreachable when `false`), so the *same* `size` can be measured both ways. That's
  what makes the crossover visible: at `size=5`, forcing actors was ~60x slower than sequential
  (pure dispatch overhead on trivial work); at `size=50` the two were within a few percent
  (the actual computation had grown enough to amortize that overhead).

**Limitation worth knowing about:** unlike `WordCountBenchmark`, the number of reducer actors
used on the actor path isn't independently configurable per run here. `DataDefinition`'s actor
context (`DDContext`) is a JVM-wide singleton — built once, lazily, from the global application
config the first time the `DataDefinition` object is touched, reading `ConfigFactory.load()`
at that moment. A benchmark instance has no hook to override it afterward; it's whatever
`majabigwaduce.DataDefinition.reducers` resolves to at JVM startup (default: 4). That's itself
a real design smell worth carrying into the typed-actors redesign discussion: an
eagerly-initialized global singleton makes a component hard to reconfigure within one running
JVM.

## DataDefinitionBenchmark's parameters

- **`size`** — number of synthetic `(key, value)` pairs in the pipeline's input (also the key
  cardinality, since every key `0 until size` is unique). The data-volume knob.
- **`reducers`** — the benchmark runs `dd.filter(...).map(...).reduce(...)`, where `dd` is
  built via `DataDefinition(kvs, reducers)`. `LazyDD.evaluate` takes the sequential in-thread
  path when `reducers < 2`, and the actor-based (`MapReducePipe`) path when `reducers >= 2`,
  with that many reducer actors — so `-p reducers=1,4,16,64` sweeps both the sequential-vs-actor
  crossover (same idea as `MatrixBenchmark`'s `forceActors`) *and* the effect of reducer count on
  the actor path, in one parameter.

**Fixed in 2.0.1** (previously a limitation): unlike `MatrixBenchmark`, `DataDefinition`'s
reducer-actor count *is* independently configurable per call now — `LazyDD.evaluate` builds a
per-call `Config` overlay (`cfs.withValue("reducers", ...)`) from its own `partitions` argument,
rather than reading the shared, JVM-wide `DDContext.config` directly. `Config` is immutable, so
this doesn't mutate anything shared; every other `DataDefinition` instance in the JVM is
unaffected. See "Design limitations found via benchmarking" item 3 below.

### A real bug this benchmark exposed

Building this benchmark surfaced a genuine correctness bug in `Actors.scala`, not just a
benchmark-code issue. Its actor-name suffix was:

```scala
private val suffix = (System.nanoTime().hashCode + Actors.getCount).toHexString
```

`DataDefinition`'s `MapReducePipe` actors are created *inside* `LazyDD.evaluate()`, fully
encapsulated — a benchmark has no handle to `close()` them between invocations (unlike
`WordCountBenchmark`, where `stage1.close()`/`stage2.close()` sidestepped this). Under JMH's
tight measurement loop (thousands of calls per second), `System.nanoTime()`'s 32-bit hash can
repeat between calls just nanoseconds apart — its upper bits barely move at that timescale —
so two different `Actors` instances could compute the *same* suffix and collide on actor names
(`InvalidActorNameException: actor name [...] is not unique!`), even though `Actors.getCount`
itself never repeats. The fix was to stop combining a hash with the counter and rely on the
counter alone, via a thread-safe, purely monotonic `AtomicLong`:

```scala
private val counter = new java.util.concurrent.atomic.AtomicLong(0)
def getCount: Long = counter.incrementAndGet()
```

This is a real fix to core library code (`src/main/scala/.../core/Actors.scala`), not scoped to
`benchmarks/` — any consumer creating actors at high frequency could have hit the same
collision. Full test suite re-verified green (125 tests) after the change.

## MapperParallelismBenchmark's parameters

Added in 2.0.1 to demonstrate map-side parallelism (see "Design limitations found via
benchmarking" item 2 below) — none of the other benchmarks in this suite have a mapper function
CPU-heavy enough to show a signal above `ask`-overhead/message-passing noise.

- **`items`** — number of synthetic `(key, value)` pairs in the batch.
- **`mappers`** — number of mapper actors `Master` spins up to parallelize the map phase (set
  via the `mappers` config key). The mapper function itself (`expensiveHash`) is a
  fixed-iteration hash-mixing loop, deliberately expensive per element, so total sequential
  cost is measurable (tens to hundreds of ms) rather than dominated by JIT/dispatch noise. The
  reducer function is a trivial pass-through (every key is unique, so `reduceLeft` never
  actually combines anything), isolating the measurement to the map phase.

## Design limitations found via benchmarking

Not bugs — the classic-actor implementation behaves exactly as designed in each case below —
but real architectural characteristics the baseline numbers surfaced, worth carrying into the
typed-actors redesign discussion rather than patching on the code that's about to be replaced.

**1. Every map-reduce operation creates and tears down its own actors, every single time.**
`Master`'s constructor (`src/main/scala/.../core/Master.scala`) spins up a fresh `Mapper` and N
`Reducer` actors per instance; there's no reuse or pooling across separate operations —
`CountWords.apply()`, `WordCountBenchmark.wordCount()`, etc. all build `stage1`/`stage2` from
scratch on every call, use them once, and discard them. This is the single biggest cost the
benchmarks actually measured, not a hypothesis: at `Matrix` size=5, forcing the actor path was
~53× slower than sequential — almost entirely the fixed create-use-destroy cost, since the
actual computation at that size is trivial. It's also the likely reason `DataDefinition` never
crossed over even at size=10000 (actors still ~4× slower than sequential there) — the
per-element work is cheap enough that this fixed cost is never amortized. A long-lived,
reusable pool of actors that survives across separate operations (rather than one rebuilt per
call) would remove this cost for repeated/small-workload use, and is a natural fit for
something like Akka Typed's routers or cluster sharding.

**Partially resolved in 2.0.1.** A single `MapReduceFirst`/`Pipe`/`FirstFold`/`PipeFold`
instance already reused its Master/Mapper/Reducers across repeated `.apply()` calls — the cost
was entirely from *call sites* (`CountWords`, `WordCountBenchmark`) constructing a fresh
instance per top-level operation instead of reusing one. Fixed: `CountWords` now builds its
actors/pipeline once (in the case-class body, gaining an explicit `close()`), and
`WordCountBenchmark` gained a `reuseInstance` param making warm-vs-cold an explicit, measured
comparison in the same run (see "WordCountBenchmark's parameters" above). `DataDefinition`'s
per-`evaluate()` cost is a separate, harder problem: a `DataDefinition` bundles data with its
transform pipeline, so "reuse the same instance" doesn't apply the same way — closing this gap
would mean caching a Master/Mapper/Reducer set in `DDContext` keyed by pipeline shape (e.g.
closure `getClass`), which is a real correctness hazard (two evaluations sharing a `getClass`
but capturing different free variables could silently reuse a stale, wrong actor). Explicitly
deferred again, to a future release.

**2. Only the reduce side is parallelized — the map side isn't.**
`MasterBase`'s constructor creates exactly **one** `Mapper` actor
(`private val mapper = actors.createActor(context, Some(Master.sMpr), mapperProps)`) alongside
N reducer actors. The map phase runs entirely sequentially inside that single actor; only
reduction gets spread across a pool. For CPU-heavy mapping work this is a real, currently
unclaimed parallelism opportunity — the classic "map-reduce" name promises parallelism on both
sides, but only one side delivers it today.

**Fixed in 2.0.1.** `Master.scala` now spawns `mappers` (a new config key, default 4) mapper
actors instead of one, splits the incoming batch into at most `mappers` contiguous chunks
(`Master.splitIntoChunks`), fans out one `DoMap` per chunk, and merges the resulting
`MapperResponse`s back together (`Master.mergeMapperResponses` deep-merges same-key results and
concatenates exceptions — a key can land in more than one chunk). No changes were needed to
`Mapper.scala` itself; its `DoMap` handler was already stateless per message. `benchmarks/`'s
new `MapperParallelismBenchmark` (see above) demonstrates a real crossover with a deliberately
CPU-heavy mapper function: `mappers=1` vs. `mappers=4` on a 400-item batch measured ~163ms vs.
~88ms in a smoke run — none of the *other* benchmarks' mapper functions are expensive enough to
show this signal, which is why a dedicated benchmark was needed. As with reducers, raising
`mappers` above 1 can be a net loss for cheap mapper functions/small batches, due to `ask`
overhead — measure before increasing it.

**3. Reducer pool size is fixed by config, not by workload.**
The number of reducer actors comes from the `reducers` config key (default 4) regardless of
how much data exists or how many distinct keys it has. For a workload with only a handful of
keys, some of those actors never receive meaningful work; for a very large key space, 4 might
be too few. Sizing the pool relative to the actual workload (or making it elastic) would help
both ends of that range, rather than a single static default trying to serve all workload
shapes.

**Partially resolved in 2.0.1.** `DataDefinition`'s `partitions` argument previously did
nothing for reducer count at all — it was used only as a binary cutoff (sequential vs. actor
path), and the actor path always spawned whatever the shared, JVM-wide `DDContext` config
resolved to (4), regardless of the `partitions` value passed in. `LazyDD.evaluate` now builds a
per-call `Config` overlay from `partitions` (immutable, so the shared context is never mutated),
so `nReducers == partitions` directly. A second bug was found and fixed alongside this: the
`DataDefinition(map, partitions)` overload silently discarded `partitions` entirely, always
falling through to the single-argument overload's default. `DataDefinition.DefaultPartitions`
was bumped from 2 to 4, so callers who don't pass `partitions` explicitly see no behavior change
now that the value genuinely drives reducer count. True dynamic/elastic sizing (resizing
reducers based on each request's *discovered* key cardinality, rather than a value the caller
chooses up front) is explicitly deferred again — `Master`'s actors are spawned once at startup,
before any request arrives, and are meant to be reused across many subsequent requests with
different data shapes (see item 1); true elasticity would mean either reintroducing per-request
actor churn or picking a static upper-bound pool size, which just moves the same sizing problem
up one level.

## Running it

Basic run, everything from the annotations:

```bash
sbt "benchmarks/Jmh/run"
```

Override anything from the command line without touching the file — this is the normal
workflow, since editing annotations for every experiment gets old fast:

```bash
sbt "benchmarks/Jmh/run -i 10 -wi 5 -f3 -t1 -p documents=10,100,1000,10000 -p executors=4,8 .*WordCountBenchmark.*"
```

- `-i` measurement iterations, `-wi` warmup iterations, `-f` forks, `-t` threads
- `-p key=v1,v2,...` overrides a `@Param` field — lets you sweep values you didn't hardcode in
  the file
- the trailing regex selects which benchmark class(es)/method(s) to run (useful once there's
  more than one)

To save results for comparing against later numbers (e.g. after a redesign):

```bash
sbt "benchmarks/Jmh/run -rf json -rff benchmarks/results/baseline-classic-actors.json .*WordCountBenchmark.*"
```

`-rf json` picks the output format, `-rff` the file path. Use a real fork count (`-f3` or
`-f5`) once you're ready to treat the numbers as an actual baseline rather than a smoke test,
and check the resulting JSON into git alongside a note of which commit/JVM/machine it was
measured on — that context is what makes the number comparable months later.

## Smoke Testing

```bash
sbt "benchmarks/Jmh/run -i 1 -wi 1 -f1 -t1 -p documents=10 -p executors=4 .*WordCountBenchmark.*"
sbt "benchmarks/Jmh/run -i 1 -wi 1 -f1 -t1 -p size=5,50 -p forceActors=true,false .*MatrixBenchmark.*"
sbt "benchmarks/Jmh/run -i 1 -wi 1 -f1 -t1 -p size=100,10000 -p forceActors=true,false .*DataDefinitionBenchmark.*"
```

(Historical note: `MatrixBenchmark` and `DataDefinitionBenchmark` used to run noticeably slower
per fork than this, because `DataDefinition`'s global `ActorSystem` was never explicitly
terminated, so each fork paid a ~24s forced-exit timeout on top of actual measurement time.
Both benchmarks now call `DataDefinition.shutdown()` in `@TearDown(Level.Trial)`, which
eliminates that tax entirely.)

