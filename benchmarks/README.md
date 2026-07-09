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
- Benchmark classes live under `benchmarks/src/main/scala/com/phasmid/majabigwaduce/benchmarks/`,
  e.g. `WordCountBenchmark.scala`.

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
  not a zipped/paired sweep. `WordCountBenchmark` has three: `documents` (3 values by default),
  `keyCardinality` (1), `reducers` (1) — that's 3 × 1 × 1 = 3 full runs. Add a second value to
  `keyCardinality` and a second to `reducers` and it becomes 3 × 2 × 2 = 12 runs; the cost of
  sweeping multiple parameters compounds fast, which is worth keeping in mind before adding a
  fourth `@Param` on a whim.
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
- **`keyCardinality`** — number of distinct grouping keys ("servers") the documents are folded
  under in stage 1. This mirrors the original `CountWords` exemplar, where real documents came
  from actual HTTP servers (`bbc.com`, `cnn.com`, `default`) and got grouped by which server
  they came from; here it's a synthetic stand-in for "how many distinct keys does the data
  have," independent of how much parallelism is applied to reduce them.
- **`reducers`** — number of reducer actors `Master` spins up to parallelize the reduce stage
  (set via the `reducers` config key that `Master` reads at construction time). This is
  actor-pool parallelism, a different concern from `keyCardinality` — they used to be
  conflated under one `servers` parameter, which meant you couldn't isolate "does more data-key
  cardinality slow things down" from "does more reducer parallelism speed things up," since
  they always moved together. Splitting them lets you vary each independently.

## Running it

Basic run, everything from the annotations:

```bash
sbt "benchmarks/Jmh/run"
```

Override anything from the command line without touching the file — this is the normal
workflow, since editing annotations for every experiment gets old fast:

```bash
sbt "benchmarks/Jmh/run -i 10 -wi 5 -f3 -t1 -p documents=10,100,1000,10000 -p keyCardinality=4,8 -p reducers=4,8 .*WordCountBenchmark.*"
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
sbt "benchmarks/Jmh/run -i 1 -wi 1 -f1 -t1 -p documents=10 -p keyCardinality=4 -p reducers=4 .*WordCountBenchmark.*"
```

