# Majabigwaduce: Classic Actors → Akka Typed Migration

*2026-07-11*

This document records the design and outcome of migrating Majabigwaduce's
core actor machinery from Akka Classic to Akka Typed, on the `TypedActors`
branch (version 2.0.0). It was written as an implementation plan before the
work started and has been updated to reflect what was actually built,
including a couple of corrections that only surfaced once the code was
written.

## Context

Majabigwaduce was, until this migration, built entirely on Akka Classic
actors (`akka-actor` 2.8.8). Classic actors are untyped — every
`receive: PartialFunction[Any, Unit]` pattern-matches on `Any`, requiring
`@unchecked` annotations to work around generic type erasure, and replies
flow through an implicit, unenforced `sender()` reference rather than an
explicit part of the message. Akka Typed removes both problems by
construction: every actor has exactly one declared message type, checked at
compile time, and replies are carried explicitly as an `ActorRef[Response]`
field on the request message itself.

The migration was scoped as follows:

- **Strategy: big-bang replacement.** Rewrite `core/*.scala` in place (same
  package, same file names), not a parallel/coexisting package.
- **Scope: protocol translation only, first.** Preserve the current
  architecture and behavior as closely as possible — same per-operation
  actor lifecycle (actors created fresh per operation, not pooled), same
  single-`Mapper`-actor design, same fixed reducer count from config. Three
  previously-documented design limitations (actor pooling/reuse, map-side
  parallelism, workload-aware reducer sizing — see `benchmarks/README.md`,
  "Design limitations found via benchmarking") were explicitly **out of
  scope** here and remain separate follow-on work. Keeping this phase pure
  protocol-translation means the classic-vs-typed benchmark comparison stays
  clean — any measured difference is attributable to typed vs. classic
  message dispatch alone, not tangled up with architecture changes.

The outcome: a fully typed-actor core, all existing tests passing (migrated
onto `ActorTestKit`/`TestProbe`), and a `benchmarks/results/baseline-typed-actors.json`
run under the same JMH settings as the classic baseline, showing no
order-of-magnitude regression.

## Key design decisions

**Top-level actor creation — `ActorSystem[T].systemActorOf`, not `SpawnProtocol`.**
`MapReduce_Base` creates its `Master` actor synchronously, at construction
time, from outside any actor context. Akka's generally-recommended idiom for
"spawn a top-level actor from outside the guardian" is `SpawnProtocol`, but
that mechanism is inherently asynchronous (ask-based, returns
`Future[ActorRef[U]]`) — adopting it would have forced Master construction to
become async, a real architecture change that would have contradicted
"preserve behavior as closely as possible." `ActorSystem[T].systemActorOf(behavior, name)`
is synchronous (returns `ActorRef[U]` directly) and is explicitly documented
by Akka as intended for **library** use, which preserved `MapReduce_Base`'s
exact shape.

**Activated the existing (previously dead) `Close` protocol as the real
shutdown mechanism.** `system.stop(master)` has no Typed equivalent for a
`systemActorOf`-created actor. `MapReduceActor.scala` already defined an
`object Close` message and a `receive` case handling it, but it was dead in
production — every real caller stopped the master via `system.stop` instead.
Each actor's new command protocol now includes a `Close` case (e.g.
`CloseMaster`, `CloseMapper`, `CloseReducer`) that returns `Behaviors.stopped`,
which stops the actor's children transitively, exactly matching what
`system.stop` used to do.

**Each actor got a small sealed command protocol carrying an explicit
`replyTo`.** Concretely:
- **Mapper**: `MapperCommand` = `DoMap(kvs, replyTo: ActorRef[MapperResponse[K2,W]])` | `CloseMapper`.
  `MapperResponse[K2,W](result: Map[K2,Seq[W]], exceptions: Seq[Throwable])`
  is now always sent, in both strict and forgiving modes — the old
  `Responder`/`prepareResponse` machinery that decided *inside the Mapper*
  whether a non-empty exception list constituted an overall failure has been
  removed. That decision now lives entirely in `Master.doMap`, which already
  had (and still has) the equivalent check. `Mapper_Forgiving` is now simply
  an alias for `Mapper`, kept only for source compatibility with existing
  call sites — with the strict/forgiving decision moved to the caller, there
  is no remaining behavioral difference between the two names.
- **Reducer**: `ReducerCommand` = `DoReduce(i: Intermediate[K2,W], replyTo: ActorRef[ReduceResult[K2,V2]])` | `CloseReducer`.
  (Named `DoReduce`, not `Reduce`, to avoid colliding with the unrelated
  `Reduce[K,T,S]` reduction-composition class in `MapReduce.scala`.)
  `ReduceResult[K2,V2](k2, result: Either[Throwable,V2])` replaces the old
  raw reply tuple.
- **Master**: `MasterCommand[K1,V1,K2,V2]` = `ComputeMap(m: Map[K1,V1], replyTo)` |
  `ComputeSeq(s: Seq[(K1,V1)], replyTo)` | `CloseMaster`, mirroring the two
  message shapes the classic `Master.receive` matched on (a bare `Map` is
  still supported for direct/low-level callers that bypass the public
  `MapReduce` API, such as the test suite and `MapReduceFuncSpec`).
  `Master_First`/`Master_First_Fold` reuse this *same* protocol with `K1 =
  Unit`, exactly as the classic `MasterBaseFirst` reused `MasterBase[Unit,...]`
  internally — their factory functions unitize the incoming `f` and callers
  pair each `V1` with a `Unit` key before sending `ComputeSeq`. This let the
  `ByReduce`/`ByFold` traits (which existed only to pick a reducer's `Props`)
  be deleted outright: `Master`/`Master_Fold`/`Master_First`/`Master_First_Fold`
  are now four small factory functions that all delegate to one shared
  `Master.behavior`, differing only in which reducer `Behavior` they spawn
  (`Reducer` vs. `Reducer_Fold`) and whether `K1` is unitized.

**Correction found during implementation: replies had to become `Try[Response[K,V]]`, not a bare `Response`.**
The original plan assumed dropping `akka.actor.Status.Failure` was free —
"a failed ask just fails the returned Future the same way `.mapTo[Response]`
did." That is not quite true: a Typed reply can *only* ever be the declared
type. If Master simply didn't reply on failure, the caller's ask would time
out (`AskTimeoutException`) instead of failing with the *original* exception
— a real behavior change, and one that would have broken tests asserting on
specific exception types (`MapReduceException`, `ArithmeticException`,
`ClassCastException`). The fix: every `MasterCommand`'s `replyTo` is typed as
`ActorRef[Try[Response[K2,V2]]]`, and `MapReduce_Base.apply` does
`Future.fromTry` on the result. This preserves the exact original failure
behavior — the caller's future fails with the real exception — while still
being a value that fits Typed's "one declared reply type" model.

**`sender()`-capture-before-`Future.onComplete` disappeared outright.**
`MasterBase`/`MasterBaseFirst`'s `val caller = sender() // XXX: this looks
strange but it is required` existed only because classic's `sender()`
becomes stale inside an async callback. `replyTo` is an ordinary immutable
value captured by normal closure semantics, and `ActorRef.tell` is
thread-safe from any thread, so this workaround is simply gone.

**`Actors` wrapper kept its role, retyped `createActor` over the spawn
mechanism.** Typed has no shared `ActorRefFactory` supertype for "system" vs.
"context", so `createActor[U](spawn: (Behavior[U], String) => ActorRef[U], maybeName, behavior): ActorRef[U]`
now abstracts over *how* something is spawned; callers pass
`(b, n) => context.spawn(b, n)` for children or `(b, n) => system.systemActorOf(b, n)`
for the top-level Master. Logging (`Actors.logException`, actor-creation
debug logs) moved from `system.log` (classic-only) to a plain SLF4J
`Logger`, since Typed's `ActorSystem[T]` has no system-level logger. The same
`system.log` → SLF4J swap was applied to every exemplar's client-side logging
(`CountWords`, `WebCrawler`, `MatrixOperation`).

**`MapReduceActor`'s shared lifecycle became a composition helper, not a
class hierarchy.** `MapReduceActor.withLifecycle(logger)(makeHandler)` wraps
a command handler with the same debug-level start/stop logging the old
abstract `Actor` base class provided via `preStart`/`postStop`. `makeHandler`
receives the `ActorContext` once, at actor start, so it can perform one-time
setup — this is what lets `Master` spawn its mapper and reducer children
exactly once, inside `Behaviors.setup`, mirroring the old constructor-time
side effects in `MasterBase`'s class body.

**Typed `ask` needs a `given Scheduler`, not an `ExecutionContext`.**
`master.ask[Try[Response[K,V]]](replyTo => ComputeSeq(ts.map(toMasterPair), replyTo))`
via `akka.actor.typed.scaladsl.AskPattern`, requiring a given
`akka.actor.typed.Scheduler` (derived from `actors.system.scheduler`)
alongside the existing `Timeout`.

**`DataDefinition`'s `ActorSystem` needed a root guardian behavior.**
`ActorSystem[T]`'s constructor requires a root `Behavior[T]`; since this
system exists purely to host ad-hoc `systemActorOf` children, it now uses
`ActorSystem(Behaviors.empty, name)`.

## What changed, file by file

- `core/MapReduceActor.scala` — abstract class replaced by the
  `withLifecycle` composition helper; `Responder`/`prepareResponse`/`Close`
  removed (superseded by per-actor Close cases and the unified
  `MapperResponse`); `CleanerCollector` kept as a plain function.
- `core/Mapper.scala` — `MapperCommand`/`DoMap`/`CloseMapper`; unified
  `MapperResponse`; `Mapper_Forgiving` is now an alias.
- `core/Reducer.scala` — `ReducerCommand`/`DoReduce`/`CloseReducer`;
  `ReduceResult` replaces the raw reply tuple.
- `core/Master.scala` — `MasterCommand`/`ComputeMap`/`ComputeSeq`/`CloseMaster`;
  `Behaviors.setup`-based mapper/reducer spawning; `ByReduce`/`ByFold` traits
  deleted; `Master.zero()`'s unused-`z`-for-non-fold quirk preserved as-is
  (this was a pre-existing wart, not something this phase set out to fix).
- `core/Actors.scala` — `createActor` retyped over the spawn function; SLF4J
  logging.
- `core/MapReduce.scala` — all six `Master*` construction sites now build a
  `Behavior[MasterCommand[...]]`; `MapReduce_Base.apply`/`close` use typed
  `ask` and `Close`-message-based shutdown.
- `dd/DataDefinition.scala` — typed `ActorSystem[Nothing]` with a
  `Behaviors.empty` guardian; `shutdown()` unchanged.
- `matrix/Matrix.scala`, `benchmarks/MatrixBenchmark.scala`,
  `benchmarks/DataDefinitionBenchmark.scala` — **needed zero changes**,
  confirming they were already fully insulated through `DataDefinition`'s
  public API.
- Exemplars (`CountWords`, `WebCrawler`, `MatrixOperation`) and
  `MapReduceFuncSpec.scala` (a consumer that talks to `Master` directly,
  bypassing the public API, so it has the same blast radius as the
  exemplars) — retyped `ActorSystem`, `system.log` → SLF4J.
- `benchmarks/WordCountBenchmark.scala` — same mechanical changes as the
  exemplars.
- Core unit tests (`MapperSpec`, `ReducerSpec`, `MasterSpec`, `ActorsSpec`,
  `MapReduceActorSpec`) — migrated to `ActorTestKit`/`TestProbe`.
  `ActorsSpec`'s `logException` tests now capture output with a Logback
  `ListAppender` directly (Akka's `LoggingTestKit` only observes log events
  published through the actor system, and `Actors.logException` now logs via
  plain SLF4J). `MapReduceActorSpec` no longer tests a shared base-class
  instance (there isn't one); it tests `withLifecycle` directly against a
  minimal probe protocol.

## A test that could no longer be expressed the same way

`MapReduceFuncSpec`'s "fail because mapper is incorrectly defined" test
exercised a genuine `ClassCastException`: classic's `Props.create(classOf[Master...], ...)`
takes constructor arguments as `Any*`, so passing a mapper function of the
wrong shape (missing the `Try` wrapper, wrong arity) bypassed all
compile-time type checking, and the mismatch only surfaced at runtime, deep
inside `Try(...).flatten`. With Typed, `Master`/`Master_First_Fold` are
ordinary type-checked factory functions — passing a wrong-shaped mapper
function is now a **compile error**, not a runtime exception. This is one of
the concrete benefits of the migration, but it also means the original test
can't be reproduced as a runtime assertion; it was replaced with a
`shouldNot compile` check.

## Verification

- All 148 tests pass (16 suites, 0 failures, 2 ignored — same ignore count
  as before the migration), including the live-network `WebCrawlerSpec`
  integration test.
- No `@unchecked`/`asInstanceOf` casts remain in the migrated actor
  protocols — the two casts in the old `Master` (`iToMapper[Z: ClassTag]`,
  `doReductionAsync`'s tuple cast) are gone, since Typed enforces the
  message types at compile time instead.
- JMH baseline comparison (`-f3 -i5 -wi3 -r2 -w2`, same as the classic run),
  `benchmarks/results/baseline-typed-actors.json` vs.
  `benchmarks/results/baseline-classic-actors.json`: no order-of-magnitude
  regression on any benchmark. Most deltas are small and bidirectional
  (roughly ±5–35%), consistent with normal run-to-run noise plus the small,
  real per-message overhead of Typed's dispatch mechanism. One data point
  (`DataDefinitionBenchmark.filterMapReduce`, `forceActors=true, size=1000`)
  showed a larger +73% delta, but its own error bar is ±57%, so this reads as
  measurement noise rather than a real regression.

## Explicitly out of scope for this phase

(Documented in `benchmarks/README.md` already — restated here so this
migration isn't conflated with fixing them.)

- Actor pooling/reuse across separate map-reduce operations.
- Map-side parallelism (currently exactly one `Mapper` actor regardless of
  reducer count).
- Workload-aware reducer pool sizing (currently a fixed config value
  regardless of data shape).
