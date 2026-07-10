# Majabigwaduce: An Assessment

*2026-07-10*

This is an outside assessment of Majabigwaduce along two axes: its value as a
pedagogical tool for CSYE7200, and its value as a general-purpose library,
independent of the course. It follows a hands-on session adding test coverage
to the `core` package and fixing two bugs the new tests surfaced — that work
is not narrated here, but it's the basis for several of the specific claims
below.

## As a pedagogical tool for CSYE7200

The three-tier API structure — raw `Master`/`Mapper`/`Reducer` actors in
`core`, the functional `MapReduce` composition layer on top of it, and the
Spark-like `DataDefinition` abstraction in `dd` above that — is a genuinely
good teaching progression. Most courses hand students a DataFrame-style API
and never show what's underneath it. This lets students build the mental
model bottom-up: see the actual message-passing between actors first, then
watch it get wrapped in successively nicer abstractions. That ordering
matters pedagogically; it's much easier to appreciate why an abstraction
exists once you've felt the friction it's removing.

The library's honesty about partial failure is also a real strength.
The `Response(left, right)` result type and the forgiving-vs-strict
configuration flag force students to confront the fact that in a
distributed (or even just concurrent) computation, individual pieces can
fail independently of the whole — a lesson that a lot of introductory
treatments quietly skip by assuming every map succeeds.

There's a specific class of bug in the codebase — a mismatched mapper
function producing a `ClassCastException` deep inside an actor's mailbox,
several layers removed from the line of code that actually caused it —
that is, honestly, better teaching material than most of the polished
examples. It's a clean, self-contained demonstration of exactly why
untyped Akka combined with reflection-based `Props.create` construction
is dangerous: a type mismatch that would be a compile error in ordinary
Scala instead surfaces as a runtime crash inside a message handler, and
by default the actor's supervision strategy swallows it silently (restart,
no reply) unless the code explicitly guards against it. I'd turn this into
an explicit lecture or lab example rather than leaving it as something
students only encounter if they go looking — it motivates Akka Typed (or
any type-safe alternative) far more viscerally than being told "erasure is
dangerous" in the abstract.

Two rougher edges are worth naming directly:

- **Test coverage was thin in exactly the places students learn from by
  example.** Core pieces of the actor machinery had little or no direct
  test coverage before this pass. That matters more in a teaching codebase
  than in a typical application, because the tests double as executable
  documentation — sparse tests quietly teach "this corner doesn't matter,"
  which usually isn't the intended lesson.
- **At least one integration test was structurally incapable of failing.**
  It fired off an asynchronous operation and made its assertions inside a
  callback that the test never waited for, so the test function returned
  (and reported success) before the assertions had any chance to run. This
  is a very easy pattern to write by accident and an easy one for a student
  to copy without noticing the flaw — worth an explicit audit of the rest of
  the integration suite for the same shape.
- **Class proliferation adds incidental complexity.** `Master`,
  `Master_Fold`, `Master_First`, and `Master_First_Fold` (crossed with
  `ByReduce`/`ByFold` traits), and the parallel `MapReduceFirst`/`Pipe`/
  `FirstFold`/`PipeFold` foursome, are really just two independent boolean
  axes — fold-vs-reduce, first-stage-vs-pipe-stage — expanded into roughly
  eight named classes. That's realistic of how a growing Scala/Akka codebase
  tends to organize itself, but if the pedagogical goal is "understand
  map-reduce," this variant proliferation is more surface area for students
  to hold in their heads than the underlying lesson strictly requires. It
  might be worth being explicit in lecture that these are two flags, not
  eight independent ideas — and being equally explicit about *why* the
  codebase didn't just parameterize over them (a decent lesson in itself
  about the limits of mixing inheritance with cross-cutting concerns in
  Scala).
- **The reflection-based construction pattern deserves an explicit warning
  label.** `Props.create(classOf[...], args...)` sidesteps Scala's type
  system entirely, which is how the classic (untyped) actor model works —
  but a student who copies this pattern without the accompanying warning
  might reasonably conclude it's how you'd build such a thing today, when
  in fact it's specifically the design Akka Typed exists to replace.

## As a general-purpose library

Majabigwaduce is a real, published artifact — it's on Maven Central as
`com.phasmidsoftware %% majabigwaduce`, not merely course-internal code —
so it's fair to ask whether it earns a place in someone's `build.sbt` for
reasons unrelated to CSYE7200.

The `dd` (`DataDefinition`) package is the strongest case for "yes." It's a
small, typed, `Monoid`-driven lazy map/filter/join/reduce abstraction. If
the goal is Spark-like ergonomics for parallel computation on a single JVM,
without the overhead of standing up a cluster, that's a legitimate — if
niche — need, and this fills it reasonably well.

The lower two tiers are harder to recommend for use outside the course.
They solve "parallel map-reduce within one process," and Scala's own
parallel collections, `Future.traverse`, or Akka Streams already solve that
problem with less ceremony and, notably, with compile-time type safety
instead of reflection-based actor construction. It's also worth noting that
despite `akka-cluster` and `akka-remote` appearing as dependencies, nothing
in the MapReduce core actually uses clustering — every actor lives in one
local `ActorSystem`. So "map-reduce using actors" doesn't, in its current
form, provide the thing real map-reduce frameworks exist to provide: fault
tolerance across machines, shuffle at scale, spilling to disk when data
doesn't fit in memory. It's architecturally interesting as a design, not
infrastructure you'd reach for to handle real data volumes.

The specific defects surfaced by testing this session — a mapper that
could silently crash instead of replying when given a badly-typed function,
fold/first `Master` variants that had never actually been exercised by any
test, an actor-naming scheme whose uniqueness guarantee only holds across
distinct `Actors` instances rather than within one — are not evidence that
the design is broken. They're evidence of corners nobody had reason to
visit yet, which is exactly what you'd expect from code that is primarily
pedagogical and only occasionally pressed into service as a real
dependency.

## Verdict

As a teaching artifact, this is a strong design, arguably strengthened
rather than weakened by exposing its rough edges to students rather than
quietly sanding them down — the rough edges *are* the lessons. As a
library, I'd reach for the `DataDefinition` layer for small, single-JVM
parallel workloads; I would not reach for it, or recommend a student reach
for it, for anything that needs to survive real production data volumes or
genuine distributed failure modes. For that, the honest answer is still
Spark, Flink, or Akka Streams.
