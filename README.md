![Sonatype Central](https://maven-badges.sml.io/sonatype-central/com.phasmidsoftware/majabigwaduce_3/badge.svg?color=blue)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/1d777e04503246a991b88b9da4aea9ff)](https://app.codacy.com/manual/scalaprof/Majabigwaduce?utm_source=github.com&utm_medium=referral&utm_content=rchillyard/Majabigwaduce&utm_campaign=Badge_Grade_Dashboard)
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/rchillyard/Majabigwaduce/tree/master.svg?style=shield)](https://dl.circleci.com/status-badge/redirect/gh/rchillyard/Majabigwaduce/tree/master)
![GitHub Top Languages](https://img.shields.io/github/languages/top/rchillyard/Majabigwaduce)
![GitHub](https://img.shields.io/github/license/rchillyard/Majabigwaduce)
![GitHub last commit](https://img.shields.io/github/last-commit/rchillyard/Majabigwaduce/master)
![GitHub issues](https://img.shields.io/github/issues-raw/rchillyard/Majabigwaduce)
![GitHub issues by-label](https://img.shields.io/github/issues/rchillyard/Majabigwaduce/bug)


Majabigwaduce
=============

Majabigwaduce\* (aka _Akka/MapReduce_) is a framework for implementing map-reduce using [Scala](http://www.scala-lang.org)
and [Akka](http://akka.io) actors.
I tried to use only the intrinsic notion of map and reduce.
Thus, it is not exactly like Hadoop's map-reduce (although it is similar).

__Why__ would anyone want to do map-reduce using actors? It's a good question.
For me, it arose initially because I needed an example of using actors for the class I was teaching on Scala and Big Data.
I also wanted to ensure that the students understood the essence of map-reduce rather than some derived version of it.
Of course, it turns out that it's a perfect application for actors and indeed demonstrates many of the proper
techniques to be used when programming with (Akka) actors.

\* Majabigwaduce was the Native American name for the site of the battle of Penobscot Bay, Maine in 1779,
see [Penobscot Expedition](https://en.wikipedia.org/wiki/Penobscot_Expedition)

API
---

Full Scaladoc for every version published to Maven Central is generated automatically by
[javadoc.io](https://javadoc.io/doc/com.phasmidsoftware/majabigwaduce_3).

Getting Started
----------------

Majabigwaduce is published on Maven Central. Add it to your `build.sbt`:

```scala
libraryDependencies += "com.phasmidsoftware" %% "majabigwaduce" % "<version>"
```

(see the Sonatype Central badge at the top of this page for the current version number).

High-level API
---------------

DataDefinition
-------------
Majabigwaduce has a high-level API, something like that used by Spark.
The classes for this API can be found in the `dd` package.
It is based on the concept of `DataDefinition`, essentially a lazy, partitionable, map of key-value pairs.
`DataDefinition` is a trait with two concrete sub-classes:

* `LazyDD` is a lazily-evaluated `DataDefinition` and which is the normal implementation of `DataDefinition` to be used.
* `EagerDD` is an eagerly (i.e. fully) evaluated `DataDefinition` which also implements `HasEvaluatedMap` which provides
a method to yield directly the wrapped key-value map.
In practice, a `LazyDD` creates an `EagerDD` when implementing its `apply` method.

A `DataDefinition` (in these cases a `LazyDD`) is normally created with a statement such as:

```scala
val dd = DataDefinition(map, partitions)
```

where `map` is either a `Map[K,V]` or a `Seq[(K,V)]`; or

```scala
val dd = DataDefinition(list, f, partitions)
```

where `list` is a `Seq[V]` and where `f` is a function of type `V=>K` (the mapper function).

In all cases, `partitions` represents the desired number of partitions for the data definition,
but can be omitted, in which case it will default to 2.

There are three types of transformation function currently supported:

* `map` which, as expected, takes a function which maps a key-value pair into a new key-value pair.
* `filter` which, as expected, takes a predicate (i.e. a boolean function) which tests each key-value pair.
* `join` which implements an inner join of two `DataDefinition`s.

There are three types of "action" function:

* `apply` which yields a `Future[Map[K,V]]`
* `reduce(f)` which yields a `Future[W]` where the function `f` is the aggregation function
of type `(W,V)=>W` where `W` is constrained by a context bound: `W: Zero`.
* `count` which yields the number of key-value pairs in the `DataDefinition`, wrapped in `Future`.

An additional type `DDContext` is used implicitly when calling the `apply` methods of the `DataDefinition` object.

For an example of using this higher-level API, please see the `Matrix` class.
Because this class is useful in its own right, it can be found in the main source area under the `matrix` package.

Functional Map-Reduce (mid-level API)
=====================

The classes for this API (and anything lower) can be found in the `core` package.
The set of `Master` classes (lowest-level API) can be used by applications exactly as described below.
However, there is a more convenient, functional form based on the trait `MapReduce` which is defined thus:

```scala
trait MapReduce[T, K1, V1] extends ASync[Seq[T], Map[K1, V1]] with AutoCloseable:
  self =>

  // Compose this stage with a subsequent map-reduce stage.
  def :&[K2, V2](f: ASync[Seq[(K1, V1)], Map[K2, V2]]): MapReduce[T, K2, V2] =
    MapReduceComposed(self, f)(self.ec)

  // Alternative, symmetrical formulation of :&
  def &[K2, V2](mr: MapReduce[(K1, V1), K2, V2]): MapReduce[T, K2, V2] = :&(mr)

  // Terminate the pipeline with a Reduce, yielding a single value of a super-type of V1.
  def :|[S](r: RF[K1, V1, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] =
    ts => for (v2K2m <- self(ts); s = r(v2K2m)) yield s

  // Alternative name for :|
  def |[S](r: RF[K1, V1, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] =
    :|(r)(executionContext)

  def ec: ExecutionContext
```

This trait casts the map-reduce process as a simple function: one which takes a `Seq[T]` and results in a
(future of) `Map[K1,V1]` where `T` is either `V0` in the case of the first stage of a map-reduce pipeline or
`(Kn,Vn)` in the case of the subsequent (nth) stage.
There are four case classes which implement this trait (and which should be specified by the application programmer):

* `MapReduceFirst`
* `MapReducePipe`
* `MapReduceFirstFold`
* `MapReducePipeFold`

Additionally, there is the `MapReduceComposed` case class which is created by invoking the `:&` operator (or its
alias `&`). A pipeline of map-reduce stages is thus composed by chaining `:&`/`&` between stages.
Such a pipeline may be (optionally) terminated by combining it with a `Reduce` instance, using the `:|` operator
(or its alias `|`), which combines the values of the final `Map[Kn,Vn]` into a single `S` value (where `S` is a
super-class of `Vn`).

Thus, a pipeline in functional form is a closure which captures all the functions,
and their parameters which are in scope at the time of defining the pipeline.

See the `CountWords` example (below).

Lowest-level API
------------

In order for a calculation to be performed in parallel, it is necessary that the complete calculation can be broken up
into smaller parts which can each be implemented independently.
These parallel calculations are performed in the `reduce` phase of map-reduce while the `map` phase is responsible for
breaking the work into these independent parts.
In order that the results from the `reduce` phase can be collated and/or aggregated,
it is usually convenient for each portion of the calculation to be identified by a unique key
(we will call the type of these keys `K2`).
The data required for each portion is typically of many similar elements. We will call the type of these elements `W`.
Thus, the natural intermediate data structure (for the `shuffle` phase, see below) which results from
the `map` stage and is used as input to the `reduce` stage is:

```scala
Map[K2, Seq[W]]
```

Thus, the first job of designing an application to use map-reduce is to figure out the types `K2` and `W`.
If you are chaining map-reduce operations together, then the input to stage N+1 will be of the same form as the output of stage N.
Thus, in general, the input to the map-reduce process is a map of key-value pairs.
We call the type of the key `K1` and the type of the value `V1`.
So, the input to the map stage is, in general:

```scala
Map[K1, V1]
```

For the first stage, there is usually no appropriate key, so instead we pass in a message of the following form
(which is more or less equivalent to `Map[Unit,V1]`):

```scala
Seq[V1]
```

The reduction stage, as we have already seen, starts with information in the form of `Map[K2,Seq[W]]` and the work
is divided up and sent to each of the reducers.
Thus, each reducer takes as input (via a message) the following tuple:

```scala
(K2, Seq[W])
```

The result of each reduction is a tuple of the following form:

```scala
(K2, V2)
```

where `V2` is the aggregate of all the `W` elements.

Note that the reason that `W` is not called `V2` and `V2` is not called `V3` is because `W` is an internal type.
It is not a factor in the incoming or outgoing messages from the `Master`.

Of course, it's possible that there are insufficient reducers available for each of the keys.
The way this project deals with that situation is simply to start sending messages to the available actors again.
In general, the so-called `shuffle` phase which precedes the `reduce` phase is able to pick and choose how to make the
best match between the key value `k` and a particular reducer.
This might be based on locality of data referenced by the values in the sequence.
Or some other criterion with a view to load-balancing.
However, this project does not currently make any such decisions, so the `shuffle` phase is really non-existent:
messages (one per key) are simply sent out to reducers in sequence.

Low-level Details
=================

Master
------

The `Master` (or one of its three siblings) is the only class which an application needs to be concerned with.
The `Master`, itself an actor, creates a mapper and a number of reducers as appropriate at startup and destroys them at
the end.
The input message and the constructor format are slightly different according to which form of the `Master`
(see below) you are employing.

Since version 2.0.0, the actors in the `core` package (`Master`, `Mapper`, `Reducer`) are implemented using
Akka Typed rather than Akka Classic. `Master`/`Mapper`/`Reducer` are now factory functions which return a
typed `Behavior`, spawned with `context.spawn`/`system.systemActorOf` rather than constructed via `Props`.
Each has its own small, sealed command protocol (e.g. `MasterCommand`, with `ComputeMap`/`ComputeSeq`/`CloseMaster`
cases) whose "compute" cases carry an explicit `replyTo: ActorRef[...]`, and a `Close` case that stops the actor
(and, transitively, its children) -- there is no more implicit `sender()`, and no more `system.stop`. None of this
affects the mid-level (`MapReduce`) or high-level (`DataDefinition`) APIs described above, which are unchanged.

Generally, there are five polymorphic types which describe the definition of `Master`: `K1, V1, K2, W,` and `V2`.
Of these, `W` is not involved in messages going to or from the master--it is internal only.
And, again generally, the constructor for the `Master` takes the following parameters:

* `config: Config`
* `f: (K1,V1)=>Try[(K2,W)]`
* `g: (V2,W)=>V2`
* (optionally) `z: ()=>V2`

where

* `config` is used for various configuration settings, such as the number of reducers to be created;
* `f` is described in `Mapper` below
* `g` and `z` are described in `Reducer` below

There are actually four `Master` types to accommodate different situations.
The first map-reduce stage in a pipeline (as mentioned above) does not involve `K1`.
Therefore, two of the `Master` types are of this "first" type.
Next, there is a difference between the pure reducers which require that these are treated separately
(see section on `Reducer` below).
This creates another pairing of master forms: the "fold" variations.
Thus, we have four forms of `Master` all told:

* `Master`
* `Master_First`
* `Master_Fold`
* `Master_First_Fold`

The "fold" variations require the `z` parameter, whereas the other variations do not.
Thus, the non-"fold" variations require that `V2` be a super-type of `W` (as required by `reduceLeft`).

The "first" variations do not require a `K1` to be defined (it defaults to `Unit`) and see below in `Mapper`
for the difference in input message types.

The __input message__ type for the "first" variations is: `Seq[V1]` while the input message type for the non-"first"
variations is `Map[K1,V1]`.

The __output message__ type is always `Response[K2,V2]`.
The `Response` type is defined thus:

```scala
case class Response[K, V](left: Map[K, Throwable], right: Map[K, V]):
  def size: Int = right.size
```

where `K` represents `K2` and `V` represents `V2`.
As you can see, the results of applying the reductions are preserved whether they are successes or failures.
The `right` value of the response is the collation of the successful reductions,
while the `left` value represents all the exceptions that were thrown (with their corresponding key).

Note that the mid-level classes described above (`MapReduceFirst`, `MapReducePipe`, `MapReduceFirstFold`,
`MapReducePipeFold`) each provide a `create` method in their companion object which you can use if your mapper
function is of the form `V1=>(K2,W)` (or `(K1,V1)=>(K2,W)`), that's to say, without the `Try`.
The low-level `Master` classes themselves do not currently provide an equivalent convenience constructor --
you must wrap your mapper function in a `Try` (or use `FP.lift`) yourself.

Mapper
-----

`Mapper` is a factory function which builds a typed actor `Behavior`.
In general, the `Mapper` takes the following polymorphic types: `[K1,V1,K2,W]`.

The constructor takes a function `f` of type `(K1,V1)=>Try[(K2,W)]`,
that's to say it is a function which transforms a `(K1,V1)` tuple into a `Try` of `(K2,W)` tuple.

The incoming message is of the form:
`KeyValuePairs[K,V]` where `KeyValuePairs` is essentially a wrapper around the input (but in sequence/tuple form) and is defined thus:

```scala
case class KeyValuePairs[K, V](m: Seq[(K, V)])
```

Where, in practice, `K=K1` and `V=V1`. For the first-stage map-reduce processes, `K1` is assumed to be `Unit`.
Therefore, you can see the reason for making the input in the form of a wrapper around `Seq[(K1,V1)]`.
If the keys are unique then this is 100% two-way convertible with a `Map[K1,V1]`.
However, since the `K1` keys can sometimes be missing entirely, we cannot properly form a `Map`.
A `Map` can always be represented as `Seq[Tuple2]`, however.

It makes sense that the output from the reducer phase and, ultimately the master,
records both successful calls to the reducer and failures.
This follows from the independent nature of the "reduce" phase.
But, what about errors in the mapper phase?
If the mapper fails on even one input tuple, the entire mapping process is pretty much trashed.
What would be the point of continuing on to do the "reduce" phase after a mapper error?
That is indeed the normal way of things: if there are any failures in mapping, the whole mapping fails.
The form of (successful) output is `Map[K2,Seq[W]]` while any failure outputs a `Throwable`
(this is all part of the `Future` class behavior).

Nevertheless, there is a "forgiving" mode (set `forgiving` to true in the configuration) in which the
master will accept both the successful output and a sequence of `Throwable` objects from the mapper, rather
than treating any mapper exception as an overall failure. (`Mapper_Forgiving` is kept as a separate name for
source compatibility, but it now behaves identically to `Mapper` -- the strict-vs-forgiving decision is made
by the master, based on this configuration setting, not by the mapper itself.)

Reducer
-------

`Reducer` is a factory function which builds a typed actor `Behavior`.
In general, the `Reducer` takes the following polymorphic types: `[K2,W,V2]`.

The constructor takes a function `g` of type `(V2,W)=>V2`,
that's to say it is a function which recursively combines an accumulator of type `V2` with an element of type `W`,
yielding a new value for the accumulator.
That's to say, `g` is passed to the `reduceLeft` method of `Seq`.

The incoming message is of the form: `Intermediate[K2,W]` where `Intermediate` is essentially a wrapper around the
input and is defined thus:

```scala
case class Intermediate[K2, W](k2: K2, ws: Seq[W])
```

There is an alternative form of reducer: `Reducer_Fold`.
This type is designed for the situation where `V2` is *not* a super-type of `W` or
where there is no natural function to combine a `V2` with a `W`.
In this case, we must use the `foldLeft` method of `Seq` instead of the `reduceLeft` method.
This takes an additional function `z` which is able to initialize the accumulator.

Configuration
============

Configuration is based on Typesafe `Config` (as is normal with Akka applications).
Please see the `reference.conf` file in the `main/resources` directory for the list of
configurable parameters with their explanations.

Dependencies
============

The components that are used by this project are:

* Scala (3.3.x)
* Akka Typed (2.8.x)
* Typesafe Configuration (1.4.x)
* ...and dependencies thereof

Testing
=======

There are two directories (under `src`) for testing: `test` (unit tests/specifications) and `it` (integration tests).
By default, all tests are in the classpath.
The example applications are in the `it` directory, given that they are not really unit tests.
If you wish to suppress the integration tests temporarily, simply un-mark `it/scala` as a test source root.
Or, you could comment out the appropriate entry (`unmanagedSourceDirectories`) in `build.sbt`.

Examples
========

There are several examples provided (in the `src/it/scala/com/phasmid/majabigwaduce/examples` directory):

* CountWords: a simple example which counts the words in documents and can provide a total word count of all documents.
* WebCrawler: a more complex version of the same sort of thing.
* Matrix: uses the high-level API.
* MatrixOperation: demonstrates matrix operations.

CountWords
----------

Here is the `CountWords` app.
It actually uses a "mock" URI rather than the real thing, but of course, it's simple to change it to use real URIs.
Here is a simplified version (see the current code for the full version, including imports and configuration lookup):

```scala
case class CountWords(resourceFunc: String => Resource)
    (using system: ActorSystem[Nothing], logger: Logger, config: Config, timeout: Timeout, ec: ExecutionContext)
  extends (Seq[String] => Future[Int]) {

  type Strings = Seq[String]

  trait StringsZeros extends Zero[Strings] {
    def zero: Strings = Nil: Strings
  }
  implicit object StringsZeros extends StringsZeros

  trait IntZeros extends Zero[Int] {
    def zero: Int = 0
  }
  implicit object IntZeros extends IntZeros

  override def apply(ws: Strings): Future[Int] =
    given actors: Actors = Actors(summon[ActorSystem[Nothing]], summon[Config])
    val stage1 = MapReduceFirstFold.create(
      { (w: String) => val u = resourceFunc(w); (u.getServer(), u.getContent()) },
      appendString
    )(actors, timeout)

    val stage2 = MapReducePipe.create[URI, Strings, URI, Int, Int](
      (w, gs) => w -> (countFields(gs) reduce addInts),
      addInts,
      1
    )
    val stage3 = Reduce[URI, Int, Int](addInts)
    val mr = stage1 & stage2 | stage3
    mr(ws)

  private def countFields(gs: Strings) = for (g <- gs) yield g.split("""\s+""").length
  private def addInts(x: Int, y: Int) = x + y
  private def appendString(a: Strings, v: String) = a :+ v
}
```

It is a three-stage map-reduce problem, including a final reduce stage.

Stage 1 takes a `Seq[String]` (representing URIs) and produces a `Map[URI,Seq[String]]`.
The mapper for the first stage returns a tuple of the `URI` (corresponding to the server for the string),
and the content of the resource defined by the string.
The reducer simply adds a `String` to a `Seq[String]`.
There is additionally an `init` function which creates an empty `Seq[String]`.
The result of the first stage is a map of `URI->Seq[String]` where the key represents a server,
and the value elements are the contents of the documents read from that server.

Stage 2 takes the result of the first stage and produces a `Map[URI,Int]`.
The second stage mapper takes the `URI` and `Seq[String]` from the first stage and splits each string on white space,
getting the number of words, then returns the sum of the lengths.
In practice (if you are using just the three default args), these sequences have only one string each.
The second stage reducer simply adds together the results of the mapping phase.
The result of stage 2 is a map of `URI->Int`.

Stage 3 (a terminating stage which produces simply a value) takes the map resulting from stage 2,
but simply sums the values (ignoring the keys) to form a grand total.
This resulting value of `Int` is printed using `println`.

Note that the first stage uses `MapReduceFirstFold`, the second stage uses `MapReducePipe`,
and the third (terminating) stage uses `Reduce`.

If the names of variables look a bit odd to you, then see my "ScalaProf" blog:
http://scalaprof.blogspot.com/2015/12/naming-of-identifiers.html

WebCrawler
----------

Here is the shape of the web crawler example app (private helper methods are omitted below for brevity --
see `WebCrawler.scala` in the `it` source tree for the full implementation):

```scala
case class WebCrawler(depth: Int)(implicit system: ActorSystem[Nothing], config: Config, timeout: Timeout, ec: ExecutionContext)
  extends (Strings => Future[Int]):

  private val logger: Logger = LoggerFactory.getLogger(classOf[WebCrawler])

  val actors: Actors = Actors(system, config)

  val stage1: MapReduce[String, URI, Strings] =
    MapReduceFirstFold(getHostAndURI, g)(actors, timeout)
  val stage2: MapReduce[(URI, Strings), URI, Strings] =
    MapReducePipeFold.create(getLinkStrings, joinWordLists, 1)(actors, timeout)
  val stage3: Reduce[URI, Strings, Strings] =
    Reduce[URI, Strings, Strings](_ ++ _)
  val crawler: Strings => Future[Strings] = stage1 & stage2 | stage3

  override def apply(ws: Strings): Future[Int] =
    doCrawl(ws, Nil, depth) transform (
      n => { val z = n.length; system.terminate(); z },
      x => { logger.error("Map/reduce error (typically in map function)", x); x }
    )

  // getHostAndURI, g, getLinkStrings, joinWordLists, and the recursive doCrawl driver
  // are omitted here -- see WebCrawler.scala for the full implementation.
```

The application is somewhat similar to the `CountWords` app,
but because of the much greater load in reading all the documents at any level of recursion,
the first stage performs the actual document reading during its reduce phase.
However, it also has three stages.

The three stages combined as a pipeline called `crawler` are invoked recursively by the method `doCrawl`.

Because you cannot predict in advance what problems you will run into with badly formed (or non-existent) links,
it is better to run this app in forgiving mode.
Expect about 250 links to be visited given the default value of `ws` and depth of 2.

Future enhancements
===================

* Enable the shuffle process to match keys with reducers according to an application-specific mapping
(rather than the current, arbitrary, mapping).
* Enable reducers (and possibly mappers) to be replicated across a cluster.

Revision History
================

* 1.0.0 Earliest version (for 2.12)
* 1.0.1 Version compatible with 2.13
* 1.0.2 Code cleanup and issues fixes.
* 1.0.3 More cleanup (TODOs, etc.)
* 1.0.4 Refactored package structure and moved `examples` into `it` directory.
* 1.0.5 Upgraded dependencies, added Flog, added more badges.
* 1.1.0 Migrated to Scala 3; added the high-level `DataDefinition` API, the `matrix` package, and JMH benchmarks.
* 2.0.0 Migrated the `core` actors (`Master`, `Mapper`, `Reducer`) from Akka Classic to Akka Typed. See
  `doc/TypedActorsMigration.md` for the design rationale. The mid-level (`MapReduce`) and high-level
  (`DataDefinition`) APIs are unaffected.
