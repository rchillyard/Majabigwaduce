[![CircleCI](https://circleci.com/gh/rchillyard/Majabigwaduce.svg?style=svg)](https://circleci.com/gh/rchillyard/Majabigwaduce)

Majabigwaduce
=============

Majabigwaduce\* (aka _Akka/MapReduce_) is a framework for implementing map-reduce using [Scala](http://www.scala-lang.org) and [Akka](http://akka.io) actors.
I tried to use only the intrinsic notion of map and reduce. Thus is is not exactly like Hadoop's map-reduce (although it is similar).

__Why__ would anyone want to do map-reduce using actors? It's a good question. For me, it arose initially because I needed an example of using actors for the class I was teaching on Scala and Big Data.
I also wanted to ensure that the students understood the essence of map-reduce rather than some derived version of it.
Of course, it turns out that it's a perfect application for actors and indeed demonstrates many of the proper techniques to be used when programming with (Akka) actors.

\* Majabigwaduce was the Native American name for the site of the battle of Penobscot Bay, Maine in 1779, see [Penobscot Expedition](https://en.wikipedia.org/wiki/Penobscot_Expedition)

API
---

Quick link to the [API](https://github.com/rchillyard/Majabigwaduce/tree/master/api/index.html) [TODO: This needs to be fixed]

High-level API
---------------

DataDefintion
-------------
Majabigwaduce has a high-level API, something like that used by Spark.
It is based on the concept of _DataDefinition_, essentially a lazy, partitionable, map of key-value pairs.
A DataDefinition is normally created with a statement such as:

    val dd = DataDefinition(map, partitions)
    
where _map_ is either a _Map[K,V]_ or a _Seq[(K,V)]_; or

    val dd = DataDefinition(list, f, partitions)
    
where _list_ is a _Seq[V]_ and where _f_ is a function of type _V=>K_ (the mapper function).

In all cases, _partitions_ represents the desired number of partitions for the data definition,
but can be omitted, in which case it will be defaulted to be 2.

The only transformation function currently supported is _map_ which, as expected, takes a function which maps a value into a new value.

There are two types of "action" function:
 
* _apply()_ which yields a _Future[Map[K,V]]_
* _aggregate(f)_ which yields a _Future[W]_ where the function _f_ is the aggregation function
of type _(W,V)=>W_ where _W_ is constrained by a context bound: _W: Zero_.

An additional type _DDContext_ is used implicitly when calling the _apply_ methods of the _DataDefintion_ object.

For an example of using this higher-level API, please see the _Matrix_ class.

Introduction
------------

In order for a calculation to be performed in parallel, it is necessary that the complete calculation can be broken up into smaller parts which can each be implemented independently.
These parallel calculations are performed in the _reduce_ phase of map-reduce while the _map_ phase is responsible for breaking the work into these independent parts.
In order that the results from the _reduce_ phase can be collated and/or aggregated, it is usually convenient for each portion of the calculation to be identified by a unique key (we will call the type of these keys _K2_).
The data required for each portion is typically of many similar elements. We will call the type of these elements _W_. Thus the natural intermediate data structure (for the _shuffle_ phase, see below) which results from
the _map_ stage and is used as input to the _reduce_ stage is:

    Map[K2, Seq[W]]
    
Thus the first job of designing an application to use map-reduce is to figure out the types _K2_ and _W_. If you are chaining map-reduce operations together, then the input to stage _N_+1
will be of the same form as the output of stage _N_. Thus, in general, the input to the map-reduce process is a map of key-value pairs. We call the type of the key _K1_ and the type of
the value _V1_. Thus the input to the map stage is, in general:

    Map[K1,V1]
    
For the first stage, there is usually no appropriate key so instead we pass in a message of the following form (which is more or less equivalent to _Map[Unit,V1]_):

	Seq[V1]
	
The reduction stage, as we have already seen, starts with information in the form of _Map[K2,Seq[W]]_ and the work is divided up and sent to each of the reducers. Thus each reducer takes as input (via a message) the following tuple:

	(K2,Seq[W])
	
 The result of each reduction is a tuple of the following form:
 
	(K2,V2)
	
where _V2_ is the aggregate of all of the _W_ elements.

Note that the reason that _W_ is not called _V2_ and _V2_ is not called _V3_ is because _W_ is an internal type. It is not a factor in the incoming or outgoing messages from the _Master_.

Of course, it's possible that there are insufficient reducers available for each of the keys. The way this project deals with that situation is simply to start sending messages to the available actors again.
In general, the so-called _shuffle_ phase which precedes the _reduce_ phase is able to pick and choose how to make the best match between the key value _k_ and a particular reducer. This might be based
on locality of data referenced by the values in the sequence. Or some other criterion with a view to load-balancing. However, this project does not currently make any such decisions so the _shuffle_ phase is really non-existent: messages
(one per key) are simply sent out to reducers in sequence.

Details
=======

Master
------

The _Master_ (or one its three siblings) is the only class which an application needs to be concerned with. The _Master_, itself an actor, creates a mapper and a number of reducers as appropriate on startup and destroys them at the end.
The input message and the constructor format are slightly different according to which form of the _Master_ (see below) you are employing.

Generally, there are five polymorphic types which describe the definition of _Master_: _K1, V1, K2, W,_ and _V2_. Of these, _W_ is not involved in messages going to or from the master--it is internal only.
And, again generally, the constructor for the _Master_ takes the following parameters:

* _config: Config_
* _f: (K1,V1)=>(K2,W)_
* _g: (V2,W)=>V2_
* _z: ()=>V2_

where

* _config_ is used for various configuration settings, such as the number of reducers to be created;
* _f_ is described in _Mapper_ below
* _g_ and _z_ are described in _Reducer_ below

There are actually four _Master_ types to accommodate different situations. The first map-reduce stage in a pipeline (as mentioned above) does not involve _K1_. Therefore, two of the _master_ types are of this "first" type.
Next, there is a difference between the pure reducers which require that these are treated separately (see section on _Reducer_ below). This creates another pairing of master forms: the "fold" variations.
Thus, we have four forms of _Master_ all told:

* _Master_
* _Master_First_
* _Master_Fold_
* _Master_First_Fold_

The "fold" variations require the _z_ parameter, whereas the other variations do not. Thus the non-"fold" variations require that _Z2_ be a super-type of _W_ (as required by _reduceLeft_).

The "first" variations do not require a _K1_ to be defined (it defaults to _Unit_) and see below in _Mapper_ for the difference in input message types.

The __input message__ type for the "first" variations is: _Seq[V1]_ while the input message type for the non-"first" variations is _Map[K1,V1]_.
    
The __output message__ type is always _Response[K2,V2]_. The _Response_ type is defined thus:

	case class Response[K,V](left: Map[K,Throwable], right: Map[K,V]) {
	  def size = right.size
	}

where _K_ represents _K2_ and _V_ represents _V2_. As you can see, the results of applying the reductions are preserved whether they are successes or failures. The _right_ value of the response is the collation of the successful reductions, while the _left_ value represents all of the exceptions that were thrown (with their corresponding key). 

Mapper
-----

The _Mapper_ class is a sub-class of _Actor_. In general, the _Mapper_ takes the following polymorphic types: _[K1,V1,K2,W]_.

The constructor takes a function _f_ of type _(K1,V1)=>(K2,W)_, that's to say it is a function which transforms a _(K1,V1)_ tuple into a _(K2,W)_ tuple.

The incoming message is of the form: _Incoming[K,V]_ where _Incoming_ is essentially a wrapper around the input (but in sequence/tuple form) and is defined thus:

	case class Incoming[K, V](m: Seq[(K,V)])

Where, in practice, _K=K1_ and _V=V1_. For the first-stage map-reduce processes, _K1_ is assumed to be _Unit_. And so you can see the reason for making the input in the form of a wrapper around _Seq[(K1,V1)]_. If the keys are unique then this is 100% two-way convertible with a _Map[K1,V1]_. But since the _K1_ keys can sometimes be missing entirely, we cannot properly form a _Map_. A _Map_ can always be represented as _Seq[Tuple2]_, however.

It makes sense that the output from the reducer phase and, ultimately the master, recalls both successful calls to the reducer and failures. This follows from the independent nature of the reduce phase.
But what about errors in the mapper phase? If the mapper fails on even one input tuple, the entire mapping process is pretty much trashed. What would be the point of continuing on to do the reduce phase after a mapper error?
That is indeed the normal way of things: if there are any failures in mapping, the whole mapping fails. The form of (successful) output is _Map[K2,Seq[W]]_ while any failure outputs a _Throwable_ (this is all part of the _Future_ class behavior). 

Nevertheless, there is an alternative form of mapper called _Mapper_Forgiving_ which will return (to the master) both (as a tuple) the successful output and a sequence of _Throwable_ objects.
This behavior is turned on my setting _forgiving_ to true in the configuration.

Reducer
-------

The _Reducer_ class is a sub-class of _Actor_. In general, the _Reducer_ takes the following polymorphic types: _[K2,W,V2]_.

The constructor takes a function _g_ of type _(V2,W)=>V2_, that's to say it is a function which recursively combines an accumulator of type _V2_ with an element of type _W_, yielding a new value for the accumulator. That's to say, _g_ is passed to the _reduceLeft_ method of _Seq_.

The incoming message is of the form: _Intermediate[K2,W]_ where Intermediate is essentially a wrapper around the input and is defined thus:

	case class Intermediate[K, V](k: K, vs: Seq[V])

Where, in practice, _K=K2_ and _V=W_. There is an alternative form of reducer: _Reducer_Fold_  This type is designed for the situation where _V2_ is _not_ a super-type of _W_ or where there is no natural function to combine a _V2_ with a _W_. In this case, we must use the _foldLeft_ method of _Seq_ instead of the _reduceLeft_ method. This takes an additional function _z_ which is able to initialize the accumulator. 

Functional Map-Reduce
=====================

The set of Master classes can of course be used by applications exactly as described above.
However, there is a more convenient, functional form based on the trait _MapReduce_ which is defined thus:

	trait MapReduce[T,K2,V2] extends Function1[Seq[T],Future[Map[K2,V2]]] {
	    def compose[K3,V3](mr: MapReduce[(K2,V2),K3,V3]): MapReduce[T,K3,V3] = MapReduceComposed(this,mr)
	    def compose[S>:V2](r: Reduce[V2,S])(implicit executionContext: ExecutionContext): Function1[Seq[T],Future[S]]= { ts => for (v2K2m <- apply(ts); s = r.apply(v2K2m)) yield s }
	    def ec: ExecutionContext
	}

This trait casts the map-reduce process as a simple function: one which takes a _Seq[T]_ and results in a (future of) _Map[K2,V2]_ where _T_ is either _V1_ in the case of the first stage of a map-reduce pipeline or _(Kn,Vn)_ in the case of the subsequent (nth) stage. There are four case classes which implement this trait (and which should be specified by the application programmer):

* _MapReduceFirst_
* _MapReducePipe_
* _MapReduceFirstFold_
* _MapReducePipeFold_

Additionally, there is the _MapReduceComposed_ case class which is created by invoking the _compose_ method. A pipeline of map-reduce stages can thus be composed by using the _compose_ method of _MapReduce_. Such a pipeline may be (optionally) terminated by composing with a _Reduce_ instance which combines the values of the final _Map[Kn,Vn]_ into a single _S_ value (where _S_ is a super-class of _Vn_).

Thus a pipeline in functional form is a closure which captures all of the functions, and their parameters which are in scope at the time of defining the pipeline.

See the _CountWords_ example (below). 

Configuration
============

Configuration is based on Typesafe _Config_ (as is normal with _Akka_ applications).
Please see the _reference.conf_ file in the main/resources directory for the list of configurable parameters with their explanations.

Dependencies
============

The components that are used by this project are:

* Scala (2.11.7)
* Akka (2.4.1) although it was developed using 2.3.12 and the only difference in source code is terminate instead of shutdown.
* Typesafe Configuration (1.3.0)
* and dependencies thereof

Examples
========

There are several examples provided (in the examples directory):

* CountWords: a simple example which counts the words in documents and can provide a total word count of all documents.
* WebCrawler: a more complex version of the same sort of thing.

CountWords
----------

Here is the _CountWords_ app. It actually uses a "mock" URI rather than the real thing, but of course, it's simple to change it to use real URIs. I have not included the mock URI code:

	object CountWords extends App {
	  val configRoot = ConfigFactory.load
	  implicit val config = configRoot.getConfig("CountWords")
	  implicit val system = ActorSystem(config.getString("name"))   
	  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))
	  import ExecutionContext.Implicits.global
	  def init = Seq[String]()
	  val stage1= MapReduceFirstFold(
	      {(q,w: String) => val u = MockURI(w); (u.getServer, u.content)},
	      {(a: Seq[String],v: String)=>a:+v},
	      init _
	    )
	  val stage2 = MapReducePipe(
	      {(w: URI, gs: Seq[String])=>(w, (for(g <- gs) yield g.split("""\s+""").length) reduce(_+_))},
	      {(x: Int, y: Int)=>x+y},
	      1
	    )
	  val stage3 = Reduce[Int,Int]({_+_})
	  val countWords = stage1 compose stage2 compose stage3
	  val ws = if (args.length>0) args.toSeq else Seq("http://www.bbc.com/doc1", "http://www.cnn.com/doc2", "http://default/doc3", "http://www.bbc.com/doc2", "http://www.bbc.com/doc3")  
	  countWords.apply(ws).onComplete {
	    case Success(n) => println(s"total words: $n"); system.terminate
	    case Failure(x) => Console.err.println(s"Map/reduce error: ${x.getLocalizedMessage}"); system.terminate
	  }
	}
	
It is a three-stage map-reduce problem, including a final reduce stage.

Stage 1 takes a _Seq[String]_ (representing URIs) and produces a _Map[URI,Seq[String]]_.
The mapper for the first stage returns a tuple of the _URI_ (corresponding to the server for the string) and the content of the resource defined by the string.
The reducer simply adds a _String_ to a _Seq[String]_.
There is additionally an _init_ function which creates an empty _Seq[String]_.
The result of the first stage is a map of _URI->Seq[String]_ where the key represents a server and the value elements are the contents of the documents read from that server. 

Stage 2 takes the result of the first stage and produces a _Map[URI,Int]_.
The second stage mapper takes the _URI_ and _Seq[String]_ from the first stage and splits each string on white space, getting the number of words, then returns the sum of the lengths.
In practice (if you are using just the three default args), these sequences have only one string each.
The second stage reducer simply adds together the results of the mapping phase.
The result of stage 2 is a map of _URI->Int_.

Stage 3 (a terminating stage which produces simply a value) takes the map resulting from stage 2 but simply sums the values (ignoring the keys) to form a grand total.
This value of _Int_ which results is printed using _println_.

Note that the first stage uses _MapReduceFirstFold_, the second stage uses _MapReducePipe_, and the third (terminating) stage uses _Reduce_.

If the names of variables look a bit odd to you, then see my "ScalaProf" blog: http://scalaprof.blogspot.com/2015/12/naming-of-identifiers.html

WebCrawler
----------

Here is the web crawler example app:

	object WebCrawler extends App {
	  val configRoot = ConfigFactory.load
	  implicit val config = configRoot.getConfig("WebCrawler")
	  implicit val system = ActorSystem(config.getString("name"))   
	  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))
	  import ExecutionContext.Implicits.global
	  val ws = if (args.length>0) args.toSeq else Seq("http://www.htmldog.com/examples/")
	  def init = Seq[String]()
	  val stage1: MapReduce[String,URI,Seq[String]] = MapReduceFirstFold(
	      {(q, w: String) => val u = new URI(w); (getHostURI(u), u)},
	      {(a: Seq[String],v: URI)=> val s = Source.fromURL(v.toURL).mkString; a:+s},
	      init _
	    )
	  val stage2 = MapReducePipeFold(
	      {(w: URI, gs: Seq[String])=>(w, (for(g <- gs) yield getLinks(w,g)) reduce(_++_))},
	      {(a: Seq[String],v: Seq[String])=>a++v},
	      init _,
	      1
	    )
	  val stage3 = Reduce[Seq[String],Seq[String]]({_++_})
	  val crawler = stage1 compose stage2 compose stage3  
	  val f = doCrawl(ws,Seq[String](),2) transform ({n: Seq[String] => println(s"total links: ${n.length}"); system.terminate}, {x: Throwable=>system.log.error(x,"Map/reduce error (typically in map function)"); x})
	  Await.result(f,10.minutes)
	  private def doCrawl(us: Seq[String], all: Seq[String], depth: Int): Future[Seq[String]] =
	    if (depth<0) Future(all)
	    else {
	      val (in, out) = us.partition { u => all.contains(u) }
	      for (ws <- crawler(cleanup(out)); gs <- doCrawl(ws.distinct, (all++us).distinct, depth-1)) yield gs
	    }
	  // For the other methods required, please see the project source code on github.
	}
  
The application is somewhat similar to the _CountWords_ app, but because of the much greater load in reading all of the documents at any level of recursion, the first stage
performs the actual document reading during its reduce phase. However, it also has three stages.

The three stages combined as a pipeline called _crawler_ are invoked recursively by the method _doCrawl_.

Because you cannot predict in advance what problems you will run into with badly formed (or non-existent) links, it is better to run this app in forgiving mode.
Expect about 250 links to be visited given the default value of _ws_ and depth of 2.

Future enhancements
===================

* Enable the shuffle process to match keys with reducers according to an application-specific mapping (rather than the current, arbitrary, mapping).
* Enable reducers (and possibly mappers) to be replicated across a cluster.
