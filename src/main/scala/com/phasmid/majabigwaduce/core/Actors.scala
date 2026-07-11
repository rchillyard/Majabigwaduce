package com.phasmid.majabigwaduce.core

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Case class to deal with the construction and configuration of actors.
 *
 * CONSIDER eliminating this class and calling spawn/systemActorOf directly. See Issue #13.
 *
 * @param system the actor system.
 * @param config the configuration for this set of actors.
 */
case class Actors(system: ActorSystem[?], config: Config) extends AutoCloseable:

  /**
   * Create a new actor, using the given spawn function (typically `context.spawn` for a child,
   * or `system.systemActorOf` for a top-level actor).
   *
   * @param spawn     the function which actually creates the actor -- abstracts over whether it's spawned
   *                  from an ActorContext or directly from the ActorSystem.
   * @param maybeName an optional name.
   * @param behavior  the Behavior to spawn.
   * @return an ActorRef[U].
   */
  def createActor[U](spawn: (Behavior[U], String) => ActorRef[U], maybeName: Option[String], behavior: Behavior[U]): ActorRef[U] =
    val actorName = maybeName.getOrElse("Nemo")
    // CONSIDER eliminating this suffix now that we create actors hierarchically (i.e. we create them from context, except the master).
    val actorId = s"$actorName-$suffix"
    Actors.logger.debug(s"createActor: $actorId")
    spawn(behavior, actorId)

  // TEST
  def logException(m: => String, x: Throwable = null): Unit =
    if exceptionStack
    then Actors.logger.error(m, x)
    else Actors.logger.warn(s"$m: ${Option(x).fold("")(_.getLocalizedMessage)}")

  // TEST
  private lazy val exceptionStack = config.getBoolean("exceptionStack")

  def close(): Unit = {}

  private val suffix = Actors.getCount.toHexString

object Actors:
  private val logger = LoggerFactory.getLogger(classOf[Actors])

  // A globally unique, thread-safe, monotonically increasing counter -- deliberately not
  // combined with System.nanoTime().hashCode as it previously was. Under high-frequency actor
  // creation (e.g. JMH benchmarks calling this thousands of times per second), nanoTime's
  // 32-bit hash can repeat between calls just nanoseconds apart, producing duplicate actor
  // names (InvalidActorNameException) even though the counter itself never repeats.
  private val counter = new java.util.concurrent.atomic.AtomicLong(0)

  /**
   * Retrieves the current value of the counter after incrementing it by one.
   *
   * @return the updated value of the globally unique, thread-safe, monotonically increasing counter.
   */
  def getCount: Long = counter.incrementAndGet()

  /**
   * Parses a string representation of a duration and returns a corresponding Timeout object.
   *
   * @param t a string representing the duration in the format "number unit" (e.g., "10 seconds").
   *          If the format is invalid, a default timeout of 10 seconds is returned.
   * @return a Timeout object representing the parsed duration or a default of 10 seconds if parsing fails.
   */
  def getTimeout(t: String): Timeout =
    val durationR = """(\d+)\s*(\w+)""".r
    t match
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10.seconds)
