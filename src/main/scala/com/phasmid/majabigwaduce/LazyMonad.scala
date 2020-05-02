package com.phasmid.majabigwaduce

trait LazyMonad[T, U] extends Iterable[U] {

  def ts: Iterable[T]

  val f: T => U

  def build[A, B](iterable: Iterable[A])(g: A => B): LazyMonad[A, B]

  override def map[V](g: U => V): LazyMonad[T, V] = build(ts)(f andThen g)

  /**
    * Filter method.
    * CONSIDER reworking this LazyMonad definition to remember a filter so that this can be lazy.
    *
    * @param p the predicate
    * @return a new LazyMonad[U, U]
    */
  override def filter(p: U => Boolean): LazyMonad[U, U] = build(ts.iterator.map(f).filter(p).toSeq)(identity)
}

case class LazySequence[T, U](ts: Iterable[T], f: T => U) extends LazyMonad[T, U] {
  def build[A, B](iterable: Iterable[A])(g: A => B): LazyMonad[A, B] = new LazySequence(iterable, g)

  def iterator: Iterator[U] = ts.iterator.map(f)
}

object LazySequence {
  def apply[T](ts: Iterable[T]): LazyMonad[T, T] = LazySequence(ts, identity)
}