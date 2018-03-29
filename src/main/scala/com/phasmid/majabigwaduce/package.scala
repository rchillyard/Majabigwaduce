package com.phasmid

import scala.concurrent.Future

package object majabigwaduce {

  /**
    * Type alias for a function which transforms a T into a Future[R]
    *
    * @tparam T the input parametric type
    * @tparam R the output parametric type
    */
  type ASync[-T, +R] = T => Future[R]

  /**
    * Type alias for a (reduce) function which takes a Map[K,T] and converts to an S
    *
    * @tparam K key type (typically, this is ignored)
    * @tparam T value type
    * @tparam S the result type
    */
  type RF[K, T, S] = Map[K, T] => S
}
