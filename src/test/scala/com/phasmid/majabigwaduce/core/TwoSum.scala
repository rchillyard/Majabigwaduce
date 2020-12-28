package com.phasmid.majabigwaduce.core

import scala.annotation.tailrec

/**
  * This doesn't really belong here of course. It's a solution to LeetCode problem 1.
  * This is not a particularly good solution, probably because there are few elements in any of the input arrays.
  */
object TwoSum extends App {
  println(twoSum(Array(2, 11, 7, 15), 9).toList)
  println(twoSum(Array(3, 2, 4, 7, 10), 6).toList)
  println(twoSum(Array(3, 3, 1), 6).toList)

  case class Element(value: Int, index: Int)

  object Element {

    implicit object Oe extends Ordering[Element] {
      def compare(x: Element, y: Element): Int = x.value.compareTo(y.value)
    }

  }

  def twoSum(nums: Array[Int], target: Int): Array[Int] = {
    val sorted = (for ((v, i) <- nums.zipWithIndex) yield Element(v, i)).sorted
    val solutions = (for (i <- LazyList.from(0).take(nums.length); j = binarySearch(sorted, target - nums(i))(); if j >= 0 && j != i) yield (i, j)).take(1)
    solutions.headOption.toArray flatMap (x => Array(x._1, x._2))
  }

  @tailrec
  def binarySearch(arr: Array[Element],
                   key: Int)
                  (low: Int = 0,
                   high: Int = arr.length - 1): Int = {
    if (low > high) return -1
    val middle = low + (high - low) / 2
    val cf = arr(middle).value.compareTo(key)
    if (cf == 0) arr(middle).index
    else if (cf > 0) binarySearch(arr, key)(low, middle - 1)
    else binarySearch(arr, key)(middle + 1, high)
  }
}