package com.phasmid.majabigwaduce

///**
//  * Created by scalaprof on 10/31/16.
//  */
//trait DD[K,V] { self =>
//  def map[W >: V](f: V=>W): DD[K,W] = transformed(self,f)
//  def flatMap[W >: V](f: V=>DD[K,W]): DD[K,W]
//  def collect: Future[Map[K,V]]
//  def transformed[W >: V](dd: DD[K,V], f: V=>W): DD[K,W]
//}
//
//class RDD[K,V](m: Map[K,V], partitions: Int) extends DD[K,V] {
//  def flatMap[W >: V](f: (V) => DD[K, W]): DD[K, W] = ???
//  def collect: Future[Map[K, V]] = Future(m)
//  def transformed[W >: V](dd: DD[K, V], f: (V) => W): DD[K, W] = new TransformedDD(dd,f)
//}
//
//class TransformedDD[K,V,W](dd: DD[K, V], g: (V) => W) extends DD[K,W] {
//  def flatMap[X >: W](f: (W) => DD[K, X]): DD[K, X] = ???
//
//  def collect: Future[Map[K, W]] = ???
//
//  def transformed[X >: W](dd: DD[K, W], f: (W) => X): DD[K, X] = new TransformedDD(dd,f compose g)
//}
//
//object RDD {
//  def apply[K,V](k_vs: Map[K,V], partitions: Int): RDD[K,V] = new RDD(k_vs,partitions)
//  def apply[K,V](vs: Seq[V], f: V=>K, partitions: Int): RDD[K,V] = apply((for (v<-vs) yield (f(v),v)).toMap,partitions)
//  def apply[K,V](vs: Seq[V], f: V=>K): RDD[K,V] = apply(vs,f,2)
//}