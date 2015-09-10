/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2015 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.c
 */

package com.redhat.et.silex.maps.prefixsum

import math.Ordering

import com.twitter.algebird.{ Monoid, MonoidAggregator }

import com.redhat.et.silex.maps.redblack.tree._
import com.redhat.et.silex.maps.ordered._

object tree {
  import com.redhat.et.silex.maps.ordered.tree._

  /** Base trait for R/B nodes supporting prefix-sum query */
  trait NodePS[K, V, P] extends NodeMap[K, V] {
    /** Monoid with increment-by-element that defines semantics of prefix sum */
    val prefixMonoid: IncrementingMonoid[P, V]

    /** Obtain the prefix (cumulative) sum of values <= a given key 'k'.
      * If 'open' is true, sums the open interval for keys strictly < k.
      * If 'k' is not present in the map, then the sum for keys < k is returned.
      */
    final def prefixSum(k: K, open: Boolean = false) = pfSum(k, prefixMonoid.zero, open)

    private [tree] def pfSum(k: K, sum: P, open: Boolean): P

    // this has to be public to support injection
    def pfs: P
  }

  /** Leaf node for R/B nodes supporting prefix-sum query */
  trait LNodePS[K, V, P] extends NodePS[K, V, P] with LNodeMap[K, V] {
    final def pfSum(k: K, sum: P, open: Boolean) = sum
    final def pfs = prefixMonoid.zero  
  }

  /** Internal node for R/B nodes supporting prefix-sum query */
  trait INodePS[K, V, P] extends NodePS[K, V, P] with INodeMap[K, V] {
    val lsub: NodePS[K, V, P]
    val rsub: NodePS[K, V, P]

    val prefix: P

    final def pfSum(k: K, sum: P, open: Boolean) =
      if (keyOrdering.lt(k, data.key))
        lsub.pfSum(k, sum, open)
      else if (keyOrdering.gt(k, data.key))
        rsub.pfSum(k, prefixMonoid.inc(prefixMonoid.plus(sum, lsub.pfs), data.value), open)
      else if (open)
        prefixMonoid.plus(sum, lsub.pfs)
      else
        prefixMonoid.inc(prefixMonoid.plus(sum, lsub.pfs), data.value)

    final def pfs = prefix
  }
}

import tree._

object infra {
  import com.redhat.et.silex.maps.ordered.tree.DataMap

  class Inject[K, V, P](val keyOrdering: Ordering[K], val prefixMonoid: IncrementingMonoid[P, V]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new Inject[K, V, P](keyOrdering, prefixMonoid)
          with INodePS[K, V, P] with PrefixSumMap[K, V, P] {
        // INode[K]
        val color = clr
        val lsub = ls.asInstanceOf[NodePS[K, V, P]]
        val rsub = rs.asInstanceOf[NodePS[K, V, P]]
        val data = dat.asInstanceOf[DataMap[K, V]]
        // INodePS[K, V, P]
        val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), data.value)
      }
  }

}

import infra._

  /** An inheritable and mixable trait for adding prefix sum query to ordered maps 
    * @tparam K The key type
    * @tparam V The value type
    * @tparam P The prefix sum type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The map self-type of the concrete map subclass
    */
  trait PrefixSumMapLike[K, V, P, IN <: INodePS[K, V, P], M <: PrefixSumMapLike[K, V, P, IN, M]]
      extends NodePS[K, V, P] with OrderedMapLike[K, V, IN, M] {

    /** A container of all prefix sums over the stored values.  If 'open' is true,
      * the sums will be for strictly < each key.
      */
    def prefixSums(open: Boolean = false) = prefixSumsIterator(open).toIterable

    /** Iterate over prefix sums for stored values.  If 'open' is true,
      * the sums will be for strictly < each key.
      */
    def prefixSumsIterator(open: Boolean = false) = {
      val itr = valuesIterator.scanLeft(prefixMonoid.zero)((p, e) => prefixMonoid.inc(p, e))
      if (open) itr.takeWhile(_ => itr.hasNext) else itr.drop(1)
    }
  }

sealed trait PrefixSumMap[K, V, P]
    extends PrefixSumMapLike[K, V, P, INodePS[K, V, P], PrefixSumMap[K, V, P]] {

  override def toString =
    "PrefixSumMap(" +
      iterator.zip(prefixSumsIterator())
        .map(x => s"${x._1._1} -> (${x._1._2}, ${x._2})").mkString(", ") +
    ")"
}

object PrefixSumMap {
  /** Instantiate a new empty PrefixSumMap from key, value and prefix types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.prefixsum._
    *
    * // map strings to integers, using default ordering and standard integer monoid
    * val map1 = PrefixSumMap.key[String].value[Int].prefix(IncrementingMonoid.fromMonoid[Int])
    * // Use a custom ordering
    * val ord: Ordering[String] = ...
    * val map2 = PrefixSumMap.key(ord).value[Int].prefix(IncrementingMonoid.fromMonoid[Int])
    * }}}
    */
  def key[K](implicit ord: Ordering[K]) = new AnyRef {
    def value[V] = new AnyRef {
      def prefix[P](implicit mon: IncrementingMonoid[P, V]): PrefixSumMap[K, V, P] =
        new Inject[K, V, P](ord, mon) with LNodePS[K, V, P] with PrefixSumMap[K, V, P]
    }
  }
}

/** A monoid that also supports an 'increment' operation.  This class is intended to encode
  * semantics similar to Scala's seq.aggregate(z)(seqop,combop), where the standard
  * Monoid 'plus' corresponds to 'combop' and the 'inc' method corresponds to 'seqop'.
  */
trait IncrementingMonoid[T, E] extends Monoid[T] {
  /** increment a monoid 't' by an element value 'e', and return the result */
  def inc(t: T, e: E): T
}

/** Factory methods for instantiating incrementing monoids */
object IncrementingMonoid {
  /** Create an incrementing monoid from a MonoidAggregator object.
    * 'zero' and 'plus' are inherited from agg.monoid.
    * 'inc' is defined by: agg.monoid.plus(t,agg.prepare(e))
    */
  def fromMonoidAggregator[T, E](agg: MonoidAggregator[E, T, T]) = new IncrementingMonoid[T, E] {
    def zero = agg.monoid.zero
    def plus(l: T, r: T) = agg.monoid.plus(l, r)
    def inc(t: T, e: E) = agg.monoid.plus(t, agg.prepare(e))
  }

  /** Create an incrementing monoid from a Monoid object.
    * 'zero' and 'plus' are inherited from monoid.
    * 'inc' is equivalent to 'plus'
    */
  def fromMonoid[T](implicit monoid: Monoid[T]) = new IncrementingMonoid[T, T] {
    def zero = monoid.zero
    def plus(l: T, r: T) = monoid.plus(l, r)
    def inc(t: T, e: T) = monoid.plus(t, e)
  }

  /** Create an incrementing monoid from zero, plus and inc 
    * {{{
    * import scala.language.reflectiveCalls
    * IncrementingMonoid.zero(0).plus(_ + _).inc(_ + _)
    * IncrementingMonoid.zero(Set.empty[Int].plus(_ ++ _).inc[Int](_ + _)
    * }}}
    */
  def zero[T](z: T) = new AnyRef {
    def plus(p: (T, T) => T) = new AnyRef {
      def inc[E](i: (T, E) => T) = new IncrementingMonoid[T, E] {
        def zero = z
        def plus(l: T, r: T) = p(l, r)
        def inc(t: T, e: E) = i(t, e)
      } 
    }
  }
}
