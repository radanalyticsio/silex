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

package com.redhat.et.silex.maps.increment

import math.Ordering

import com.twitter.algebird.Monoid

import com.redhat.et.silex.maps.redblack.tree._
import com.redhat.et.silex.maps.ordered._
import com.redhat.et.silex.maps.ordered.tree.DataMap

object tree {
  import com.redhat.et.silex.maps.ordered.tree._

  /** Base trait of R/B tree nodes supporting increment */
  trait NodeInc[K, V] extends NodeMap[K, V] {
    /** The monoid that defines what it means to increment a value */
    val valueMonoid: Monoid[V]

    /** Increment the value at a key, by another value */
    private[increment] final def incr(di: DataMap[K, V]) = blacken(inc(di))

    private[tree] def inc(di: DataMap[K, V]): Node[K]
  }

  /** Leaf R/B node supporting increment */
  trait LNodeInc[K, V] extends NodeInc[K, V] with LNodeMap[K, V] {
    final def inc(di: DataMap[K, V]) = {
      val d = new DataMap[K, V] {
        val key = di.key
        val value = valueMonoid.plus(valueMonoid.zero, di.value)
      }
      rNode(d, this, this)
    }
  }

  /** Internal R/B node supporting increment */
  trait INodeInc[K, V] extends NodeInc[K, V] with INodeMap[K, V] {
    val lsub: NodeInc[K, V]
    val rsub: NodeInc[K, V]

    final def inc(di: DataMap[K, V]) =
      if (color == R) {
        if (keyOrdering.lt(di.key, data.key)) rNode(data, lsub.inc(di), rsub)
        else if (keyOrdering.gt(di.key, data.key)) rNode(data, lsub, rsub.inc(di))
        else {
          val d = new DataMap[K, V] {
            val key = data.key
            val value = valueMonoid.plus(data.value, di.value)
          }
          rNode(d, lsub, rsub)
        }
      } else {
        if (keyOrdering.lt(di.key, data.key)) balance(bNode(data, lsub.inc(di), rsub))
        else if (keyOrdering.gt(di.key, data.key)) balance(bNode(data, lsub, rsub.inc(di)))
        else {
          val d = new DataMap[K, V] {
            val key = data.key
            val value = valueMonoid.plus(data.value, di.value)
          }
          bNode(d, lsub, rsub)
        }
      }
  }
}

import tree._

object infra {
  class Inject[K, V](val keyOrdering: Ordering[K], val valueMonoid: Monoid[V]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new Inject[K, V](keyOrdering, valueMonoid) with INodeInc[K, V] with IncrementMap[K, V] {
        // INode[K]
        val color = clr
        val lsub = ls.asInstanceOf[NodeInc[K, V]]
        val rsub = rs.asInstanceOf[NodeInc[K, V]]
        val data = dat.asInstanceOf[DataMap[K, V]]
      }
  }
}

import infra._

/** An inheritable and mixable trait for adding increment operation to ordered maps 
  * @tparam K The key type
  * @tparam V The value type
  * @tparam IN The node type of the concrete internal R/B tree subclass
  * @tparam M The map self-type of the concrete map subclass
  */
trait IncrementMapLike[K, V, IN <: INodeInc[K, V], M <: IncrementMapLike[K, V, IN, M]]
    extends NodeInc[K, V] with OrderedMapLike[K, V, IN, M] {

  /** Add (w.r.t. valueMonoid) a given value to the value currently stored at key.
    * @note If key is not present, equivalent to insert(k, valueMonoid.plus(valueMonoid.zero, iv)
    */
  def increment(k: K, iv: V) = this.incr(
    new DataMap[K, V] {
      val key = k
      val value = iv
    }).asInstanceOf[M]
}

sealed trait IncrementMap[K, V] extends IncrementMapLike[K, V, INodeInc[K, V], IncrementMap[K, V]] {
  override def toString =
    "IncrementMap(" +
      nodesIterator.map(n => s"${n.data.key} -> ${n.data.value}").mkString(", ") +
    ")"
}

object IncrementMap {
  /** Instantiate a new empty IncrementMap from key and value types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.increment._
    *
    * // map strings to integers, using default string ordering and default value monoid
    * val map1 = IncrementMap.key[String].value[Int]
    * // Use a custom ordering
    * val ord: Ordering[String] = ...
    * val map2 = IncrementMap.key(ord).value[Int]
    * // A custom value monoid, defines what 'increment' means
    * val mon: Monoid[Int] = ...
    * val map2 = IncrementMap.key[String].value(mon)
    * }}}
    */
  def key[K](implicit ord: Ordering[K]) = new AnyRef {
    def value[V](implicit mon: Monoid[V]): IncrementMap[K, V] =
      new Inject[K, V](ord, mon) with LNodeInc[K, V] with IncrementMap[K, V]
  }
}
