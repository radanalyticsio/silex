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
package com.redhat.et.silex.maps.nearest

import math.Numeric

import com.redhat.et.silex.maps.redblack.tree._
import com.redhat.et.silex.maps.ordered._
import com.redhat.et.silex.maps.ordered.tree.DataMap

object tree {
  import com.redhat.et.silex.maps.ordered.tree._

  /** Base trait of R/B tree nodes supporting nearest-key query */
  trait NodeNear[K] extends Node[K] {
    /** Ordering that also supports linear distance |x-y| */
    val keyOrdering: Numeric[K] // <: Ordering[K]

    /** Obtain the nearest nodes to a given key */
    private [nearest] def near(k: K): Seq[INodeNear[K]]

    private[tree] final def dist(k1: K, k2: K) = keyOrdering.abs(keyOrdering.minus(k1, k2))
  }

  /** Leaf R/B tree nodes supporting nearest-key query */
  trait LNodeNear[K] extends NodeNear[K] with LNode[K] {
    final def near(k: K) = Seq.empty[INodeNear[K]]
  }

  /** Internal R/B tree nodes supporting nearest-key query */
  trait INodeNear[K] extends NodeNear[K] with INode[K] {
    val lsub: NodeNear[K]
    val rsub: NodeNear[K]

    val kmin: K
    val kmax: K

    final def near(k: K) = {
      if (keyOrdering.lt(k, data.key)) {
        lsub match {
          case ls: INodeNear[K] => {
            if (keyOrdering.lteq(k, ls.kmax)) ls.near(k)
            else {
              val (dk, ldk) = (dist(k, data.key), dist(k, ls.kmax))
              if (keyOrdering.lt(dk, ldk)) Seq(this)
              else if (keyOrdering.gt(dk, ldk))
                Seq(ls.node(ls.kmax).get.asInstanceOf[INodeNear[K]])
              else Seq(ls.node(ls.kmax).get.asInstanceOf[INodeNear[K]], this)
            }
          }
          case _ => Seq(this)
        }
      } else if (keyOrdering.gt(k, data.key)) {
        rsub match {
          case rs: INodeNear[K] => {
            if (keyOrdering.gteq(k, rs.kmin)) rs.near(k)
            else {
              val (dk, rdk) = (dist(k, data.key), dist(k, rs.kmin))
              if (keyOrdering.lt(dk, rdk)) Seq(this)
              else if (keyOrdering.gt(dk, rdk))
                Seq(rs.node(rs.kmin).get.asInstanceOf[INodeNear[K]])
              else Seq(this, rs.node(rs.kmin).get.asInstanceOf[INodeNear[K]])
            }
          }
          case _ => Seq(this)
        }
      } else Seq(this)
    }
  }

  trait NodeNearMap[K, V] extends NodeNear[K] with NodeMap[K, V]
  trait LNodeNearMap[K, V] extends NodeNearMap[K, V] with LNodeNear[K] with LNodeMap[K, V]
  trait INodeNearMap[K, V] extends NodeNearMap[K, V] with INodeNear[K] with INodeMap[K, V]
}

import tree._

object infra {
  class InjectSet[K](val keyOrdering: Numeric[K]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectSet[K](keyOrdering) with INodeNear[K] with NearestSet[K] {
        // INode
        val color = clr
        val lsub = ls.asInstanceOf[NodeNear[K]]
        val rsub = rs.asInstanceOf[NodeNear[K]]
        val data = dat
        // INodeNear
        val kmin = lsub match {
          case n: INodeNear[K] => n.kmin
          case _ => data.key
        }
        val kmax = rsub match {
          case n: INodeNear[K] => n.kmax
          case _ => data.key
        }
      }
  }

  class InjectMap[K, V](val keyOrdering: Numeric[K]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectMap[K, V](keyOrdering) with INodeNearMap[K, V] with NearestMap[K, V] {
        // INode
        val color = clr
        val lsub = ls.asInstanceOf[NodeNear[K]]
        val rsub = rs.asInstanceOf[NodeNear[K]]
        val data = dat.asInstanceOf[DataMap[K, V]]
        // INodeNear
        val kmin = lsub match {
          case n: INodeNear[K] => n.kmin
          case _ => data.key
        }
        val kmax = rsub match {
          case n: INodeNear[K] => n.kmax
          case _ => data.key
        }
      }
  }
}

import infra._

/** An inheritable and mixable trait for adding nearest-key query to ordered containers
  * @tparam K The key type
  * @tparam IN The node type of the concrete internal R/B tree subclass
  * @tparam M The self-type of the concrete container
  */
trait NearestLike[K, IN <: INodeNear[K], M <: NearestLike[K, IN, M]]
    extends NodeNear[K] with OrderedLike[K, IN, M] {

  /** Obtain the nodes nearest to a key */
  def nearestNodes(k: K) = this.near(k).map(_.asInstanceOf[IN])
}

/** An inheritable and mixable trait for adding nearest-key query to an ordered set
  * @tparam K The key type
  * @tparam IN The node type of the concrete internal R/B tree subclass
  * @tparam M The self-type of the concrete container
  */
trait NearestSetLike[K, IN <: INodeNear[K], M <: NearestSetLike[K, IN, M]]
    extends NearestLike[K, IN, M] with OrderedSetLike[K, IN, M] {

  /** Return entries nearest to a given key.  The sequence that is returned may
    * have zero, one or two elements.  If (k) is at the midpoint between two keys, the two 
    * nearest will be returned.  If container is empty, an empty sequence will be returned.
    */
  def nearest(k: K) = this.near(k).map(_.data.key)
}

/** An inheritable and mixable trait for adding nearest-key query to an ordered map
  * @tparam K The key type
  * @tparam V The value type
  * @tparam IN The node type of the concrete internal R/B tree subclass
  * @tparam M The self-type of the concrete container
  */
trait NearestMapLike[K, V, IN <: INodeNearMap[K, V], M <: NearestMapLike[K, V, IN, M]]
    extends NodeNearMap[K, V] with NearestLike[K, IN, M] with OrderedMapLike[K, V, IN, M] {

  /** Return entries nearest to a given key.  The sequence that is returned may
    * have zero, one or two elements.  If (k) is at the midpoint between two keys, the two 
    * nearest will be returned.  If container is empty, an empty sequence will be returned.
    */
  def nearest(k: K) = this.near(k).map { n =>
    val dm = n.data.asInstanceOf[DataMap[K, V]]
    (dm.key, dm.value)
  }
}

sealed trait NearestSet[K] extends NearestSetLike[K, INodeNear[K], NearestSet[K]] {
  override def toString =
    "NearestSet(" +
      nodesIterator.map(n => s"${n.data.key}").mkString(", ") +
    ")"
}

sealed trait NearestMap[K, V] extends NearestMapLike[K, V, INodeNearMap[K, V], NearestMap[K, V]] {
  override def toString =
    "NearestMap(" +
      nodesIterator.map(n => s"${n.data.key} -> ${n.data.value}").mkString(", ") +
    ")"
}

object NearestSet {
  /** Instantiate a new empty NearestSet 
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.nearest._
    *
    * // set of integers, using default Numeric[Int]
    * val map1 = NearestSet.key[Int]
    * // Use a custom numeric
    * val num: Numeric[Int] = ...
    * val map2 = NearestSet.key(num)
    * }}}
    */
  def key[K](implicit num: Numeric[K]): NearestSet[K]
    = new InjectSet[K](num) with LNodeNear[K] with NearestSet[K]
}

object NearestMap {
  /** Instantiate a new empty NearestMap from key and value types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.nearest._
    *
    * // map integers to strings, using default Numeric[Int]
    * val map1 = NearestMap.key[Int].value[String]
    * // Use a custom numeric
    * val num: Numeric[Int] = ...
    * val map2 = NearestMap.key(num).value[String]
    * }}}
    */
  def key[K](implicit num: Numeric[K]) = new AnyRef {
    def value[V]: NearestMap[K, V] =
      new InjectMap[K, V](num) with LNodeNearMap[K, V] with NearestMap[K, V]
  }
}
