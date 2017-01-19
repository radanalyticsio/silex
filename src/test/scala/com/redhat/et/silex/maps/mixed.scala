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

package com.redhat.et.silex.maps

import org.scalatest._

import com.twitter.algebird.Monoid

import com.redhat.et.silex.testing.matchers._

object mixed {
  import math.Numeric
  import com.redhat.et.silex.maps.increment._
  import com.redhat.et.silex.maps.prefixsum._
  import com.redhat.et.silex.maps.nearest._

  object tree {
    import com.redhat.et.silex.maps.increment.tree._
    import com.redhat.et.silex.maps.prefixsum.tree._
    import com.redhat.et.silex.maps.nearest.tree._

    trait NodeTD[K, V, P] extends NodePS[K, V, P] with NodeInc[K, V] with NodeNearMap[K, V]

    trait LNodeTD[K, V, P] extends NodeTD[K, V, P]
        with LNodePS[K, V, P] with LNodeInc[K, V] with LNodeNearMap[K, V]

    trait INodeTD[K, V, P] extends NodeTD[K, V, P]
        with INodePS[K, V, P] with INodeInc[K, V] with INodeNearMap[K, V] {
      val lsub: NodeTD[K, V, P]
      val rsub: NodeTD[K, V, P]
    }
  }

  import tree._

  object infra {
    import com.redhat.et.silex.maps.redblack.tree._
    import com.redhat.et.silex.maps.ordered.tree.DataMap

    class Inject[K, V, P](
      val keyOrdering: Numeric[K],
      val valueMonoid: Monoid[V],
      val prefixMonoid: IncrementingMonoid[P, V]) {

      def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
        new Inject[K, V, P](keyOrdering, valueMonoid, prefixMonoid)
            with INodeTD[K, V, P] with TDigestMap[K, V, P] {
          // INode[K]
          val color = clr
          val lsub = ls.asInstanceOf[NodeTD[K, V, P]]
          val rsub = rs.asInstanceOf[NodeTD[K, V, P]]
          val data = dat.asInstanceOf[DataMap[K, V]]
          // INodePS[K, V, P]
          val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), data.value)
          // INodeNear[K, V]
          val kmin = lsub match {
            case n: INodeTD[K, V, P] => n.kmin
            case _ => data.key
          }
          val kmax = rsub match {
            case n: INodeTD[K, V, P] => n.kmax
            case _ => data.key
          }
        }
    }

  }

  import infra._

  sealed trait TDigestMap[K, V, P]
    extends IncrementMapLike[K, V, INodeTD[K, V, P], TDigestMap[K, V, P]]
    with PrefixSumMapLike[K, V, P, INodeTD[K, V, P], TDigestMap[K, V, P]]
    with NearestMapLike[K, V, INodeTD[K, V, P], TDigestMap[K, V, P]] {

    override def toString =
      "TDigestMap(" +
        iterator.zip(prefixSumsIterator())
          .map(x => s"${x._1._1} -> (${x._1._2}, ${x._2})").mkString(", ") +
      ")"
  }

  object TDigestMap {
    def key[K](implicit num: Numeric[K]) = new AnyRef {
      def value[V](implicit vm: Monoid[V]) = new AnyRef {
        def prefix[P](implicit im: IncrementingMonoid[P, V]): TDigestMap[K, V, P] =
          new Inject[K, V, P](num, vm, im) with LNodeTD[K, V, P] with TDigestMap[K, V, P]
      }
    }
  }
}

class MixedMapSpec extends FlatSpec with Matchers {
  import scala.language.reflectiveCalls

  import com.redhat.et.silex.maps.prefixsum.IncrementingMonoid

  import com.redhat.et.silex.maps.ordered.RBProperties._
  import com.redhat.et.silex.maps.ordered.OrderedMapProperties._
  import com.redhat.et.silex.maps.prefixsum.PrefixSumMapProperties._
  import com.redhat.et.silex.maps.increment.IncrementMapProperties._
  import com.redhat.et.silex.maps.nearest.NearestMapProperties._

  import mixed.TDigestMap

  def mapType1 =
    TDigestMap.key[Double].value[Int]
      .prefix(IncrementingMonoid.fromMonoid(implicitly[Monoid[Int]]))

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => (j.toDouble, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val map = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(map)
      testKV(data, map)
      testDel(data, map)
      testPrefix(data, map) 
      testIncrement(data, map) 
      testNearest(data, map) 
    }
  }  
}
