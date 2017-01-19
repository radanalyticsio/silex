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

package com.redhat.et.silex.maps.ordered

import org.scalatest._

import com.redhat.et.silex.testing.matchers._

object RBProperties extends FlatSpec with Matchers {
  import com.redhat.et.silex.maps.redblack.tree._

  def color[K](node: Node[K]): Color = node match {
    case n: INode[K] => n.color
    case _ => B
  }

  def testBlackHeight[K](node: Node[K]): Int = node match {
    case n: LNode[K] => 1

    case n: INode[K] if (n.color == R) => {
      val lh = testBlackHeight(n.lsub)
      val rh = testBlackHeight(n.rsub)
      if (!(lh > 0 && lh == rh)) 0 else lh
    }

    case n: INode[K] if (n.color == B) => {
      val lh = testBlackHeight(n.lsub)
      val rh = testBlackHeight(n.rsub)
      if (!(lh > 0 && lh == rh)) 0 else 1 + lh
    }

    case _ => throw new Exception("bad node trait")
  }

  def testRedChildrenBlack[K](node: Node[K]): Boolean = node match {
    case n: LNode[K] => true

    case n: INode[K] if (n.color == R) =>
      color(n.lsub) == B && color(n.rsub) == B &&
      testRedChildrenBlack(n.lsub) && testRedChildrenBlack(n.rsub)

    case n: INode[K] if (n.color == B) =>
      testRedChildrenBlack(n.lsub) && testRedChildrenBlack(n.rsub)

    case _ => throw new Exception("bad node trait")
  }

  def testBalance[K](node: Node[K]): (Int, Int) = node match {
    case n: LNode[K] => (0, 0)

    case n: INode[K] => {
      val (lmin, lmax) = testBalance(n.lsub)
      val (rmin, rmax) = testBalance(n.rsub)
      if ((lmax <= (2 * lmin)) && (rmax <= (2 * rmin)))
        (1 + math.min(lmin, rmin), 1 + math.max(lmax, rmax))
      else
        (-1, 0)
    }

    case _ => throw new Exception("bad node trait")
  }

  // test RB tree invariant properties related to RB construction
  def testRB[K](root: Node[K]) = {
    // The root node of a RB tree should be black
    color(root) should be (B)

    // Every path from a node to its descendant leafs should contain the same # of black nodes
    testBlackHeight(root) should be > 0

    // If a node is red, then both its children should be black
    testRedChildrenBlack(root) should be (true)

    // Depth of deepest node should be <= twice the depth of shallowest
    val (bmin, bmax) = testBalance(root)
    bmin should be >= 0
  }
}

object OrderedSetProperties extends FlatSpec with Matchers {
  import com.redhat.et.silex.maps.redblack.tree.INode
  import tree._
  import infra._
  import RBProperties._

  // Assumes 'data' is in key order
  def testK[K, IN <: INode[K], M <: OrderedSetLike[K, IN, M]](
    data: Seq[K],
    omap: OrderedSetLike[K, IN, M]) {

    // verify the map elements are ordered by key
    omap.keys should beEqSeq(data)
  }

  // Assumes 'data' is in key order
  def testDel[K, IN <: INode[K], M <: OrderedSetLike[K, IN, M]](
    data: Seq[K],
    omap: OrderedSetLike[K, IN, M]) {

    data.foreach { key =>
      val delMap = omap - key
      val delData = data.filter(_ != key)
      testRB(delMap)
      testK(delData, delMap)
    }
  }
}

object OrderedMapProperties extends FlatSpec with Matchers {
  import tree._
  import infra._
  import RBProperties._

  // Assumes 'data' is in key order
  def testKV[K, V, IN <: INodeMap[K, V], M <: OrderedMapLike[K, V, IN, M]](
    data: Seq[(K, V)],
    omap: OrderedMapLike[K, V, IN, M]) {

    // verify the map elements are ordered by key
    omap.keys should beEqSeq(data.map(_._1))

    // verify the map correctly preserves key -> value mappings
    data.map(x => omap.get(x._1)) should beEqSeq(data.map(x => Option(x._2)))
    omap.values should beEqSeq(data.map(_._2))
  }

  // Assumes 'data' is in key order
  def testDel[K, V, IN <: INodeMap[K, V], M <: OrderedMapLike[K, V, IN, M]](
    data: Seq[(K, V)],
    omap: OrderedMapLike[K, V, IN, M]) {

    data.iterator.map(_._1).foreach { key =>
      val delMap = omap - key
      val delData = data.filter(_._1 != key)
      testRB(delMap)
      testKV(delData, delMap)
    }
  }
}

class OrderedSetSpec extends FlatSpec with Matchers {
  import scala.language.reflectiveCalls

  import RBProperties._
  import OrderedSetProperties._

  def mapType1 = OrderedSet.key[Int]

  it should "pass exhaustive tree patterns" in {
    // N should remain small, because we are about to exhaustively test N! patterns.
    // Values larger than 8 rapidly start to take a lot of time, for example my runs for
    // N = 10 complete in about 10-15 minutes.
    val N = 8
    (0 to N).foreach { n =>
      val data = Vector.tabulate(n)(j => j)
      data.permutations.foreach { shuffle =>
        val omap = shuffle.foldLeft(mapType1)((m, e) => m + e)
        testRB(omap)
        testK(data, omap)
        testDel(data, omap)
      }
    }
  }

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => j)
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val omap = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(omap)
      testK(data, omap)
      testDel(data, omap)
    }
  }
}

class OrderedMapSpec extends FlatSpec with Matchers {
  import scala.language.reflectiveCalls

  import RBProperties._
  import OrderedMapProperties._

  def mapType1 = OrderedMap.key[Int].value[Int]

  it should "pass exhaustive tree patterns" in {
    // N should remain small, because we are about to exhaustively test N! patterns.
    // Values larger than 8 rapidly start to take a lot of time, for example my runs for
    // N = 10 complete in about 10-15 minutes.
    val N = 8
    (0 to N).foreach { n =>
      val data = Vector.tabulate(n)(j => (j, j))
      data.permutations.foreach { shuffle =>
        val omap = shuffle.foldLeft(mapType1)((m, e) => m + e)
        testRB(omap)
        testKV(data, omap)
        testDel(data, omap)
      }
    }
  }

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => (j, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val omap = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(omap)
      testKV(data, omap)
      testDel(data, omap)
    }
  }
}
