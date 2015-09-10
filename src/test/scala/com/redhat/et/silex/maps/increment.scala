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

import org.scalatest._

import com.twitter.algebird.Monoid

import com.redhat.et.silex.testing.matchers._

object IncrementMapProperties extends FlatSpec with Matchers {
  import tree._
  import infra._

  def testIncrement[K, V, IN <: INodeInc[K, V], M <: IncrementMapLike[K, V, IN, M]](
    data: Seq[(K, V)],
    map: IncrementMapLike[K, V, IN, M]) {
    val mon = map.valueMonoid

    // add values to themselves w.r.t. monoid
    val incTruth1 = data.map(_._2).zip(data.map(_._2)).map(x => mon.plus(x._1, x._2))
    val map1 = data.foldLeft(map)((m, e) => m.increment(e._1, e._2))
    map1.values should beEqSeq(incTruth1)

    // add values to a shuffle
    val v2 = scala.util.Random.shuffle(data.map(_._2))
    val data2 = data.map(_._1).zip(v2)
    val incTruth2 = data.map(_._2).zip(v2).map(x => mon.plus(x._1, x._2))
    val map2 = data2.foldLeft(map)((m, e) => m.increment(e._1, e._2))
    map2.values should beEqSeq(incTruth2)
  }
}


class IncrementMapSpec extends FlatSpec with Matchers {
  import scala.language.reflectiveCalls

  import com.redhat.et.silex.maps.ordered.RBProperties._
  import com.redhat.et.silex.maps.ordered.OrderedMapProperties._

  import IncrementMapProperties._

  def mapType1 = IncrementMap.key[Int].value[Int]

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => (j, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      // incrementing new key is same as insertion:
      val map = shuffled.foldLeft(mapType1)((m, e) => m.increment(e._1, e._2))

      testRB(map)
      testKV(data, map)
      testDel(data, map)
      testIncrement(data, map) 
    }
  }  
}
