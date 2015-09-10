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

import org.scalatest._

import com.twitter.algebird.Monoid

import com.redhat.et.silex.testing.matchers._

object PrefixSumMapProperties extends FlatSpec with Matchers {
  import tree._
  import infra._

  // Assumes 'data' is in key order
  def testPrefix[K, V, P, IN <: INodePS[K, V, P], M <: PrefixSumMapLike[K, V, P, IN, M]](
    data: Seq[(K, V)],
    psmap: PrefixSumMapLike[K, V, P, IN, M]) {

    val mon = psmap.prefixMonoid
    val psTruth = data.map(_._2).scanLeft(mon.zero)((v, e) => mon.inc(v, e))
    psmap.prefixSums() should beEqSeq(psTruth.tail)
    psmap.prefixSums(open=true) should beEqSeq(psTruth.dropRight(1))
    psmap.prefixSums() should beEqSeq(psmap.keys.map(k => psmap.prefixSum(k)))
    psmap.prefixSums(open=true) should beEqSeq(psmap.keys.map(k => psmap.prefixSum(k, open=true)))
  }
}

class PrefixSumMapSpec extends FlatSpec with Matchers {
  import scala.language.reflectiveCalls

  import com.redhat.et.silex.maps.ordered.RBProperties._
  import com.redhat.et.silex.maps.ordered.OrderedMapProperties._

  import PrefixSumMapProperties._

  def mapType1 =
    PrefixSumMap.key[Int].value[Int]
      .prefix(IncrementingMonoid.fromMonoid(implicitly[Monoid[Int]]))

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => (j, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val psmap = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(psmap)
      testKV(data, psmap)
      testDel(data, psmap)
      testPrefix(data, psmap) 
    }
  }
}
