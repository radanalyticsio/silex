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

package com.redhat.et.silex.histogram.rdd

import com.redhat.et.silex.testing.PerTestSparkContext

import org.scalatest._

class HistogramRDDSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import com.redhat.et.silex.histogram.implicits._
  import com.redhat.et.silex.testing.matchers._

  it should "provide countBy enriched method on RDDs" in {
    val sc = context

    sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).countBy(x => x) should equal (
      Map((3, 3.0), (2, 2.0), (1, 1.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3")).countBy(x => x) should equal (
      Map(("3", 3.0), ("2", 2.0), ("1", 1.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3")).countBy(x => 1 + x.toInt) should equal (
      Map((4, 3.0), (3, 2.0), (2, 1.0)))
  }

  it should "provide histBy enriched method on RDDs" in {
    val sc = context

    sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).histBy(x => x) should equal (
      Seq((3, 3.0), (2, 2.0), (1, 1.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3")).histBy(x => x) should equal (
      Seq(("3", 3.0), ("2", 2.0), ("1", 1.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3")).histBy(x => 1 + x.toInt) should equal (
      Seq((4, 3.0), (3, 2.0), (2, 1.0)))

    val t1 = sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).histBy(x => x, normalized = true)
    t1.map(_._2) should beEqNumSeq (Seq(0.5, 0.3333, 0.1667), 0.001)
    t1.map(_._1) should equal (Seq(3, 2, 1))

    val t2 = sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).histBy(x => x, cumulative = true)
    t2.map(_._2) should beEqNumSeq (Seq(3.0, 5.0, 6.0), 0.001)
    t2.map(_._1) should equal (Seq(3, 2, 1))

    val t3 = sc.parallelize(Seq(1, 2, 3, 2, 3, 3))
      .histBy(x => x, normalized = true, cumulative = true)
    t3.map(_._2) should beEqNumSeq (Seq(0.5, 0.8333, 1.0), 0.001)
    t3.map(_._1) should equal (Seq(3, 2, 1))
  }

  it should "provide countByFlat enriched method on RDDs" in {
    val sc = context

    sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).countByFlat(x => Seq(x)) should equal (
      Map((3, 3.0), (2, 2.0), (1, 1.0)))

    sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).countByFlat(x => Seq(x, x)) should equal (
      Map((3, 6.0), (2, 4.0), (1, 2.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3")).countByFlat(x => Seq(x)) should equal (
      Map(("3", 3.0), ("2", 2.0), ("1", 1.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3"))
      .countByFlat(x => Seq(1 + x.toInt)) should equal (
        Map((4, 3.0), (3, 2.0), (2, 1.0)))
  }

  it should "provide histByFlat enriched method on RDDs" in {
    val sc = context

    sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).histByFlat(x => Seq(x)) should equal (
      Seq((3, 3.0), (2, 2.0), (1, 1.0)))

    sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).histByFlat(x => Seq(x, x)) should equal (
      Seq((3, 6.0), (2, 4.0), (1, 2.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3")).histByFlat(x => Seq(x)) should equal (
      Seq(("3", 3.0), ("2", 2.0), ("1", 1.0)))

    sc.parallelize(Seq("1", "2", "3", "2", "3", "3"))
      .histByFlat(x => Seq(1 + x.toInt)) should equal (
        Seq((4, 3.0), (3, 2.0), (2, 1.0)))

    val t1 = sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).histByFlat(x => Seq(x), normalized = true)
    t1.map(_._2) should beEqNumSeq (Seq(0.5, 0.3333, 0.1667), 0.001)
    t1.map(_._1) should equal (Seq(3, 2, 1))

    val t2 = sc.parallelize(Seq(1, 2, 3, 2, 3, 3)).histByFlat(x => Seq(x), cumulative = true)
    t2.map(_._2) should beEqNumSeq (Seq(3.0, 5.0, 6.0), 0.001)
    t2.map(_._1) should equal (Seq(3, 2, 1))

    val t3 = sc.parallelize(Seq(1, 2, 3, 2, 3, 3))
      .histByFlat(x => Seq(x), normalized = true, cumulative = true)
    t3.map(_._2) should beEqNumSeq (Seq(0.5, 0.8333, 1.0), 0.001)
    t3.map(_._1) should equal (Seq(3, 2, 1))
  }
}
