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

package com.redhat.et.silex.feature.extractor

import org.scalatest._

object FeatureSeqSpecSupport extends FlatSpec with Matchers {
  import com.redhat.et.silex.scalatest.matchers._

  val densityEpsilon = 0.0001

  def drTest(s: FeatureSeq) {
    s.length should be >= (0)
    s.density should (be >= 0.0 and be <= 1.0)

    (0 until s.length).foreach { j => { s.isDefinedAt(j) should be (true) } }

    s.iterator.size should be (s.length)

    s.activeKeysIterator.toSeq should equal (s.activeKeysIterator.toSeq.sorted)
    s.activeKeysIterator.size should be (s.activeValuesIterator.size)
    s.activeKeysIterator.size should be <= (s.length)
    s.activeKeysIterator.toSet.subsetOf(s.keysIterator.toSet) should be (true)

    val inactiveKeys = s.keysIterator.toSet -- s.activeKeysIterator.toSet
    inactiveKeys.filter(j => s(j) != 0.0).size should be (0)

    s.activeValuesIterator.toSet.subsetOf(s.valuesIterator.toSet) should be (true)

    s.keysIterator.size should be (s.length)
    s.keysIterator should beEqSeq(0 until s.length)

    s.valuesIterator.size should be (s.length)
    s.valuesIterator should beEqSeq(s.iterator)

    s.keysIterator.map(s) should beEqSeq(s.valuesIterator)
    s.activeKeysIterator.map(s) should beEqSeq(s.activeValuesIterator)
  }

  def identityTest(s: FeatureSeq) {
    s.length should be (0)
    s.density should be (1.0)
    s.iterator.size should be (0)
    s.activeKeysIterator.size should be (0)
    s.activeValuesIterator.size should be (0)
    s.keysIterator.size should be (0)
    s.valuesIterator.size should be (0)
    drTest(s)
  }

  def weightedDensity(s1: FeatureSeq, s2: FeatureSeq) = {
    val (l1, l2) = (s1.length.toDouble, s2.length.toDouble)
    val d = l1 + l2
    if (d <= 0.0) 1.0 else (l1 * s1.density + l2 * s2.density) / d
  }

  def opTest(s1: FeatureSeq, s2: FeatureSeq) {
    val r = s1 ++ s2
    r.length should be (s1.length + s2.length)
    r.density should be (weightedDensity(s1, s2) +- densityEpsilon)
    r.iterator should beEqSeq((s1.iterator ++ s2.iterator))
    r.activeKeysIterator should beEqSeq(
      s1.activeKeysIterator ++ s2.activeKeysIterator.map(_ + s1.length))
    r.activeValuesIterator should beEqSeq(s1.activeValuesIterator ++ s2.activeValuesIterator)
    r.keysIterator should beEqSeq(s1.keysIterator ++ s2.keysIterator.map(_ + s1.length))
    r.valuesIterator should beEqSeq(s1.valuesIterator ++ s2.valuesIterator)
    s1.keysIterator.map(r) should beEqSeq(s1.valuesIterator)
    s2.keysIterator.map(_ + s1.length).map(r) should beEqSeq(s2.valuesIterator)
    s1.activeKeysIterator.map(r) should beEqSeq(s1.activeValuesIterator)
    s2.activeKeysIterator.map(_ + s1.length).map(r) should beEqSeq(s2.activeValuesIterator)
    drTest(r)
  }

  def equalTest(s1: FeatureSeq, s2: FeatureSeq) {
    s1.length should be (s2.length)
    s1.density should be (s2.density +- densityEpsilon)
    s1.iterator should beEqSeq(s2.iterator)
    s1.valuesIterator should beEqSeq(s2.valuesIterator)
    s1.keysIterator should beEqSeq(s2.keysIterator)
    s1.keysIterator.map(s2) should beEqSeq(s1.iterator)
    s2.keysIterator.map(s1) should beEqSeq(s2.iterator)
    s1.activeKeysIterator.map(s2) should beEqSeq(s1.activeValuesIterator)
    s2.activeKeysIterator.map(s1) should beEqSeq(s2.activeValuesIterator)
  }

  def opIdentityTest(s: FeatureSeq) {
    val z = FeatureSeq.empty
    identityTest(z)
    List (z ++ s, s ++ z, (z ++ s) ++ z, z ++ (s ++ z)).foreach { t =>
      equalTest(s, t)
      equalTest(t, s)
      drTest(t)
    }
  }

  def associativeTest(s1: FeatureSeq, s2: FeatureSeq, s3: FeatureSeq) {
    equalTest((s1 ++ s2) ++ s3, s1 ++ (s2 ++ s3))
  }

  def propertyTest(s: FeatureSeq*) {
    s.foreach { drTest(_) }
    s.foreach { opIdentityTest(_) }
    s.combinations(2).flatMap(_.permutations).foreach { x => opTest(x(0), x(1)) }
    s.foreach { x => opTest(x, x) }
    s.combinations(3).flatMap(_.permutations).foreach { x => associativeTest(x(0), x(1), x(2)) }
  }

  def xyTest(s: FeatureSeq, xy: (Int, Double)*) {
    xy.foreach { xy =>
      val (x, y) = xy
      s.isDefinedAt(x) should be (true)
      s(x) should be (y)
    }
  } 
}

class FeatureSeqSpec extends FlatSpec with Matchers {
  import FeatureSeqSpecSupport._

  it should "be a subclass immutable.Seq[Double]" in {
    def t(s: scala.collection.immutable.Seq[Double]) = s.length
    "t(FeatureSeq.empty)" should compile
  }

  it should "provide FeatureSeq.empty factory method" in {
    identityTest(FeatureSeq.empty)
  }

  it should "construct a FeatureSeq from List" in {
    identityTest(FeatureSeq(List[Double]()))
    val s1 = FeatureSeq(List(1.0, 2.0, 3.0))
    s1.length should be (3)
    s1.density should be (1.0)
    xyTest(s1, (0, 1.0), (1, 2.0), (2, 3.0))
    propertyTest(
      FeatureSeq(List(1.0, 2.0, 3.0)),
      FeatureSeq(List(11.0, 12.0)),
      FeatureSeq(List(111.0, 112.0, 113.0))
    )
  }

  it should "construct a FeatureSeq from Array" in {
    identityTest(FeatureSeq(Array[Double]()))
    val s1 = FeatureSeq(Array(1.0, 2.0, 3.0))
    s1.length should be (3)
    s1.density should be (1.0)
    xyTest(s1, (0, 1.0), (1, 2.0), (2, 3.0))
    propertyTest(
      FeatureSeq(Array(1.0, 2.0, 3.0)),
      FeatureSeq(Array(11.0, 12.0)),
      FeatureSeq(Array(111.0, 112.0, 113.0))
    )
  }

  it should "construct a FeatureSeq from Vector" in {
    identityTest(FeatureSeq(Vector[Double]()))
    val s1 = FeatureSeq(Vector(1.0, 2.0, 3.0))
    s1.length should be (3)
    s1.density should be (1.0)
    xyTest(s1, (0, 1.0), (1, 2.0), (2, 3.0))
    propertyTest(
      FeatureSeq(Vector(1.0, 2.0, 3.0)),
      FeatureSeq(Vector(11.0, 12.0)),
      FeatureSeq(Vector(111.0, 112.0, 113.0))
    )
  }

  it should "construct a FeatureSeq from Mixed" in {
    propertyTest(
      FeatureSeq(FeatureSeq(List(777.0, 747.0))),
      FeatureSeq(scala.collection.mutable.ArrayBuffer(666.0, 668.0)),
      FeatureSeq(Vector(3.14)) ++ FeatureSeq(List(33.0)),
      FeatureSeq(List(1.0, 2.0, 3.0)),
      FeatureSeq(Array(11.0, 12.0))
    )
  }
}

class ExtractorSpec extends FlatSpec with Matchers {
}
