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
    (0 until s.length).map(j => s(j)) should beEqSeq(s.iterator)

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

object ExtractorSpecSupport extends FlatSpec with Matchers {
  import com.redhat.et.silex.feature.extractor.Extractor
  import com.redhat.et.silex.scalatest.matchers._
  import com.redhat.et.silex.feature.indexfunction.{
    IndexFunctionSpecSupport,
    InvertibleIndexFunctionSpecSupport
  }

  def drTest[D](e: Extractor[D])(implicit d: Seq[D]) {
    e.width should be >= (0)
    e.names.width should be (e.width)
    e.categoryInfo.width should be (e.width)
    d.foreach { x =>
      e(x).length should be (e.width)
      e.function(x).length should be (e.width)
      FeatureSeqSpecSupport.equalTest(e(x), e.function(x))
    }
  }

  def identityTest[D](e: Extractor[D])(implicit d: Seq[D]) {
    e.width should be (0)
    d.foreach { x =>
      e(x).length should be (0)
    }
    drTest(e)
  }

  def opTest[D](e1: Extractor[D], e2: Extractor[D])(implicit d: Seq[D]) {
    val r = e1 ++ e2
    r.width should be (e1.width + e2.width)
    InvertibleIndexFunctionSpecSupport.equalTest(r.names, e1.names ++ e2.names)
    IndexFunctionSpecSupport.equalTest(r.categoryInfo, e1.categoryInfo ++ e2.categoryInfo)
    d.foreach { x =>
      FeatureSeqSpecSupport.equalTest(r(x), e1(x) ++ e2(x))
    }
    drTest(r)
  }

  def equalTest[D](e1: Extractor[D], e2: Extractor[D])(implicit d: Seq[D]) {
    e1.width should be (e2.width)
    InvertibleIndexFunctionSpecSupport.equalTest(e1.names, e2.names)
    IndexFunctionSpecSupport.equalTest(e1.categoryInfo, e2.categoryInfo)
    d.foreach { x =>
      FeatureSeqSpecSupport.equalTest(e1(x), e2(x))
    }
  }

  def opIdentityTest[D](e: Extractor[D])(implicit d: Seq[D]) {
    val z = Extractor.empty[D]
    identityTest(z)
    List (z ++ e, e ++ z, (z ++ e) ++ z, z ++ (e ++ z)).foreach { t =>
      equalTest(e, t)
      equalTest(t, e)
      drTest(t)
    }
  }

  def associativeTest[D](e1: Extractor[D], e2: Extractor[D], e3: Extractor[D])(implicit d: Seq[D]) {
    equalTest((e1 ++ e2) ++ e3, e1 ++ (e2 ++ e3))
  }

  def propertyTest[D](e: Extractor[D]*)(implicit d: Seq[D]) {
    e.foreach { drTest(_) }
    e.foreach { opIdentityTest(_) }
    e.combinations(2).flatMap(_.permutations).foreach { x => opTest(x(0), x(1)) }
    e.combinations(3).flatMap(_.permutations).foreach { x => associativeTest(x(0), x(1), x(2)) }
  }

  def xyTest[D](e: Extractor[D], xy: (D, FeatureSeq)*) {
    xy.foreach { xy =>
      val (x, y) = xy
      e(x) should beEqSeq (y)
    }
  }
}

class ExtractorSpec extends FlatSpec with Matchers {
  import com.redhat.et.silex.feature.extractor.Extractor
  import ExtractorSpecSupport._
  import com.redhat.et.silex.feature.indexfunction.{
    IndexFunction,
    InvertibleIndexFunction,
    IndexFunctionSpecSupport,
    InvertibleIndexFunctionSpecSupport
  }

  object domainImplicits {
    implicit val domainValuesInt = List(1, 2, 3)
    implicit val domainValuesDouble = List(1.0, 2.0, 3.0)
    implicit val domainValuesString = List("1", "2", "3")
    implicit val domainValuesIntSeq = List(
      Vector(1, 2, 3),
      Vector(3, 2, 1),
      Vector(2, 2, 2)
    )
    implicit val domainValuesDoubleSeq = List(
      Vector(1.0, 2.0, 3.0),
      Vector(3.0, 2.0, 1.0),
      Vector(2.0, 2.0, 2.0)
    )
    implicit val domainValuesStringSeq = List(
      Vector("1", "2", "3"),
      Vector("3", "2", "1"),
      Vector("2", "2", "2")
    )
  }
  import domainImplicits._

  it should "enforce range type consistency during concatenation" in {
    "Extractor.empty[Int] ++ Extractor.empty[String]" shouldNot typeCheck
  }

  it should "provide Extractor.empty factory method" in {
    identityTest(Extractor.empty[Int])
    identityTest(Extractor.empty[String])
    identityTest(Extractor.empty[Vector[Double]])    
  }

  it should "provide Extractor.constant factory method" in {
    identityTest(Extractor.constant[String]())
    val e1 = Extractor.constant[Int](3.14, 2.72)
    e1.width should be (2)
    val fs1 = FeatureSeq(3.14, 2.72)
    xyTest(e1, (-777, fs1), (0, fs1), (123435, fs1))
    propertyTest(
      Extractor.constant[Int](3.14, 2.72),
      Extractor.constant[Int](5.55, 7.77, 11.1111),
      Extractor.constant[Int](1.0))
    propertyTest(
      Extractor.constant[String](3.14, 2.72),
      Extractor.constant[String](5.55, 7.77, 11.1111),
      Extractor.constant[String](1.0))
  }

  it should "provide Extractor.apply factory method" in {
    identityTest(Extractor.apply[Double]())
    val e1 = Extractor((x: Int) => 2.0 * x, (x: Int) => 3.0 * x)
    xyTest(e1, (1, FeatureSeq(2.0, 3.0)), (-1, FeatureSeq(-2.0, -3.0)), (3, FeatureSeq(6.0, 9.0)))
    propertyTest(
      Extractor((x: Int) => 2.0 * x, (x: Int) => 3.0 * x),
      Extractor((x: Int) => 2.0 * x + 3.0),
      Extractor((x: Int) => 2.0 * x, (x: Int) => 3.0 * x, (x: Int) => -7.0 * x))
    propertyTest(
      Extractor((x: Double) => 2.0 * x, (x: Double) => 3.0 * x),
      Extractor((x: Double) => 2.0 * x + 3.0),
      Extractor((x: Double) => 2.0 * x, (x: Double) => 3.0 * x, (x: Double) => -7.0 * x))
  }

  it should "provide Extractor.numeric factory method" in {
    identityTest(Extractor.numeric[Int]())
    val e1 = Extractor.numeric[Int](1, 0)
    e1.width should be (2)
    xyTest(e1,
      (List(1, 2), FeatureSeq(2.0, 1.0)),
      (List(55, 77), FeatureSeq(77.0, 55.0)),
      (List(3, 4), FeatureSeq(4.0, 3.0)))
    propertyTest(
      Extractor.numeric[Int](2, 1, 0),
      Extractor.numeric[Int](1),
      Extractor.numeric[Int](0, 1, 2, 2, 1, 0))
  }

  it should "provide Extractor.string factory method" in {
    identityTest(Extractor.string())
    val e1 = Extractor.string(1, 0)
    e1.width should be (2)
    xyTest(e1,
      (List("1", "2"), FeatureSeq(2.0, 1.0)),
      (List("55", "77"), FeatureSeq(77.0, 55.0)),
      (List("3", "4"), FeatureSeq(4.0, 3.0)))
    propertyTest(
      Extractor.string(2, 1, 0),
      Extractor.string(1),
      Extractor.string(0, 1, 2, 2, 1, 0))
  }

  it should "compose with another function" in {
    val e1 = Extractor.numeric[Long](0).compose((s:String) => List(s.toLong))
    e1.width should be (1)
    xyTest(e1,
      ("1", FeatureSeq(1)),
      ("42", FeatureSeq(42)),
      ("7", FeatureSeq(7)))
    propertyTest(
      Extractor((x:String) => 2.0 * x.toDouble).compose((s:Seq[String]) => s(2)),
      Extractor.numeric[Long](1).compose((s:Seq[String])=>s.map(_.toLong)),
      Extractor.numeric[Double](2, 1, 0).compose((s:Seq[String])=>s.map(_.toDouble)))
  }

  it should "provide Extractor.numericSeq factory method" in {
    identityTest(Extractor.numericSeq[Double](0).compose((x:Seq[Double])=>x.take(0)))
    val e1 = Extractor.numericSeq[Long](2)
    e1.width should be (2)
    xyTest(e1,
      (List(1L, 2L), FeatureSeq(1.0, 2.0)),
      (List(55L, 77L), FeatureSeq(55.0, 77.0)),
      (List(3L, 4L), FeatureSeq(3.0, 4.0)))
    propertyTest(
      Extractor.numericSeq[Int](3),
      Extractor.numericSeq[Int](1).compose((x:Seq[Int])=>x.take(1)),
      Extractor.numericSeq[Int](2).compose((x:Seq[Int])=>x.take(2)))
  }

  it should "support withNames" in {
    val e1 = Extractor.constant[Int](1.0, 2.0)
    InvertibleIndexFunctionSpecSupport.undefinedTest(e1.names)
    val e2 = e1.withNames("a", "b")
    e2.names(0) should equal("a")
    e2.names(1) should equal("b")
    propertyTest(
      Extractor.constant[Int](1.0, 2.0).withNames("a", "b"),
      Extractor.constant[Int](3.0).withNames(InvertibleIndexFunction(Vector("c"))),
      Extractor.constant[Int](4.0, 5.0, 6.0).withNames("d", "e", "f"))
  }

  it should "support withCategoryInfo" in {
    val e1 = Extractor.constant[Int](1.0, 2.0, 3.0).withNames("a", "b", "c")
    IndexFunctionSpecSupport.undefinedTest(e1.categoryInfo)
    val e2 = e1.withCategoryInfo(("a", 2), ("c", 5))
    e2.categoryInfo(0) should be (2)
    e2.categoryInfo(2) should be (5)
    e2.categoryInfo.isDefinedAt(1) should be (false)
    propertyTest(
      Extractor.constant[Int](1.0, 2.0).withNames("a", "b").withCategoryInfo(("a", 2), ("c", 5)),
      Extractor.constant[Int](3.0).withNames("c").withCategoryInfo(IndexFunction(Vector(7))),
      Extractor.constant[Int](4.0, 5.0).withNames("d", "e").withCategoryInfo(("d", 8), ("e", 9)))
  }
}
