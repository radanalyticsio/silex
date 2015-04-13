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

package com.redhat.et.silex.indexfunction

import org.scalatest._

object IndexFunctionSpecSupport extends FlatSpec with Matchers {
  def drTest[V](f: IndexFunction[V]) {
    f.width should be >= (0)
    val rng = f.range.toSet
    f.domain.foreach { j =>
      j should be >= (0)
      j should be < (f.width)
      f.isDefinedAt(j) should be (true)
      rng.contains(f(j)) should be (true)
    }
    f.domain.map(f).toSet should equal(rng)
  }

  def undefinedTest[V](f: IndexFunction[V]) {
    f.width should be >= (0)
    f.domain should have size 0
    f.range should have size 0
    (-100 to 100).foreach { j =>
      f.isDefinedAt(j) should be (false)
    }
  }

  def identityTest[V](f: IndexFunction[V]) {
    f.width should be (0)
    undefinedTest(f)
  }

  def opTest[V](f1: IndexFunction[V], f2: IndexFunction[V]) {
    val r = f1 ++ f2
    r.width should be (f1.width + f2.width)
    r.domain.size should be (f1.domain.size + f2.domain.size)
    r.range.size should be >= (math.max(f1.range.size, f2.range.size))
    r.domain.toSet should equal (f1.domain.toSet union f2.domain.map(_ + f1.width).toSet)
    r.range.toSet should equal (f1.range.toSet union f2.range.toSet)
    f1.domain.map(r).toList should equal (f1.domain.map(f1).toList)
    f2.domain.map(_ + f1.width).map(r).toList should equal(f2.domain.map(f2).toList)
    drTest(r)
  }

  def equalTest[V](f1: IndexFunction[V], f2: IndexFunction[V]) {
    f1.width should be (f2.width)
    f1.domain.size should be (f2.domain.size)
    f1.range.size should be (f2.range.size)
    f1.domain.toSet should equal (f2.domain.toSet)
    f1.domain.map(f1).toList should equal (f1.domain.map(f2).toList)
    f2.domain.map(f1).toList should equal (f2.domain.map(f2).toList)
  }

  def opIdentityTest[V](f: IndexFunction[V]) {
    val z = IndexFunction.empty[V]
    identityTest(z)
    List (z ++ f, f ++ z, z ++ f ++ z).foreach { t =>
      equalTest(f, t)
      equalTest(t, f)
      drTest(t)
    }
  }

  def associativeTest[V](
    f1: IndexFunction[V],
    f2: IndexFunction[V],
    f3: IndexFunction[V]) {
    equalTest((f1 ++ f2) ++ f3, f1 ++ (f2 ++ f3))
  }

  def propertyTest[V](fs: IndexFunction[V]*) {
    fs.foreach { drTest(_) }
    fs.foreach { opIdentityTest(_) }
    fs.combinations(2).flatMap(_.permutations).foreach { f => opTest(f(0), f(1)) }
    fs.foreach { f => opTest(f, f) }
    fs.combinations(3).flatMap(_.permutations).foreach { f => associativeTest(f(0), f(1), f(2)) }
  }

  def xyTest[V](f: IndexFunction[V], xy: (Int, V)*) {
    xy.foreach { xy =>
      val (x, y) = xy
      f.isDefinedAt(x) should be (true)
      f(x) should be (y)
    }
  }
}

object InvertableIndexFunctionSpecSupport extends FlatSpec with Matchers {
  def drTest[V](f: InvertableIndexFunction[V]) {
    IndexFunctionSpecSupport.drTest(f)
    f.width should be (f.inverse.width)
    f.domain.size should be (f.range.size)
    f.domain.size should be (f.inverse.range.size)
    f.domain.toSet should equal (f.inverse.range.toSet)
    f.range.size should be (f.inverse.domain.size)
    f.range.toSet should equal (f.inverse.domain.toSet)
    val rng = f.inverse.range.toSet
    f.inverse.domain.foreach { v =>
      f.inverse.isDefinedAt(v) should be (true)
      rng.contains(f.inverse(v)) should be (true)
    }
  }

  def undefinedTest[V](f: InvertableIndexFunction[V]) {
    IndexFunctionSpecSupport.undefinedTest(f)
    f.width should be (f.inverse.width)
    f.inverse.domain.size should be (0)
    f.inverse.range.size should be (0)
  }

  def identityTest[V](f: InvertableIndexFunction[V]) {
    undefinedTest(f)
    f.width should be (0)
    f.inverse.width should be (0)
  }

  def opTest[V](f1: InvertableIndexFunction[V], f2: InvertableIndexFunction[V]) {
    IndexFunctionSpecSupport.opTest(f1, f2)
    val r = f1 ++ f2
    drTest(r)
    f1.range.map(r.inverse).toSet should equal (f1.domain.toSet)
    f2.range.map(r.inverse).toSet should equal (f2.domain.map(_ + f1.width).toSet)
  }

  def equalTest[V](f1: InvertableIndexFunction[V], f2: InvertableIndexFunction[V]) {
    IndexFunctionSpecSupport.equalTest(f1, f2)
    val i1 = f1.inverse
    val i2 = f2.inverse
    i1.width should be (i2.width)
    i1.domain.size should be (i2.domain.size)
    i1.domain.toSet should equal (i2.domain.toSet)
    i1.range.size should be (i2.range.size)
    i1.range.toSet should equal (i2.range.toSet)
    i1.domain.map(i1).toList should equal (i1.domain.map(i2).toList)
    i2.domain.map(i1).toList should equal (i2.domain.map(i2).toList)
  }

  def opIdentityTest[V](f: InvertableIndexFunction[V]) {
    IndexFunctionSpecSupport.opIdentityTest(f)
    val z = InvertableIndexFunction.empty[V]
    identityTest(z)
    List (z ++ f, f ++ z, z ++ f ++ z).foreach { t =>
      equalTest(f, t)
      equalTest(t, f)
      drTest(t)
    }
  }

  def associativeTest[V](
    f1: InvertableIndexFunction[V],
    f2: InvertableIndexFunction[V],
    f3: InvertableIndexFunction[V]) {
    equalTest((f1 ++ f2) ++ f3, f1 ++ (f2 ++ f3))
  }

  def propertyTest[V](fs: InvertableIndexFunction[V]*) {
    fs.foreach { drTest(_) }
    fs.foreach { opIdentityTest(_) }
    fs.combinations(2).flatMap(_.permutations).foreach { f => opTest(f(0), f(1)) }
    fs.foreach { f =>
      if (f.range.size > 0) { an [Exception] should be thrownBy (f ++ f) }
    }
    fs.combinations(3).flatMap(_.permutations).foreach { f => associativeTest(f(0), f(1), f(2)) }
  }

  def xyTest[V](f: InvertableIndexFunction[V], xy: (Int, V)*) {
    IndexFunctionSpecSupport.xyTest(f, xy:_*)
    val i = f.inverse
    xy.foreach { xy =>
      val (x, y) = xy
      i.isDefinedAt(y) should be (true)
      i(y) should be (x)
    }
  }
}

class IndexFunctionSpec extends FlatSpec with Matchers {
  import IndexFunctionSpecSupport._

  it should "enforce range type consistency during concatenation" in {
    "IndexFunction.empty[Int] ++ IndexFunction.empty[String]" shouldNot typeCheck
  }

  it should "provide IndexFunction.empty factory method" in {
    identityTest(IndexFunction.empty[Nothing])
    identityTest(IndexFunction.empty[Int])
    identityTest(IndexFunction.empty[String])
    identityTest(IndexFunction.empty[String] ++ IndexFunction.empty[String])
    propertyTest(
      IndexFunction.empty[String],
      IndexFunction.empty[String],
      IndexFunction.empty[String])
  }

  it should "provide IndexFunction.undefined factory method" in {
    an [Exception] should be thrownBy IndexFunction.undefined(-1)
    identityTest(IndexFunction.undefined[Int](0))
    undefinedTest(IndexFunction.undefined[Int](5))
    undefinedTest(IndexFunction.undefined[String](1))
    undefinedTest(IndexFunction.undefined[String](1) ++ IndexFunction.undefined[String](3))
    propertyTest(
      IndexFunction.undefined[Int](4),
      IndexFunction.undefined[Int](1000),
      IndexFunction.undefined[Int](1))
  }

  it should "provide IndexFunction.constant factory method" in {
    an [Exception] should be thrownBy IndexFunction.constant(3.14, -1)

    identityTest(IndexFunction.constant("a", 0))

    val f1 = IndexFunction.constant(42, 42)
    (0 until f1.width).foreach { j =>
      f1.isDefinedAt(j) should be (true)
      f1(j) should be (42)
    }
    propertyTest(
      IndexFunction.constant(42, 42),
      IndexFunction.constant(54, 6 * 9),
      IndexFunction.constant(73, 1))

    val f2 = IndexFunction.constant("6 * 9", 42)
    (0 until f2.width).foreach { j =>
      f2.isDefinedAt(j) should be (true)
      f2(j) should be ("6 * 9")
    }
    propertyTest(
      IndexFunction.constant("73", 100),
      IndexFunction.constant("6 * 9", 42),
      IndexFunction.constant("42", 6 * 9))
  }

  it should "provide IndexFunction.apply method on IndexedSeq" in {
    identityTest(IndexFunction(Vector()))

    val f1 = IndexFunction(Vector(2, 3, 5, 7, 3))
    f1.width should be (5)
    f1.domain.toSet should equal ((0 until f1.width).toSet)
    f1.range.toSet should equal (Set(2, 3, 5, 7))
    xyTest(f1, (0, 2), (1, 3), (2, 5), (3, 7), (4, 3))
    propertyTest(f1, IndexFunction(Vector(2, 3, 5)), IndexFunction(Vector(7, 11, 13)))

    val f2 = IndexFunction(Vector('a, 'c, 'f, 'j, 'c))
    f2.width should be (5)
    f2.domain.toSet should equal ((0 until f2.width).toSet)
    f2.range.toSet should equal (Set('a, 'c, 'f, 'j))
    xyTest(f2, (0, 'a), (1, 'c), (2, 'f), (3, 'j), (4, 'c))
    propertyTest(f2, IndexFunction(Vector('a, 'c, 'f)), IndexFunction(Vector('j, 'r, 'z)))
  }

  it should "provide IndexFunction.apply method on ordered pairs" in {
    an [Exception] should be thrownBy IndexFunction(5, (1, 'a), (2, 'c), (4, 'f), (10, 'j))
    an [Exception] should be thrownBy IndexFunction(-1, (1, 'a), (2, 'c), (4, 'f))
    val f1 = IndexFunction(5, (1, 'a), (2, 'c), (4, 'f))
    f1.width should be (5)
    f1.domain.toSet should equal (Set(1, 2, 4))
    f1.range.toSet should equal (Set('a, 'c, 'f))
    xyTest(f1, (1, 'a), (2, 'c), (4, 'f))
    propertyTest(f1, IndexFunction(Vector('w)), IndexFunction(Vector('j, 'r, 'z)))
  }

  it should "provide IndexFunction.apply method on a map" in {
    an [Exception] should be thrownBy IndexFunction(5, Map((1, 'a), (2, 'c), (4, 'f), (10, 'z)))
    an [Exception] should be thrownBy IndexFunction(-1, Map((1, 'a), (2, 'c), (4, 'f)))
    val m = Map((1, 'a), (2, 'c), (4, 'f))
    val f1 = IndexFunction(5, m)
    f1.width should be (5)
    f1.domain.toSet should equal (Set(1, 2, 4))
    f1.range.toSet should equal (Set('a, 'c, 'f))
    xyTest(f1, (1, 'a), (2, 'c), (4, 'f))
    propertyTest(f1, IndexFunction(Vector('j, 'r, 'z)), IndexFunction(13, Map((1, 'q))))
  }
}

class InvertableIndexFunctionSpec extends FlatSpec with Matchers {
  import InvertableIndexFunctionSpecSupport._

  it should "enforce range type consistency during concatenation" in {
    "InvertableIndexFunction.empty[Int] ++ InvertableIndexFunction.empty[String]" shouldNot typeCheck
  }

  it should "provide InvertableIndexFunction.empty factory method" in {
    identityTest(InvertableIndexFunction.empty[Nothing])
    identityTest(InvertableIndexFunction.empty[Int])
    identityTest(InvertableIndexFunction.empty[String])
    identityTest(InvertableIndexFunction.empty[String] ++ InvertableIndexFunction.empty[String])
    propertyTest(
      InvertableIndexFunction.empty[String],
      InvertableIndexFunction.empty[String],
      InvertableIndexFunction.empty[String])
  }

  it should "provide InvertableIndexFunction.undefined factory method" in {
    an [Exception] should be thrownBy InvertableIndexFunction.undefined(-1)
    identityTest(InvertableIndexFunction.undefined[Int](0))
    undefinedTest(InvertableIndexFunction.undefined[Int](5))
    undefinedTest(InvertableIndexFunction.undefined[String](1))
    undefinedTest(InvertableIndexFunction.undefined[String](1) ++ InvertableIndexFunction.undefined[String](3))
    propertyTest(
      InvertableIndexFunction.undefined[Int](4),
      InvertableIndexFunction.undefined[Int](77),
      InvertableIndexFunction.undefined[Int](1000))
  }

  it should "provide InvertableIndexFunction.apply method on IndexedSeq" in {
    identityTest(InvertableIndexFunction(Vector()))

    val f1 = InvertableIndexFunction(Vector(2, 3, 5, 7, 3))
    f1.width should be (4)
    f1.domain.toSet should equal ((0 until f1.width).toSet)
    f1.range.toSet should equal (Set(2, 3, 5, 7))
    xyTest(f1, (0, 2), (1, 3), (2, 5), (3, 7))
    propertyTest(
      InvertableIndexFunction(Vector(1)),
      InvertableIndexFunction(Vector(2, 3, 5)),
      InvertableIndexFunction(Vector(7, 11, 13)))

    val f2 = InvertableIndexFunction(Vector('a, 'c, 'f, 'j, 'c))
    f2.width should be (4)
    f2.domain.toSet should equal ((0 until f2.width).toSet)
    f2.range.toSet should equal (Set('a, 'c, 'f, 'j))
    xyTest(f2, (0, 'a), (1, 'c), (2, 'f), (3, 'j))
    propertyTest(
      InvertableIndexFunction(Vector('a, 'c, 'f)),
      InvertableIndexFunction(Vector('u)),
      InvertableIndexFunction(Vector('j, 'r, 'z)))
  }

  it should "provide InvertableIndexFunction.apply method on ordered pairs" in {
    an [Exception] should be thrownBy InvertableIndexFunction(5, (1, 'a), (2, 'c), (4, 'f), (10, 'j))
    an [Exception] should be thrownBy InvertableIndexFunction(-1, (1, 'a), (2, 'c), (4, 'f))
    an [Exception] should be thrownBy InvertableIndexFunction(5, (1, 'a), (2, 'c), (4, 'a))
    val f1 = InvertableIndexFunction(5, (1, 'a), (2, 'c), (4, 'f))
    f1.width should be (5)
    f1.domain.toSet should equal (Set(1, 2, 4))
    f1.range.toSet should equal (Set('a, 'c, 'f))
    xyTest(f1, (1, 'a), (2, 'c), (4, 'f))
    propertyTest(
      f1,
      InvertableIndexFunction(Vector('g)),
      InvertableIndexFunction(Vector('j, 'r, 'z)))
  }

  it should "provide InvertableIndexFunction.apply method on a map" in {
    an [Exception] should be thrownBy InvertableIndexFunction(5, Map((1, 'a), (2, 'c), (4, 'f), (10, 'j)))
    an [Exception] should be thrownBy InvertableIndexFunction(-1, Map((1, 'a), (2, 'c), (4, 'f)))
    an [Exception] should be thrownBy InvertableIndexFunction(-1, Map((1, 'a), (2, 'c), (4, 'a)))
    identityTest(InvertableIndexFunction(0, Map.empty[Int, Double]))
    val m = Map((1, 'a), (2, 'c), (4, 'f))
    val f1 = InvertableIndexFunction(5, m)
    f1.width should be (5)
    f1.domain.toSet should equal (Set(1, 2, 4))
    f1.range.toSet should equal (Set('a, 'c, 'f))
    xyTest(f1, (1, 'a), (2, 'c), (4, 'f))
    propertyTest(
      f1,
      InvertableIndexFunction(3, (1, 'y)),
      InvertableIndexFunction(Vector('j, 'r, 'z)))
  }

  it should "provide InvertableIndexFunction.serialName method" in {
    an [Exception] should be thrownBy InvertableIndexFunction.serialName("foo", -1)
    identityTest(InvertableIndexFunction.serialName("foo", 0))
    val f1 = InvertableIndexFunction.serialName("foo", 3)
    f1.width should be (3)
    f1.domain.toSet should equal (Set(0, 1, 2))
    f1.range.toSet should equal (Set("foo0", "foo1", "foo2"))
    xyTest(f1, (0, "foo0"), (1, "foo1"), (2, "foo2"))
    f1.inverse.isDefinedAt("foo3") should be (false)
    f1.inverse.isDefinedAt("goo1") should be (false)
    propertyTest(
      f1,
      InvertableIndexFunction.serialName("cow", 33),
      InvertableIndexFunction(Vector("a", "b", "z")))
  }
}
