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

class IndexFunctionSpec extends FlatSpec with Matchers {
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

  def xyTest[V](f: IndexFunction[V], xy: (Int, V)*) {
    xy.foreach { xy =>
      val (x, y) = xy
      f.isDefinedAt(x) should be (true)
      f(x) should be (y)
    }
  }

  it should "provide IndexFunction.empty factory method" in {
    identityTest(IndexFunction.empty[Nothing])
    identityTest(IndexFunction.empty[Int])
    identityTest(IndexFunction.empty[String])
    identityTest(IndexFunction.empty[String] ++ IndexFunction.empty[String])
    opTest(IndexFunction.empty[String], IndexFunction.empty[String])
  }

  it should "enforce range type consistency during concatenation" in {
    "IndexFunction.empty[Int] ++ IndexFunction.empty[String]" shouldNot typeCheck
  }

  it should "provide IndexFunction.undefined factory method" in {
    identityTest(IndexFunction.undefined[Int](0))
    undefinedTest(IndexFunction.undefined[Int](5))
    undefinedTest(IndexFunction.undefined[String](1))
    undefinedTest(IndexFunction.undefined[String](1) ++ IndexFunction.undefined[String](3))
    opTest(IndexFunction.undefined[Int](4), IndexFunction.undefined[Int](1000))
    an [Exception] should be thrownBy IndexFunction.undefined(-1)
  }

  it should "provide IndexFunction.constant factory method" in {
    identityTest(IndexFunction.constant("a", 0))

    val f1 = IndexFunction.constant(42, 42)
    (0 until f1.width).foreach { j =>
      f1.isDefinedAt(j) should be (true)
      f1(j) should be (42)
    }
    drTest(f1)
    opIdentityTest(f1)
    opTest(IndexFunction.constant(42, 42), IndexFunction.constant(42, 6 * 9))

    val f2 = IndexFunction.constant("6 * 9", 42)
    (0 until f2.width).foreach { j =>
      f2.isDefinedAt(j) should be (true)
      f2(j) should be ("6 * 9")
    }
    drTest(f2)
    opIdentityTest(f2)
    opTest(IndexFunction.constant("6 * 9", 42), IndexFunction.constant("42", 6 * 9))
  }

  it should "provide IndexFunction.apply method on IndexedSeq" in {
    identityTest(IndexFunction(Vector()))

    val f1 = IndexFunction(Vector(2, 3, 5, 7, 3))
    f1.width should be (5)
    f1.domain.toSet should equal ((0 until f1.width).toSet)
    f1.range.toSet should equal (Set(2, 3, 5, 7))
    xyTest(f1, (0, 2), (1, 3), (2, 5), (3, 7), (4, 3))
    drTest(f1)
    opIdentityTest(f1)
    opTest(IndexFunction(Vector(2, 3, 5)), IndexFunction(Vector(7, 11, 13)))

    val f2 = IndexFunction(Vector('a, 'c, 'f, 'j, 'c))
    f2.width should be (5)
    f2.domain.toSet should equal ((0 until f2.width).toSet)
    f2.range.toSet should equal (Set('a, 'c, 'f, 'j))
    xyTest(f2, (0, 'a), (1, 'c), (2, 'f), (3, 'j), (4, 'c))
    drTest(f2)
    opIdentityTest(f2)
    opTest(IndexFunction(Vector('a, 'c, 'f)), IndexFunction(Vector('j, 'r, 'z)))    
  }

  it should "provide IndexFunction.apply method on ordered pairs" in {
    val f1 = IndexFunction(5, (1, 'a), (2, 'c), (4, 'f), (10, 'j))
    f1.width should be (5)
    f1.domain.toSet should equal (Set(1, 2, 4))
    f1.range.toSet should equal (Set('a, 'c, 'f))
    xyTest(f1, (1, 'a), (2, 'c), (4, 'f))
    drTest(f1)
    opIdentityTest(f1)
    opTest(f1, IndexFunction(Vector('j, 'r, 'z)))    
  }

  it should "provide IndexFunction.apply method on a map" in {
    val m = Map((1, 'a), (2, 'c), (4, 'f), (10, 'j))
    val f1 = IndexFunction(5, m)
    f1.width should be (5)
    f1.domain.toSet should equal (Set(1, 2, 4))
    f1.range.toSet should equal (Set('a, 'c, 'f))
    xyTest(f1, (1, 'a), (2, 'c), (4, 'f))
    drTest(f1)
    opIdentityTest(f1)
    opTest(f1, IndexFunction(Vector('j, 'r, 'z)))    
  }
}
