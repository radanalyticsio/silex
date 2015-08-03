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

package com.redhat.et.silex.util

import org.scalatest._

import com.redhat.et.silex.testing.matchers._

object OptionalArgProperties extends FlatSpec with Matchers {
  def testEmpty[A](a: OptionalArg[A], d: => A) = {
    a.isEmpty should be (true)
    a.isDefined should be (false)
    an [Exception] should be thrownBy a.get
    a.getOrElse(d) should be (d)
    true
  }
}

class OptionalArgSpec extends FlatSpec with Matchers {
  import OptionalArgProperties._

  it should "support empty factory method" in {
    val a = OptionalArg.empty[Int]
    testEmpty(a, { 42 })
  }

  it should "support apply factory method on value type A" in {
    OptionalArg(3).get should be (3)
    OptionalArg("foo").get should be ("foo")
  }

  it should "support apply factory method on Option[A]" in {
    OptionalArg(Some(3)).get should be (3)
    OptionalArg(Some("foo")).get should be ("foo")
    val o: Option[Int] = None
    testEmpty(OptionalArg(o), { 21 * 2 })
  }

  it should "provide underlying option" in {
    OptionalArg(1.0).option should be(Some(1.0))
    OptionalArg("goo").option should be(Some("goo"))
    OptionalArg.empty[Int].option should be(None)
  }

  it should "provide isEmpty" in {
    OptionalArg.empty[Int].isEmpty should be (true)
    OptionalArg(3.14).isEmpty should be (false)
  }

  it should "provide isDefined" in {
    OptionalArg.empty[Int].isDefined should be (false)
    OptionalArg(3.14).isDefined should be (true)
  }

  it should "provide get" in {
    an [Exception] should be thrownBy OptionalArg.empty[String].get
    OptionalArg(3).get should be (3)
  }

  it should "provide getOrElse" in {
    OptionalArg("x").getOrElse { "z" } should be("x")
    OptionalArg.empty[Int].getOrElse { 7 } should be(7)
  }

  it should "provide fullOptionSupport" in {
    import OptionalArg.fullOptionSupport
    (OptionalArg(5) : Option[Int]) should be(Some(5))
    OptionalArg("xxx").map(_ + "z").get should be("xxxz")
    OptionalArg(3.14).iterator should beEqSeq(Seq(3.14))
  }

  it should "provide nice optional arguments" in {
    def f(a1: OptionalArg[Int] = None, a2: OptionalArg[String] = None) =
      (a1.getOrElse(77), a2.getOrElse("xxx"))

    f() should be(((77, "xxx")))
    f(a1 = 55) should be(((55, "xxx")))
    f(a2 = "goo") should be (((77, "goo")))
    f(a2 = "zzz", a1 = 11) should be (((11, "zzz")))

    f(a1 = None) should be(((77, "xxx")))
    f(a2 = None) should be(((77, "xxx")))
    f(a1 = Some(55)) should be(((55, "xxx")))    
    f(a2 = Some("goo")) should be (((77, "goo")))
  }
}
