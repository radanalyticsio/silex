/*
 * cramersv.scala
 * author:  RJ Nowling <rnowling@redhat.com>
 *
 * Copyright (c) 2016 Red Hat, Inc.
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
 * limitations under the License.
 */

package com.redhat.et.silex.statistics

import com.redhat.et.silex.testing.PerTestSparkContext

import org.scalatest._

class CramersVSpec extends FlatSpec with Matchers with PerTestSparkContext {

  val eps = 1e-5

  "CramersV.cramersV" should "report 1.0 under perfect association" in {
    val values1 = Seq(1, 1, 1, 0, 0, 0, 2, 2, 2)
    val values2 = Seq(0, 0, 0, 1, 1, 1, 2, 2, 2)
    val values3 = Seq(1, 1, 1, 1, 2, 2, 1, 1)
    val values4 = Seq(3, 3, 3, 3, 4, 4, 3, 3)

    val sameV12 = CramersV.cramersV(values1, values1)
    val negatedV12 = CramersV.cramersV(values1, values2)
    val v34 = CramersV.cramersV(values3, values4)

    assert(math.abs(sameV12 - 1.0) < eps)
    assert(math.abs(negatedV12 - 1.0) < eps)
    assert(math.abs(v34 - 1.0) < eps)
  }

  "CramersV.cramersV" should "throw IllegalArgumentException when given values of unequal lengths" in {
    val values1 = Seq(1, 2, 3)
    val values2 = Seq(1, 2)

    intercept[IllegalArgumentException] {
      CramersV.cramersV(values1, values2)
    }
  }

  "CramersV.cramersV" should "report 0.0 with no association" in {
    val values1 = Seq(1, 1, 1, 1, 0, 0, 0, 0)
    val values2 = Seq(0, 1, 0, 1, 1, 0, 1, 0)
 
    val v = CramersV.cramersV(values1, values2)

    assert(v < eps) 
  }

  "CramersV.cramersV" should "report 1.0 with single-value sets" in {
    val values1 = Seq(1, 1, 1, 1)
    val values2 = Seq(1, 1, 1, 1)

    val v = CramersV.cramersV(values1, values2)

    assert(math.abs(v - 1.0) < eps)
  }

  "CramersV.cramersV" should "report 0.0 when one variable has 1 value and the other has more" in {
    val values1 = Seq(1, 1, 1, 1)
    val values2 = Seq(1, 2, 3, 4)

    val v = CramersV.cramersV(values1, values2)

    assert(v < eps)
  }

  "CramersV.cramersV" should "report 0.0 for empty sets" in {
    val v = CramersV.cramersV(Seq[Int](), Seq[Int]())

    assert(v < eps)
  }

  "CramersV.permutationTest" should "report p-value 1.0 under perfect correlation" in {
    val values = (1 to 100).flatMap {
      i =>
        Seq(i, i)
    }
    
    val pvalue = CramersV.permutationTest(values, values, 1000)

    assert(math.abs(pvalue - 1.0) < eps)
  }
}
