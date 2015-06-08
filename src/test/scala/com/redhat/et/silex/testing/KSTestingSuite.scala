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

package com.redhat.et.silex.testing

import scala.util.Random
import org.scalatest._

class KSTestingSpec extends FlatSpec with Matchers {
  scala.util.Random.setSeed(23571113)

  it should "sanity check KSD" in {
    val c1 = Vector(0.4, 0.8, 1.0, 1.0)
    val c2 = Vector(0.2, 0.6, 0.8, 1.0)
    KSTesting.KSDStatistic(c1, c2) should be (0.2 +- 0.000001)
    KSTesting.KSDStatistic(c2, c1) should be (KSTesting.KSDStatistic(c1, c2))
  }

  it should "sanity check medianKSD" in {
    var d: Double = 0.0

    // should be statistically same, i.e. fail to reject null hypothesis strongly
    KSTesting.medianKSD(
      KSTesting.SamplingIterator { Iterator.single(scala.util.Random.nextInt(10)) },
      KSTesting.SamplingIterator { Iterator.single(scala.util.Random.nextInt(10)) }
    ) should be < KSTesting.D

    // should be statistically different
    KSTesting.medianKSD(
      KSTesting.SamplingIterator { Iterator.single(scala.util.Random.nextInt(9)) },
      KSTesting.SamplingIterator { Iterator.single(scala.util.Random.nextInt(10)) }
    ) should be > KSTesting.D
  }
}
