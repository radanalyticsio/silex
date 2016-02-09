/*
 * This file is part of the "silex" library of helpers for Apache Spark.
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
 * limitations under the License
 */
package com.redhat.et.silex.rdd.multiplex

import org.scalatest._

import com.redhat.et.silex.testing.PerTestSparkContext

class SplitSampleSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import com.redhat.et.silex.testing.matchers._
  import com.redhat.et.silex.sample.split.implicits._
  import com.redhat.et.silex.testing.KSTesting.{ medianKSD, SamplingIterator, D }

  scala.util.Random.setSeed(235711)

  it should "provide splitSample with integer argument" in {
    val rdd = context.parallelize(1 to 10000, 4)
    val rsRes = rdd.randomSplit(Array(1.0, 1.0, 1.0)).toSeq
      .map(_.collect.seq.sliding(2).map(e => e(1) - e(0)).toVector)
    val ssRes = rdd.splitSample(3)
      .map(_.collect.seq.sliding(2).map(e => e(1) - e(0)).toVector)
    val Dvals = rsRes.zip(ssRes).map { case (rs, ss) =>
      medianKSD(SamplingIterator { rs }, SamplingIterator { ss })
    }
    Dvals.forall(_ < D) should be (true)
  }

  it should "provide weightedSplitSample with weights argument" in {
    val rdd = context.parallelize(1 to 10000, 4)
    val rsRes = rdd.randomSplit(Array(1.0, 2.0, 3.0)).toSeq
      .map(_.collect.seq.sliding(2).map(e => e(1) - e(0)).toVector)
    val ssRes = rdd.weightedSplitSample(Seq(1.0, 2.0, 3.0))
      .map(_.collect.seq.sliding(2).map(e => e(1) - e(0)).toVector)
    val Dvals = rsRes.zip(ssRes).map { case (rs, ss) =>
      medianKSD(SamplingIterator { rs }, SamplingIterator { ss })
    }
    Dvals.forall(_ < D) should be (true)
  }  
}
