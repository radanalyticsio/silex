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

package com.redhat.et.silex.sample.iid.rdd

import com.redhat.et.silex.testing.PerTestSparkContext

import org.scalatest._

import com.redhat.et.silex.testing.matchers._
import com.redhat.et.silex.testing.KSTesting.{ medianKSD, SamplingIterator, D }

class IIDFeatureSamplingMethodsRDDSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import com.redhat.et.silex.sample.iid.implicits._

  def iidTest(data: SamplingIterator[Seq[Double]]) = {
    val idxSet = (0 until data.next.length).toVector
    idxSet.combinations(2).flatMap(_.permutations).foreach { idxPair =>
      val (j, r) = (idxPair(0), idxPair(1))
      val rvals = data.sample(1000).map(_(r)).distinct
      val du = data.map(_(j))
    }
    true
  }

  scala.util.Random.setSeed(23571113)

  it should "sample sequences i.i.d" in {
    // Data that has a joint distribution very different than i.i.d. of its marginals
    val data = (1 to 10000).map { unused =>
      if (scala.util.Random.nextDouble() < 0.2) Seq(0.0, 1.0) else Seq(1.0, 0.0)
    }
    val rdd = context.parallelize(data, 1)

    val iid = rdd.iidFeatureSeqRDD(9999, iSS = 1000, oSS = 1000).collect
    iid.length should be (9999)

    medianKSD(
      SamplingIterator { iid.map(_(0)) },
      SamplingIterator.continually { if (scala.util.Random.nextDouble() < 0.2) 0.0 else 1.0 }
    ) should be < D

    medianKSD(
      SamplingIterator { iid.map(_(1)) },
      SamplingIterator.continually { if (scala.util.Random.nextDouble() < 0.2) 1.0 else 0.0 }
    ) should be < D

    iidTest(SamplingIterator { iid }) should be (true)
  }
}
