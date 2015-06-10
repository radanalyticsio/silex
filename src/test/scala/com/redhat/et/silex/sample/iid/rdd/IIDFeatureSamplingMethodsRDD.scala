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
    var success = true
    val idxSet = (0 until data.next.length).toVector
    idxSet.combinations(2).flatMap(_.permutations).foreach { idxPair =>
      val (j, r) = (idxPair(0), idxPair(1))
      // distinct values in column (r)
      val rvals = data.fork.map(_(r)).sample(1000).distinct
      // unconditioned sampling from column (j)
      val du = data.fork.map(_(j))
      // unconditioned sampling should have same distribution as sampling conditioned on
      // any value from column (r), otherwise the columns aren't independent distributions
      rvals.foreach { v =>
        val dc = data.fork.filter(s => s(r) == v).map(_(j))
        if (medianKSD(du, dc) > D) {
          success = false
        }
      }
    }
    success
  }

  scala.util.Random.setSeed(23571113)

  it should "sample sequences of zeros and ones i.i.d" in {
    // Data that has a joint distribution very different than i.i.d. of its marginals
    val data = Vector.fill(10000) {
      val t = scala.util.Random.nextDouble()
      if      (t < 0.2) Seq(1.0, 0.0, 0.0)
      else if (t < 0.5) Seq(0.0, 1.0, 0.0)
      else              Seq(0.0, 0.0, 1.0)
    }
    // Verify that columns of data are not independent
    iidTest(SamplingIterator { data }) should be (false)

    val rdd = context.parallelize(data, 1)

    val iid = rdd.iidFeatureSeqRDD(9999, iSS = 1000, oSS = 1000).collect
    iid.length should be (9999)

    // Individual marginal distributions should remain the same
    (0 until data.head.length).foreach { j =>
      medianKSD(
        SamplingIterator { iid.map(_(j)) },
        SamplingIterator { data.map(_(j)) }
      ) should be < D
    }

    // Verify that sampled columns are statistically independent of each other
    iidTest(SamplingIterator { iid }) should be (true)
  }

  it should "sample sequences of different values i.i.d" in {
    // Data that has a joint distribution very different than i.i.d. of its marginals
    val data = Vector.fill(10000) {
      val t = scala.util.Random.nextDouble()
      if      (t < 0.2) Seq(1.0, 0.0, 2.0)
      else if (t < 0.5) Seq(3.0, 3.0, 7.0)
      else              Seq(2.0, 8.0, 0.0)
    }
    // Verify that columns of data are not independent
    iidTest(SamplingIterator { data }) should be (false)

    val rdd = context.parallelize(data, 1)

    val iid = rdd.iidFeatureSeqRDD(9999, iSS = 1000, oSS = 1000).collect
    iid.length should be (9999)

    // Individual marginal distributions should remain the same
    (0 until data.head.length).foreach { j =>
      medianKSD(
        SamplingIterator { iid.map(_(j)) },
        SamplingIterator { data.map(_(j)) }
      ) should be < D
    }

    // Verify that sampled columns are statistically independent of each other
    iidTest(SamplingIterator { iid }) should be (true)
  }
}
