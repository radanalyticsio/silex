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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD

import com.redhat.et.silex.sample.iid.IIDFeatureSamplingMethods
import com.redhat.et.silex.feature.extractor.FeatureSeq

/** Implementation-specific subclass of [[IIDFeatureSamplingMethods]] for RDDs */
class IIDFeatureSamplingMethodsRDD(data: RDD[Seq[Double]]) extends IIDFeatureSamplingMethods {

  import com.redhat.et.silex.sample.iid.infra._

  /** @inheritdoc */
  def iidFeatureSeqRDD(
      n: Int,
      iSS: Int = 10000,
      oSS: Int = 10000
      ): RDD[FeatureSeq] = {
    require(n >= 0)
    require(iSS >= 0)
    require(oSS >= 0)
    require(n == 0 || (iSS > 0 && oSS > 0))

    logInfo("Computing data sampling ratio")
    val N = data.count
    require(n == 0 || N > 0L)

    val M = data.first.length
    val fraction = if (N > 0L) math.min(1.0, iSS.toDouble / N.toDouble) else 0.0

    var remaining = n
    val parts = ArrayBuffer.empty[RDD[FeatureSeq]]
    while (remaining > 0) {
      logInfo(f"Sampling elements using sampling ratio $fraction%.3g")
      val sample = data.sample(false, fraction).collect

      logInfo("Accumulating marginal distributions")
      val csa = Vector.fill(M) { new CompactSamplerAccumulator }
      sample.foreach { s =>
        require(s.length == M)
        s match {
          case fs: FeatureSeq if (fs.density < 0.5) => {
            val kItr = fs.activeKeysIterator
            val vItr = fs.activeValuesIterator
            while (kItr.hasNext) { csa(kItr.next) += vItr.next }
          }
          case _ => {
            val cItr = csa.iterator
            val vItr = s.iterator
            while (vItr.hasNext) { cItr.next += vItr.next }
          }
        }
      }

      val nn = math.min(remaining, oSS)
      logInfo("Constructing $nn i.i.d. samples")
      val cse = new CompactSamplerExtractor(csa.map(_.sampler(sample.length)))
      val iid = ArrayBuffer.empty[FeatureSeq]
      while (iid.length < nn) { iid += cse(()) }

      parts += data.sparkContext.parallelize(iid)
      remaining -= nn
      logInfo(s"Completed ${n-remaining} / $n i.i.d. feature sequences")
    }

    if (parts.length > 0) {
      new org.apache.spark.rdd.UnionRDD(data.sparkContext, parts)
    } else {
      data.sparkContext.emptyRDD[FeatureSeq]
    }
  }
}
