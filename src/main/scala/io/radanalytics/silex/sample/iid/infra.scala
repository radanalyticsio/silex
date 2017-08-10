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
 * limitations under the License.
 */

package io.radanalytics.silex.sample.iid

private [sample] object infra {
  import scala.collection.mutable.ArrayBuffer

  import org.apache.spark.mllib.linalg.{
    DenseVector => DenseSV,
    SparseVector => SparseSV
  }

  import io.radanalytics.silex.feature.extractor.spark.implicits._
  import io.radanalytics.silex.feature.extractor.{ FeatureSeq, Extractor }

  class CompactSampler(n: Int, n1: Int, non01: ArrayBuffer[Double]) {
    require(n > 0  &&  n1 >= 0)
    require((n1 + non01.length) <= n)
    private val n0 = n - n1 - non01.length
    private val t0 = n0.toDouble / n.toDouble
    private val t1 = if (non01.isEmpty) 1.0 else t0 + (n1.toDouble / n.toDouble)

    def sample = {
      if (n0 == n) 0.0
      else {
        val x = scala.util.Random.nextDouble
        if (x <= t0) 0.0
        else if (x <= t1) 1.0
        else non01(scala.util.Random.nextInt(non01.length))
      }
    }

    override def toString = s"CompactSampler($t0, ${t1-t0}, $non01)"
  }

  object CompactSampler {
    def extractor(samplers: Vector[CompactSampler]) = {
      val width = samplers.length
      val rho = samplers.count(_.sample != 0.0).toDouble / width.toDouble
      val dense = (width < 10) || (rho >= 0.5)
      Extractor(
        width,
        if (dense) {
          (unused: Unit) => FeatureSeq(new DenseSV(samplers.iterator.map(_.sample).toArray))
        } else {
          (unused: Unit) => {
            var j = 0
            val vals = ArrayBuffer.empty[Double]
            val keys = ArrayBuffer.empty[Int]
            samplers.foreach { s =>
              val v = s.sample
              if (v != 0.0) {
                vals += v
                keys += j
              }
              j += 1
            }
            FeatureSeq(new SparseSV(width, keys.toArray, vals.toArray))
          }
        })
    }
  }

  // This class assumes that total samples (n) is being kept track of externally
  class CompactSamplerAccumulator {
    private var n1 = 0
    private var non01 = ArrayBuffer.empty[Double]

    def += (v: Double) {
      if (v == 1.0) {
        n1 += 1
      } else if (v != 0.0) {
        non01 += v
      }
    }

    def sampler(n: Int) = new CompactSampler(n, n1, non01)

    override def toString = s"CompactSamplerAccumulator($n1, $non01)"
  }
}
