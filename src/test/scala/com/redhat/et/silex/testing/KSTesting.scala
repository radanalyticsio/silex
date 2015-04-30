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

/**
  * There are no actual KS tests implemented for scala (that I can find) - and so what I
  * have done here is pre-compute "D" - the KS statistic - that corresponds to a "weak"
  * p-value for a particular sample size.  I can then test that my measured KS stats
  * are less than D.  Computing D-values is easy, and implemented below.
  *
  * I used the scipy 'kstwobign' distribution to pre-compute my D value:
  *
  * def ksdval(q=0.1, n=1000):
  *     en = np.sqrt(float(n) / 2.0)
  *     return stats.kstwobign.isf(float(q)) / (en + 0.12 + 0.11 / en)
  *
  * When comparing KS stats I take the median of a small number of independent test runs
  * to compensate for the issue that any sampled statistic will show "false positive" with
  * some probability.  Even when two distributions are the same, they will register as
  * different 10% of the time at a p-value of 0.1
  */
object KSTesting {

  // This D value is the precomputed KS statistic for p-value 0.1, sample size 1000:
  // to-do: support other predefined sample sizes and p-values?  maybe via lookup table?
  val sampleSize = 1000
  val D = 0.0544280747619

  // Computes the Kolmogorov-Smirnov 'D' statistic from two cumulative distributions
  def KSD(cdf1: Seq[Double], cdf2: Seq[Double]): Double = {
    require(cdf1.length == cdf2.length)
    val n = cdf1.length
    require(n > 0)
    require(cdf1(n-1) == 1.0)
    require(cdf2(n-1) == 1.0)
    cdf1.zip(cdf2).map { x => Math.abs(x._1 - x._2) }.max
  }

  // Returns the median KS 'D' statistic between two samples, over (m) sampling trials
  // to-do: generalize to support any N <: Numeric
  def medianKSD(data1: => Iterator[Int], data2: => Iterator[Int], m: Int = 5): Double = {
    val t = Array.fill[Double](m) {
      val (c1, c2) = cumulants(data1.take(sampleSize).toVector,
                               data2.take(sampleSize).toVector)
      KSD(c1, c2)
    }.sorted
    // return the median KS statistic
    t(m / 2)
  }

  // Returns the cumulative distribution from a histogram
  private def cumulativeDist(hist: Array[Int]): Array[Double] = {
    val n = hist.sum.toDouble
    require(n > 0.0)
    hist.scanLeft(0)(_ + _).drop(1).map { _.toDouble / n }
  }

  // Returns aligned cumulative distributions from two arrays of data
  // to-do: generalize to support any N <: Numeric
  private def cumulants(d1: Seq[Int], d2: Seq[Int],
      ss: Int = sampleSize): (Array[Double], Array[Double]) = {
    require(math.min(d1.length, d2.length) > 0)
    require(math.min(d1.min, d2.min)  >=  0)
    val m = 1 + math.max(d1.max, d2.max)
    val h1 = Array.fill[Int](m)(0)
    val h2 = Array.fill[Int](m)(0)
    for (v <- d1) { h1(v) += 1 }
    for (v <- d2) { h2(v) += 1 }
    require(h1.sum == h2.sum)
    require(h1.sum == ss)
    (cumulativeDist(h1), cumulativeDist(h2))
  }
}
