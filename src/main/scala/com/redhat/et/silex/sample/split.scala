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
 * limitations under the License.c
 */

package com.redhat.et.silex.sample.split

import scala.reflect.ClassTag

import org.apache.spark.storage.StorageLevel

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

/**
 * Enhances RDDs with methods for split-sampling
 * @tparam T The row type of the RDD
 * {{{
 * // import conversions to enhance RDDs with split sampling
 * import com.redhat.et.silex.sample.split.implicits._
 *
 * // obtain a sequence of 5 RDDs randomly split from RDD 'data', where each element
 * // has probability 1/5 of being assigned to each output.
 * val splits = data.splitSample(5)
 *
 * // randomly split data so that the second output has twice the probability of receiving 
 * // a data element as the first, and the third output has three times the probability.
 * val splitsW = data.weightedSplitSample(Seq(1.0, 2.0, 3.0))
 * }}}
 */
class SplitSampleRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {
  import com.redhat.et.silex.rdd.multiplex.implicits._

  import SplitSampleRDDFunctions.{defaultSL, find}

  /**
   * Split an RDD into `n` random subsets, where each row is assigned to an output with
   * equal probability 1/n.
   * @param n The number of output RDDs to split into
   * @param persist The storage level to use for persisting the intermediate result.
   * @param seed A random seed to use for sampling.  Will be modified, deterministically, 
   * by partition id.
   */
  def splitSample(n: Int,
    persist: StorageLevel = defaultSL,
    seed: Long = scala.util.Random.nextLong): Seq[RDD[T]] = {
    require(n > 0, "n must be > 0")
    self.flatMuxPartitions(n, (id: Int, data: Iterator[T]) => {
      scala.util.Random.setSeed(id.toLong * seed)
      val samples = Vector.fill(n) { scala.collection.mutable.ArrayBuffer.empty[T] }
      data.foreach { e => samples(scala.util.Random.nextInt(n)) += e }
      samples
    }, persist)
  }

  /**
   * Split an RDD into weighted random subsets, where each row is assigned to an output (j) with
   * probability proportional to the corresponding jth weight.
   * @param weights A sequence of weights that determine the relative probabilities of
   * sampling into the corresponding RDD outputs.  Weights will be normalized so that they
   * sum to 1.  Individual weights must be strictly > 0.
   * @param persist The storage level to use for persisting the intermediate result.
   * @param seed A random seed to use for sampling.  Will be modified, deterministically, 
   * by partition id.
   */
  def weightedSplitSample(weights: Seq[Double],
    persist: StorageLevel = defaultSL,
    seed: Long = scala.util.Random.nextLong): Seq[RDD[T]] = {
    require(weights.length > 0, "weights must be non-empty")
    require(weights.forall(_ > 0.0), "weights must be > 0")
    val n = weights.length
    val z = weights.sum
    val w = weights.scan(0.0)(_ + _).map(_ / z).toVector
    self.flatMuxPartitions(n, (id: Int, data: Iterator[T]) => {
      scala.util.Random.setSeed(id.toLong * seed)
      val samples = Vector.fill(n) { scala.collection.mutable.ArrayBuffer.empty[T] }
      data.foreach { e =>
        val x = scala.util.Random.nextDouble
        val j = find(x, w)
        samples(j) += e
      }
      samples
    }, persist)
  }
}

/** Definitions used by SplitSampleRDDFunctions instances */
object SplitSampleRDDFunctions {
  /** The default storage level used for intermediate sampling results */
  val defaultSL = StorageLevel.MEMORY_ONLY

  private def find(x: Double, w: Seq[Double]) = {
    var (l, u) = (0, w.length - 1)
    if (x >= 1.0) u - 1
    else {
      var m = (l + u) / 2
      while (m > l) {
        if (x < w(m)) u = m
        else if (x >= w(m + 1)) l = m + 1
        else { l = m; u = m + 1 }
        m = (l + u) / 2
      }
      m
    }
  }
}

/** Implicit conversions to enhance RDDs with split sampling methods */
object implicits {
  import scala.language.implicitConversions
  implicit def splitSampleRDDFunctions[T :ClassTag](rdd: RDD[T]): SplitSampleRDDFunctions[T] =
    new SplitSampleRDDFunctions(rdd)
}
