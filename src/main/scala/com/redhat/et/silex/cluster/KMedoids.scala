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

package com.redhat.et.silex.cluster

import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.util.random.XORShiftRandom

class KMedoids[T] private (
  private var metric: (T, T) => Double,
  private var k: Int,
  private var maxIterations: Int,
  private var epsilon: Double,
  private var fractionEpsilon: Double,
  private var sampleSize: Int,
  private var seed: Long) extends Serializable with Logging {

  def this(metric: (T, T) => Double) = this(
    metric,
    KMedoids.default.k,
    KMedoids.default.maxIterations,
    KMedoids.default.epsilon,
    KMedoids.default.fractionEpsilon,
    KMedoids.default.sampleSize,
    KMedoids.default.seed)

  def medoidDist(e: T, mv: Seq[T]) = mv.iterator.map(metric(e, _)).min
  def medoidIdx(e: T, mv: Seq[T]) = mv.iterator.map(metric(e, _)).zipWithIndex.min._2
  def medoidCost(e: T, data: Seq[T]) = data.iterator.map(metric(e, _)).sum
  def medoid(data: Seq[T]) = data.iterator.minBy(medoidCost(_, data))
  def modelCost(mv: Seq[T], data: Seq[T]) = data.iterator.map(medoidDist(_, mv)).sum

  def setMetric(metric: (T, T) => Double): this.type = {
    this.metric = metric
    this
  }

  def setK(k: Int): this.type = {
    require(k > 0, s"k= $k must be > 0")
    this.k = k
    this
  }

  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations > 0, s"maxIterations= $maxIterations must be > 0")
    this.maxIterations = maxIterations
    this
  }

  def setEpsilon(epsilon: Double): this.type = {
    require(epsilon >= 0.0, s"epsilon= $epsilon must be >= 0.0")
    this.epsilon = epsilon
    this
  }

  def setFractionEpsilon(fractionEpsilon: Double): this.type = {
    require(fractionEpsilon >= 0.0, s"fractionEpsilon= $fractionEpsilon must be >= 0.0")
    this.fractionEpsilon = fractionEpsilon
    this
  }

  def setSampleSize(sampleSize: Int): this.type = {
    require(sampleSize > 0, s"sampleSize= $sampleSize must be > 0")
    this.sampleSize = sampleSize
    this
  }

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  def run(data: RDD[T]) = {
    val runStartTime = System.nanoTime
    val rng = new scala.util.Random(seed)
    logInfo(s"collecting data sample")
    val sample = KMedoids.sampleBySize(data, sampleSize, rng.nextLong)
    logInfo(s"sample size= ${sample.length}")
    val model = doRun(sample, rng)
    val runSeconds = (System.nanoTime - runStartTime) / 1e9
    logInfo(f"total clustering time= $runSeconds%.1f sec")
    model
  }

  def run(data: Seq[T]) = {
    val runStartTime = System.nanoTime
    val rng = new scala.util.Random(seed)
    logInfo(s"collecting data sample")
    val sample = KMedoids.sampleBySize(data, sampleSize, rng.nextLong)
    logInfo(s"sample size= ${sample.length}")
    val model = doRun(sample, rng)
    val runSeconds = (System.nanoTime - runStartTime) / 1e9
    logInfo(f"total clustering time= $runSeconds%.1f sec")
    model
  }

  private def doRun(data: Seq[T], rng: scala.util.Random) = {
    val startTime = System.nanoTime
    logInfo(s"initializing model from $k random elements")
    var current = KMedoids.sampleDistinct(data, k, rng)
    var currentCost = modelCost(current, data)

    val itrStartTime = System.nanoTime
    val initSeconds = (itrStartTime - startTime) / 1e9
    logInfo(f"model initialization completed $initSeconds%.1f sec")

    logInfo(s"refining model")
    val (refined, refinedCost, itr, converged) = refine(data, current, currentCost)

    val avgSeconds = (System.nanoTime - itrStartTime) / 1e9 / itr
    logInfo(f"finished at $itr iterations with model cost= $refinedCost%.6g   avg sec per iteration= $avgSeconds%.1f")
    new KMedoidsModel(refined, metric)
  }

  private def refine(
    data: Seq[T],
    initial: Seq[T],
    initialCost: Double): (Seq[T], Double, Int, Boolean) = {

    val runStartTime = System.nanoTime

    var current = initial
    var currentCost = initialCost
    var converged = false

    val itrStartTime = System.nanoTime

    var itr = 1
    var halt = false
    while (!halt) {
      val itrTime = System.nanoTime
      val itrSeconds = (itrTime - itrStartTime) / 1e9
      logInfo(f"iteration $itr  cost= $currentCost%.6g  elapsed= $itrSeconds%.1f")

      val next = data.groupBy(medoidIdx(_, current)).toVector.sortBy(_._1).map(_._2).map(medoid)
      val nextCost = modelCost(next, data)

      val curSeconds = (System.nanoTime - itrTime) / 1e9
      logInfo(f"updated cost= $nextCost%.6g  elapsed= $curSeconds%.1f sec")

      val delta = currentCost - nextCost
      val fractionDelta = if (currentCost > 0.0) delta / currentCost else 0.0

      if (delta <= epsilon) {
        logInfo(f"converged with delta= $delta%.4g")
        halt = true
        converged = true
      } else if (fractionDelta <= fractionEpsilon) {
        logInfo(f"converged with fractionDelta= $fractionDelta%.4g")
        halt = true
        converged = true
      } else if (itr >= maxIterations) {
        logInfo(s"halting at maximum iteration $itr")
        halt = true
      }

      if (!halt) {
        itr += 1
        current = next
        currentCost = nextCost
      } else if (nextCost < currentCost) {
        current = next
        currentCost = nextCost
      }
    }

    val runTime = System.nanoTime
    val runSeconds = (runTime - runStartTime) / 1e9
    logInfo(f"refined over $itr iterations  final cost= $currentCost%.6g  elapsed= $runSeconds%.1f")
    (current, currentCost, itr, converged)
  }
}

object KMedoids extends Logging {
  private[cluster] object default {
    def k = 2
    def maxIterations = 25
    def epsilon = 0.0
    def fractionEpsilon = 0.0001
    def sampleSize = 1000
    def seed = scala.util.Random.nextLong()
  }

  def sampleFraction[N :Numeric](n: N, sampleSize: Int): Double = {
    val num = implicitly[Numeric[N]]
    require(num.gteq(n, num.zero), "n must be >= 0")
    require(sampleSize >= 0, "sampleSize must be >= 0")
    if (sampleSize <= 0 || num.lteq(n, num.zero)) {
      0.0
    } else {
      val nD = num.toDouble(n)
      val ss = math.min(sampleSize.toDouble, nD)
      val fraction = math.min(1.0, ss / nD)
      fraction
    }
  }

  def sampleBySize[T](data: RDD[T], sampleSize: Int, seed: Long): Seq[T] = {
    require(sampleSize >= 0, "sampleSize must be >= 0")
    val fraction = sampleFraction(data.count, sampleSize)
    if (fraction <= 0.0) {
      Seq.empty[T]
    } else if (fraction >= 1.0) {
      data.collect.toSeq
    } else {
      data.sample(false, fraction, seed = seed).collect.toSeq
    }
  }

  def sampleBySize[T](data: RDD[T], sampleSize: Int): Seq[T] =
    sampleBySize(data, sampleSize, scala.util.Random.nextLong())

  def sampleBySize[T](data: Seq[T], sampleSize: Int, seed: Long): Seq[T] = {
    require(sampleSize >= 0, "sampleSize must be >= 0")
    val fraction = sampleFraction(data.length, sampleSize)
    if (fraction <= 0.0) {
      Seq.empty[T]
    } else if (fraction >= 1.0) {
      data
    } else {
      val rng = new scala.util.Random(seed)
      data.filter(x => rng.nextDouble() < fraction)
    }
  }

  def sampleBySize[T](data: Seq[T], sampleSize: Int): Seq[T] =
    sampleBySize(data, sampleSize, scala.util.Random.nextLong())

  def sampleDistinct[T](data: Seq[T], k: Int, rng: scala.util.Random): Seq[T] = {
    require(k >= 0, "k must be >= 0")
    require(data.length >= k, s"data did not have >= $k distinct elements")
    var s = Set.empty[T]
    var tries = 0
    while (s.size < k  &&  tries <= 2*k) {
      s = s + data(rng.nextInt(data.length))
      tries += 1
    }
    if (s.size < k) {
      // if we are having trouble getting distinct elements, try it the hard way
      val ds = (data.toSet -- s).toSeq
      require((ds.length + s.size) >= k, s"data did not have >= $k distinct elements")
      val kr = k - s.size
      s = s ++ (if (kr <= (ds.length / 2)) {
        sampleDistinct(ds, kr, rng).toSet
      } else {
        ds.toSet -- sampleDistinct(ds, ds.length - kr, rng).toSet
      })
    }
    require(s.size == k, "logic error in sampleDistinct")
    s.toSeq
  }

  def sampleDistinct[T](data: Seq[T], k: Int, seed: Long): Seq[T] = 
    sampleDistinct(data, k, new scala.util.Random(seed))

  def sampleDistinct[T](data: Seq[T], k: Int): Seq[T] =
    sampleDistinct(data, k, scala.util.Random.nextLong())
}
