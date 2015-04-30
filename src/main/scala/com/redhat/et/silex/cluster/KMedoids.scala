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
    logInfo(s"KMedoids: collecting data sample")
    val sample = KMedoids.sampleBySize(data, sampleSize, rng.nextLong)
    logInfo(s"KMedoids: sample size= ${sample.length}")
    val model = doRun(sample, rng)
    val runSeconds = (System.nanoTime - runStartTime) / 1e9
    logInfo(f"KMedoids: total clustering time= $runSeconds%.1f sec")
    model
  }

  def run(data: Seq[T]) = {
    val runStartTime = System.nanoTime
    val rng = new scala.util.Random(seed)
    logInfo(s"KMedoids: collecting data sample")
    val sample = KMedoids.sampleBySize(data, sampleSize, rng.nextLong)
    logInfo(s"KMedoids: sample size= ${sample.length}")
    val model = doRun(sample, rng)
    val runSeconds = (System.nanoTime - runStartTime) / 1e9
    logInfo(f"KMedoids: total clustering time= $runSeconds%.1f sec")
    model
  }

  private def doRun(data: Seq[T], rng: scala.util.Random) = {
    val minDist = (e: T, mv: Seq[T]) => mv.iterator.map(metric(e, _)).min
    val cost = (mv: Seq[T], data: Seq[T]) => data.iterator.map(minDist(_, mv)).sum
    val medoidCost = (e: T, data: Seq[T]) => data.iterator.map(metric(e, _)).sum

    val startTime = System.nanoTime
    logInfo(s"KMedoids: initializing model from $k random elements")
    var current = KMedoids.sampleDistinct(data, k, rng)
    var currentCost = cost(current, data)

    val itrStartTime = System.nanoTime
    val initSeconds = (itrStartTime - startTime) / 1e9
    logInfo(f"KMedoids: model initialization completed $initSeconds%.1f sec")

    logInfo(s"KMedoids: refining model")
    val (refined, refinedCost, itr, converged) = 
    KMedoids.refine(
      data,
      metric,
      current,
      currentCost,
      cost,
      medoidCost,
      maxIterations,
      epsilon,
      fractionEpsilon)

    val avgSeconds = (System.nanoTime - itrStartTime) / 1e9 / itr
    logInfo(f"KMedoids: finished at $itr iterations with model cost= $currentCost   avg sec per iteration= $avgSeconds%.1f")
    new KMedoidsModel(refined, metric)
  }
}

object KMedoids extends Logging {
  def wrt[T](rdd: RDD[T])(metric: (T, T) => Double) = new KMedoids(metric)

  private[cluster] object default {
    def k = 2
    def maxIterations = 10
    def epsilon = 0.0
    def fractionEpsilon = 0.01
    def sampleSize = 1000
    def seed = scala.util.Random.nextLong()
  }

  private[cluster] def refine[T](
    data: Seq[T],
    metric: (T, T) => Double,
    initial: Seq[T],
    initialCost: Double,
    cost: (Seq[T], Seq[T]) => Double,
    medoidCost: (T, Seq[T]) => Double,
    maxIterations: Int,
    epsilon: Double,
    fractionEpsilon: Double): (Seq[T], Double, Int, Boolean) = {

    require(maxIterations > 0, s"maxIterations= $maxIterations must be > 0")
    require(epsilon >= 0.0, s"epsilon= $epsilon must be >= 0.0")
    require(fractionEpsilon >= 0.0, s"fractionEpsilon= $fractionEpsilon must be >= 0.0")

    val runStartTime = System.nanoTime

    val k = initial.length
    val medoidIdx = (e: T, mv: Seq[T]) => mv.iterator.map(metric(e, _)).zipWithIndex.min._2
    val medoid = (data: Seq[T]) => data.iterator.minBy(medoidCost(_, data))

    var current = initial
    var currentCost = initialCost
    var converged = false

    val itrStartTime = System.nanoTime

    var itr = 1
    var halt = false
    while (!halt) {
      val itrTime = System.nanoTime
      val itrSeconds = (itrTime - itrStartTime) / 1e9
      logInfo(f"KMedoids.refine: iteration $itr  cost= $currentCost  elapsed= $itrSeconds%.1f")

      val next = data.groupBy(medoidIdx(_, current)).toVector.sortBy(_._1).map(_._2).map(medoid)
      val nextCost = cost(next, data)

      val curSeconds = (System.nanoTime - itrTime) / 1e9
      logInfo(f"KMedoids.refine: updated cost= $nextCost  elapsed= $curSeconds%.1f sec")

      val delta = currentCost - nextCost
      val fractionDelta = if (currentCost > 0.0) delta / currentCost else 0.0

      if (delta <= epsilon) {
        logInfo(s"KMedoids.refine: converged with delta= $delta")
        halt = true
        converged = true
      } else if (fractionDelta <= fractionEpsilon) {
        logInfo(s"KMedoids.refine: converged with fractionDelta= $fractionDelta")
        halt = true
        converged = true
      } else if (itr >= maxIterations) {
        logInfo(s"KMedoids.refine: halting at maximum iteration $itr")
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
    logInfo(f"KMedoids.refine: finished at iteration $itr  cost= $currentCost  elapsed= $runSeconds%.1f")
    (current, currentCost, itr, converged)
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
      data.sample(false, fraction, seed = seed).take(sampleSize).toSeq
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
      data.filter(x => rng.nextDouble() < fraction).take(sampleSize)
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
