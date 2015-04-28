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
  private var resampleInterval: Int,
  private var seed: Long) extends Serializable with Logging {

  def this(metric: (T, T) => Double) = this(
    metric,
    KMedoids.default.k,
    KMedoids.default.maxIterations,
    KMedoids.default.epsilon,
    KMedoids.default.fractionEpsilon,
    KMedoids.default.sampleSize,
    KMedoids.default.resampleInterval,
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

  def setResampleInterval(resampleInterval: Int): this.type = {
    require(resampleInterval > 0, s"resampleInterval= $resampleInterval must be > 0")
    this.resampleInterval = resampleInterval
    this
  }

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  def run(data: RDD[T]) = {
    val n = data.count
    require(n >= k, s"data size= $n must be >= k= $k")

    val rng = new scala.util.Random(seed)

    val minDist = (e: T, mv: Seq[T]) => mv.view.map(metric(e, _)).min
    val cost = (mv: Seq[T], data: Seq[T]) => data.view.map(minDist(_, mv)).sum
    val medoidCost = (e: T, data: Seq[T]) => data.view.map(metric(e, _)).sum

    val ss = math.min(sampleSize, n).toInt
    val fraction = math.min(1.0, ss.toDouble / n.toDouble)

    val runStartTime = System.nanoTime
    logInfo(s"KMedoids: collecting initial data sample. Sample fraction= $fraction")
    var sample: Array[T] = data.sample(false, fraction, seed = rng.nextLong).collect

    // initialize medoids to a set of (k) random and unique elements
    logInfo(s"KMedoids: initializing model from $k random elements")
    val distinct = sample.toSeq.distinct
    require(distinct.length >= k, s"data must have at least k= $k distinct values")

    var s = Set.empty[T]
    while (s.size < k) {
      s = s + distinct(rng.nextInt(distinct.length))
    }
    var current = s.toSeq
    var currentCost = cost(current, sample)

    val itrStartTime = System.nanoTime
    val initSeconds = (itrStartTime - runStartTime) / 1e9
    logInfo(f"KMedoids: model initialization completed $initSeconds%.1f sec")

    var itr = 1
    var halt = itr > maxIterations
    while (!halt) {
      val itrTime = System.nanoTime
      val itrSeconds = (itrTime - itrStartTime) / 1e9
      logInfo(f"KMedoids: iteration $itr  cost= $currentCost  elapsed= $itrSeconds%.1f sec")

      if (fraction < 1.0  &&  itr > 1) {
        logInfo(s"KMedoids: updating data sample")
        sample = data.sample(false, fraction, seed = rng.nextLong).collect
      }

      val maxItr = math.min(1 + maxIterations - itr, resampleInterval)
      logInfo(s"KMedoids: refining model (maximum $maxItr iterations)")
      val (next, nextCost, refItr, converged) = 
      KMedoids.refine(
        sample,
        metric,
        current,
        currentCost,
        cost,
        medoidCost,
        maxItr,
        epsilon,
        fractionEpsilon)

      val refSeconds = (System.nanoTime - itrTime) / 1e9
      logInfo(f"KMedoids: refined cost= $nextCost  elapsed= $refSeconds%.1f sec")
      itr += refItr - 1

      // output of refinement guaranteed to be at least as good as input
      current = next
      currentCost = nextCost

      if (converged) {
        logInfo(s"KMedoids: converged at iteration $itr")
        halt = true
      } else if (itr >= maxIterations) {
        logInfo(s"KMedoids: halting at maximum iteration $itr")
        halt = true
      }
    }

    val runTime = System.nanoTime
    val runSeconds = (runTime - runStartTime) / 1e9
    val avgSeconds = (runTime - itrStartTime) / 1e9 / itr
    logInfo(f"KMedoids: finished at $itr iterations with model cost= $currentCost  elapsed= $runSeconds%.1f  sec  avg sec per iteration= $avgSeconds%.1f")
    new KMedoidsModel(current, metric)
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
    def resampleInterval = Int.MaxValue
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
    var halt = itr > maxIterations
    while (!halt) {
      val itrTime = System.nanoTime
      val itrSeconds = (itrTime - itrStartTime) / 1e9
      logInfo(f"KMedoids.refine: iteration $itr  cost= $currentCost  elapsed= $itrSeconds%.1f")

      val next = data.groupBy(medoidIdx(_, current)).toVector.sortBy(_._1).map(_._2).map(medoid)
      val nextCost = cost(next, data)

      val curSeconds = (System.nanoTime - itrTime) / 1e9
      logInfo(f"KMedoids.refine: iteration elapsed= $curSeconds%.1f")

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
}
