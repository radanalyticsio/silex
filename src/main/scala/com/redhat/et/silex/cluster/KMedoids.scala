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

package com.redhat.et.silex.cluster

import scala.util.Random

import scala.concurrent.forkjoin.ForkJoinPool
import scala.collection.parallel.ForkJoinTaskSupport

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

/** An object for training a K-Medoid clustering model on Seq or RDD data.
  *
  * Data is required to have a metric function defined on it, but it does not require an algebra
  * over data elements, as K-Means clustering does.
  *
  * @param metric The distance metric imposed on data elements
  * @param k The number of clusters to use
  * @param maxIterations The maximum number of model refinement iterations to run
  * @param epsilon The epsilon threshold to use.  Must be >= 0.
  *
  * If c1 is the current clustering model cost, and c0 is the cost of the previous model,
  * then refinement halts when (c0 - c1) <= epsilon (Lower cost is better).
  * @param fractionEpsilon The fractionEpsilon threshold to use.  Must be >= 0.
  *
  * If c1 is the current clustering model cost, and c0 is the cost of the previous model,
  * then refinement halts when (c0 - c1) / c0 <= fractionEpsilon (Lower cost is better).
  * @param sampleSize The target size of the random sample.  Must be > 0.
  * @param numThreads The number of threads to use while clustering
  * @param seed The random seed to use for RNG.
  *
  * Cluster training runs with the same starting random seed will be the same.  By default,
  * training runs will vary randomly.
  */
case class KMedoids[T](
  metric: (T, T) => Double,
  k: Int,
  maxIterations: Int,
  epsilon: Double,
  fractionEpsilon: Double,
  sampleSize: Int,
  numThreads: Int,
  seed: Long
  ) extends Serializable with Logging {

  require(k > 0, s"k= ${k} must be > 0")
  require(maxIterations > 0, s"maxIterations= ${maxIterations} must be > 0")
  require(epsilon >= 0.0, s"epsilon= ${epsilon} must be >= 0.0")
  require(fractionEpsilon >= 0.0, s"fractionEpsilon= ${fractionEpsilon} must be >= 0.0")
  require(sampleSize > 0, s"sampleSize= ${sampleSize} must be > 0")
  require(numThreads > 0, s"numThreads= $numThreads must be > 0")

  /** Set the distance metric to use over data elements
    *
    * @param metric_ The distance metric
    * @return Copy of this instance with new metric
    */
  def setMetric(metric_ : (T, T) => Double) = this.copy(metric = metric_)

  /** Set the number of clusters to train
    *
    * @param k_ The number of clusters.  Must be > 0.
    * @return Copy of this instance with new value for k
    */
  def setK(k_ : Int) = this.copy(k = k_)

  /** Set the maximum number of iterations to allow before halting cluster refinement.
    *
    * @param maxIterations_ The maximum number of refinement iterations.  Must be > 0.
    * @return Copy of this instance, with updated value for maxIterations
    */
  def setMaxIterations(maxIterations_ : Int) = this.copy(maxIterations = maxIterations_)

  /** Set epsilon halting threshold for clustering cost improvement between refinements.
    *
    * If c1 is the current clustering model cost, and c0 is the cost of the previous model,
    * then refinement halts when (c0 - c1) <= epsilon (Lower cost is better).
    *
    * @param epsilon_ The epsilon threshold to use.  Must be >= 0.
    * @return Copy of this instance, with updated value of epsilon
    */
  def setEpsilon(epsilon_ : Double) = this.copy(epsilon = epsilon_)

  /** Set fractionEpsilon threshold for clustering cost improvement between refinements.
    *
    * If c1 is the current clustering model cost, and c0 is the cost of the previous model,
    * then refinement halts when (c0 - c1) / c0 <= fractionEpsilon (Lower cost is better).
    * @param fractionEpsilon_ The fractionEpsilon threshold to use.  Must be >= 0.
    * @return Copy of this instance, with updated fractionEpsilon setting
    */
  def setFractionEpsilon(fractionEpsilon_ : Double) = this.copy(fractionEpsilon = fractionEpsilon_)

  /** Set the size of the random sample to take from input data to use for clustering.
    *
    * @param sampleSize_ The target size of the random sample.  Must be > 0.
    * @return Copy of this instance, with updated value of sampleSize
    */
  def setSampleSize(sampleSize_ : Int) = this.copy(sampleSize = sampleSize_)

  /** Set the number of threads to use for clustering runs
    *
    * @param numThreads_ The number of threads to use while clustering.  Must be > 0.
    * @return Copy of this instance with updated value of numThreads
    */
  def setNumThreads(numThreads_ : Int) = this.copy(numThreads = numThreads_)

  /** Set the random number generation (RNG) seed.
    *
    * Cluster training runs with the same starting random seed will be the same.  By default,
    * training runs will vary randomly.
    *
    * @param seed_ The random seed to use for RNG
    * @return Copy of this instance, with updated random seed
    */
  def setSeed(seed_ : Long) = this.copy(seed = seed_)

  /** Obtain the minimum distance from element (e) to a collection of cluster medoids (mv)
    * @param e An element of the data space
    * @param mv A collection of cluster medoids
    * @return The minimum distance from (e) to one of the medoids in (mv)
    */
  private def medoidDist(e: T, mv: Vector[T]) = {
    val n = mv.length            // number of cluster medoids
    var mMin = Double.MaxValue   // maintains minimum distance
    var j = 0
    while (j < n) {
      val m = metric(e, mv(j))
      if (m < mMin) { mMin = m }
      j += 1
    }
    mMin
  }

  /** Obtain the index of a cluster medoid that is closest to element (e)
    * @param e An element of the data space
    * @param mv A collection of cluster medoids
    * @return The index of medoid in (mv) with least distance to (e)
    */
  private def medoidIdx(e: T, mv: Vector[T]) = {
    val n = mv.length            // number of cluster medoids
    var mMin = Double.MaxValue   // maintain minimum distance
    var jMin = 0                 // index of medoid with minimum distance
    var j = 0
    while (j < n) {
      val m = metric(e, mv(j))
      if (m < mMin) {
        mMin = m
        jMin = j
      }
      j += 1
    }
    jMin
  }

  private def medoidCost(e: T, data: Seq[T]) = data.iterator.map(metric(e, _)).sum
  private def modelCost(mv: Vector[T], data: Seq[T]) =
    data.iterator.map(medoidDist(_, mv)).sum / data.length.toDouble

  private def medoid(data: Seq[T], threadPool: ForkJoinPool) = {
    val pardata = data.par
    pardata.tasksupport = new ForkJoinTaskSupport(threadPool)
    pardata.minBy(medoidCost(_, data))
  }

  /** Perform a K-Medoid clustering model training run on some input data
    *
    * @param data The input data to train the clustering model on.
    * @return A [[KMedoidsModel]] object representing the clustering model.
    */
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

  /** Perform a K-Medoid clustering model training run on some input data
    *
    * @param data The input data to train the clustering model on.
    * @return A [[KMedoidsModel]] object representing the clustering model.
    */
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
    val threadPool = new ForkJoinPool(numThreads)
    logInfo(s"initializing model from $k random elements")
    var current = KMedoids.sampleDistinct(data, k, rng).toVector
    var currentCost = modelCost(current, data)

    val itrStartTime = System.nanoTime
    val initSeconds = (itrStartTime - startTime) / 1e9
    logInfo(f"model initialization completed $initSeconds%.1f sec")

    logInfo(s"refining model")
    val (refined, refinedCost, itr, converged) = refine(data, current, currentCost, threadPool)

    val avgSeconds = (System.nanoTime - itrStartTime) / 1e9 / itr
    logInfo(f"finished at $itr iterations with model cost= $refinedCost%.6g   avg sec per iteration= $avgSeconds%.1f")
    new KMedoidsModel(refined, metric)
  }

  private def refine(
    data: Seq[T],
    initial: Vector[T],
    initialCost: Double,
    threadPool: ForkJoinPool): (Vector[T], Double, Int, Boolean) = {

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

      val clusters = data.groupBy(medoidIdx(_, current)).toVector.sortBy(_._1).map(_._2).par
      clusters.tasksupport = new ForkJoinTaskSupport(threadPool)
      val next = clusters.map(medoid(_, threadPool)).toVector
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

/** Utilities used by K-Medoids clustering */
object KMedoids extends Logging {

  /** Default values for KMedoids class paramers.
    * @note If you alter these, make sure you update documentation, update the corresponding
    * doc for the [[apply]] method below
    */
  private[cluster] object default {
    def k = 2
    def maxIterations = 25
    def epsilon = 0.0
    def fractionEpsilon = 0.0001
    def sampleSize = 1000
    def numThreads = 1
    def seed = scala.util.Random.nextLong()
  }

  /** Return a KMedoids object with the given metric function, and other parameters defaulted.
    *
    * Defaults are as follows:
    *
    * k = 2
    *
    * maxIterations = 25
    *
    * epsilon = 0.0
    *
    * fractionEpsilon = 0.0001
    *
    * sampleSize = 1000
    *
    * numThreads = 1
    *
    * seed = randomly initialized seed value
    *
    * @param metric The metric function to impose on elements of the data space
    */
  def apply[T](metric : (T, T) => Double): KMedoids[T] =
    KMedoids(
      metric,
      default.k,
      default.maxIterations,
      default.epsilon,
      default.fractionEpsilon,
      default.sampleSize,
      default.numThreads,
      default.seed)

  /** Return the random sampling fraction corresponding to a desired number of samples
    *
    * @param n The size of data being sampled from
    * @param sampleSize The desired sample size
    * @return A sampling fraction, >= 0.0 and <= 1.0 that will yield the desired sample size
    * @note When used with typical Bernoulli sampling the returned samping fraction will yield
    * a sample size that varies randomly, with a mean of 'sampleSize'
    */
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

  /** Return a random sample of data having an expected sample size of the requested amount.
    *
    * @param data The input data to sample
    * @param sampleSize The desired sample size.
    * @param seed Seed for RNG
    * @return A sample whose expected mean size is sampleSize.
    */
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

  /** Return a random sample of data having an expected sample size of the requested amount.
    *
    * @param data The input data to sample
    * @param sampleSize The desired sample size.
    * @return A sample whose expected mean size is sampleSize.
    */
  def sampleBySize[T](data: RDD[T], sampleSize: Int): Seq[T] =
    sampleBySize(data, sampleSize, scala.util.Random.nextLong())

  /** Return a random sample of data having an expected sample size of the requested amount.
    *
    * @param data The input data to sample
    * @param sampleSize The desired sample size.
    * @param seed Seed for RNG
    * @return A sample whose expected mean size is sampleSize.
    */
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

  /** Return a random sample of data having an expected sample size of the requested amount.
    *
    * @param data The input data to sample
    * @param sampleSize The desired sample size.
    * @return A sample whose expected mean size is sampleSize.
    */
  def sampleBySize[T](data: Seq[T], sampleSize: Int): Seq[T] =
    sampleBySize(data, sampleSize, scala.util.Random.nextLong())

  /** Return a given number of distinct elements randomly selected from data
    *
    * @param data The data to sample from
    * @param k The number of distinct samples to return.
    * @param rng Random number generator to use when sampling
    * @return A collection of k distinct elements randomly selected from the data
    * @note If the number of distinct elements in the data is < k, an exception will be thrown
    */
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
    s.toVector
  }

  /** Return a given number of distinct elements randomly selected from data
    *
    * @param data The data to sample from
    * @param k The number of distinct samples to return
    * @param seed A seed to use for RNG when sampling
    * @return A collection of k distinct elements randomly selected from the data
    * @note If the number of distinct elements in the data is < k, an exception will be thrown
    */
  def sampleDistinct[T](data: Seq[T], k: Int, seed: Long): Seq[T] = 
    sampleDistinct(data, k, new scala.util.Random(seed))

  /** Return a given number of distinct elements randomly selected from data
    *
    * @param data The data to sample from
    * @param k The number of distinct samples to return
    * @return A collection of k distinct elements randomly selected from the data
    * @note If the number of distinct elements in the data is < k, an exception will be thrown
    */
  def sampleDistinct[T](data: Seq[T], k: Int): Seq[T] =
    sampleDistinct(data, k, scala.util.Random.nextLong())
}
