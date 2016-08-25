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
 * limitations under the License.
 */

package com.redhat.et.silex.cluster

import org.apache.spark.rdd.RDD
import com.redhat.et.silex.util.Logging
import org.apache.spark.mllib.tree.RandomForest

import com.redhat.et.silex.feature.extractor.Extractor

import com.redhat.et.silex.sample.iid.implicits._
import com.redhat.et.silex.util.vectors.implicits._
import ClusteringRandomForestModel._

/**
 * An object for training a Random Forest clustering model on unsupervised data.
 *
 * Data is required to have a mapping into a feature space of type Seq[Double].
 *
 * @param extractor A feature extraction function for data objects
 * @param categoryInfo A map from feature indexes into numbers of categories.  Feature indexes
 * that do not have an entry in the map are assumed to be numeric, not categorical.  Defaults to
 * category-info from [[Extractor]], if the feature extraction function is of this type.  Otherwise
 * defaults to empty, i.e. all numeric features.
 * @param syntheticSS The size of synthetic (margin-sampled) data to be constructed.  Defaults to
 * the size of the input data.
 * @param rfNumTrees The number of decision trees to train in the Random Forest  Defaults to 10.
 * @param rfMaxDepth Maximum decision tree depth.  Defaults to 5.
 * @param rfMaxBins Maximum histogramming bins to use for numeric data.  Defaults to 5.
 * @param clusterK The number of clusters to use when clustering leaf-id vectors.  Defaults to
 * an automatic estimation of a "good" number of clusters.
 * @param clusterMaxIter Maximum clustering refinement iterations to compute.  Defaults to 25.
 * @param clusterEps Halt clustering if clustering metric-cost changes by less than this value.
 * Defaults to 0
 * @param clusterFractionEps Halt clustering if clustering metric-cost changes by this fractional
 * value from previous iteration.  Defaults to 0.0001
 * @param clusterSS If data is larger, use this random sample size.  Defaults to 1000.
 * @param clusterThreads Use this number of threads to accelerate clustering.  Defaults to 1.
 * @param seed A seed to use for RNG.  Defaults to using a randomized seed value.
 */
case class RandomForestCluster[T](
  extractor: T => Seq[Double],
  categoryInfo: Map[Int, Int],
  syntheticSS: Int,
  rfNumTrees: Int,
  rfMaxDepth: Int,
  rfMaxBins: Int,
  clusterK: Int,
  clusterMaxIter: Int,
  clusterEps: Double,
  clusterFractionEps: Double,
  clusterSS: Int,
  clusterThreads: Int,
  seed: Long) extends Serializable with Logging {

  require(categoryInfo.valuesIterator.forall(_ > 0), "category counts must be > 0")
  require(syntheticSS >= 0, "syntheticSS must be >= 0")
  require(rfNumTrees > 0, "rfNumTrees must be > 0")
  require(rfMaxDepth > 0, "rfMaxDepth must be > 0")
  require(rfMaxBins > 1, "rfMaxBins must be > 1")
  // clustering parameters checked by KMedoids class

  /** Set a new feature extraction function for input objects
    * @param extractorNew The feature extraction function
    * @return Copy of this instance with new extractor
    */
  def setExtractor(extractorNew: T => Seq[Double]) = this.copy(extractor = extractorNew)

  /** Set a new category info map
    * @param categoryInfoNew New category-info map to use
    * @return Copy of this instance with new category info
    */
  def setCategoryInfo(categoryInfoNew: Map[Int, Int]) = this.copy(categoryInfo = categoryInfoNew)

  /** Set a new synthetic data sample size
    * @param syntheticSSNew New synthetic data size to use
    * @return Copy of this instance with new synthetic data size
    */
  def setSyntheticSS(syntheticSSNew: Int) = this.copy(syntheticSS = syntheticSSNew)

  /** Set a new number of Random Forest trees to train for the model
    * @param rfNumTreesNew New number of trees to use for the RF
    * @return Copy of this instance with new Random Forest size
    */
  def setRfNumTrees(rfNumTreesNew: Int) = this.copy(rfNumTrees = rfNumTreesNew)

  /** Set a new Random Forest maximum tree depth
    * @param rfMaxDepthNew New maximum decision tree depth
    * @return Copy of this instance with new maximum decision tree depth
    */
  def setRfMaxDepth(rfMaxDepthNew: Int) = this.copy(rfMaxDepth = rfMaxDepthNew)

  /** Set a new Random Forest maximum numeric binning value
    * @param rfMaxBinsNew New maximum numeric binning value
    * @return Copy of this instance with new maximum binning value
    */
  def setRfMaxBins(rfMaxBinsNew: Int) = this.copy(rfMaxBins = rfMaxBinsNew)

  /** Set a new target cluster size
    * @param clusterKNew New target cluster number.  Zero sets to automatic determination.
    * @return Copy of this instance with new target cluster size
    */
  def setClusterK(clusterKNew: Int) = this.copy(clusterK = clusterKNew)

  /** Set a new maximum clustering refinement iteration
    * @param clusterMaxIterNew New maximum number of refinement iterations
    * @return Copy of this instance with new maximum iteration
    */
  def setClusterMaxIter(clusterMaxIterNew: Int) = this.copy(clusterMaxIter = clusterMaxIterNew)

  /** Set a new clustering epsilon halting threshold
    * @param clusterEpsNew New epsilon halting threshold
    * @return Copy of this instance with new clustering epsilon threshold
    */
  def setClusterEps(clusterEpsNew: Double) = this.copy(clusterEps = clusterEpsNew)

  /** Set a new clustering fractional epsilon halting threshold
    * @param clusterFractionEpsNew New fractional epsilon value
    * @return Copy of this instance with new fractional epsilon threshold
    */
  def setClusterFractionEps(clusterFractionEpsNew: Double) =
    this.copy(clusterFractionEps = clusterFractionEpsNew)

  /** Set a new clustering sample size
    * @param clusterSSNew New clustering sample size
    * @return Copy of this instance with new sample size
    */
  def setClusterSS(clusterSSNew: Int) = this.copy(clusterSS = clusterSSNew)

  /** Set a new clustering number of threads
    * @param clusterThreadsNew New number of process threads to use
    * @return Copy of this instance with new threading number
    */
  def setClusterThreads(clusterThreadsNew: Int) = this.copy(clusterThreads = clusterThreadsNew)

  /** Set a new RNG seed
    * @param seedNew New RNG seed to use
    * @return Copy of this instance with new RNG seed
    */
  def setSeed(seedNew: Long) = this.copy(seed = seedNew)

  /** Train a Random Forest clustering model from input data
    * @param data The input data objects to cluster
    * @return An RF clustering model of the input data
    */
  def run(data: RDD[T]) = {

    scala.util.Random.setSeed(seed)

    val spark = data.sparkContext

    logInfo(s"Extracting feature data...")
    val fvData = data.map(extractor)

    logInfo(s"Assembling synthetic data from marginal distributions...")
    val sss = if (syntheticSS > 0) syntheticSS else fvData.count.toInt
    val synData = fvData.iidFeatureSeqRDD(sss)

    logInfo(s"Assembling RF model training set...")
    val fvVec = fvData.map(_.toSpark)
    val synVec = synData.map(_.toSpark)
    val trainVec = new org.apache.spark.rdd.UnionRDD(spark, List(fvVec, synVec))
    val trainLab = new org.apache.spark.rdd.UnionRDD(spark,
      List(fvVec.map(_.toLabeledPoint(1.0)), synVec.map(_.toLabeledPoint(0.0))))

    logInfo(s"Training RF model...")
    val rfModel = RandomForest.trainClassifier(
      trainLab,
      2,
      categoryInfo,
      rfNumTrees,
      "auto",
      "gini",
      rfMaxDepth,
      rfMaxBins,
      scala.util.Random.nextInt)

    val kMedoids = KMedoids(RandomForestCluster.leafIdDist)
      .setK(clusterK)
      .setMaxIterations(clusterMaxIter)
      .setEpsilon(clusterEps)
      .setFractionEpsilon(clusterFractionEps)
      .setSampleSize(clusterSS)
      .setNumThreads(clusterThreads)
      .setSeed(scala.util.Random.nextInt)

    logInfo(s"Clustering leaf id vectors...")
    val clusterModel = kMedoids.run(fvVec.map(rfModel.predictLeafIds))

    logInfo(s"Completed RF clustering model")
    new RandomForestClusterModel(
      extractor,
      rfModel,
      clusterModel)
  }
}

/** Factory functions and implicits for RandomForestCluster */
object RandomForestCluster {
  private [cluster] def leafIdDist(a: Vector[Int], b: Vector[Int]): Double = {
    var d = 0
    var j = 0
    val n = a.length
    while (j < n) {
      if (a(j) != b(j)) d += 1
      j += 1
    }
    d.toDouble
  }

  private [cluster] object default {
    def categoryInfo[T](extractor: T => Seq[Double]) = extractor match {
      case e: Extractor[_] => e.categoryInfo.toMap
      case _ => Map.empty[Int, Int]
    }
    def syntheticSS = 0
    def rfNumTrees = 10
    def rfMaxDepth = 5
    def rfMaxBins = 5
    def clusterK = 0
    def clusterMaxIter = KMedoids.default.maxIterations
    def clusterEps = KMedoids.default.epsilon
    def clusterFractionEps = KMedoids.default.fractionEpsilon
    def clusterSS = KMedoids.default.sampleSize
    def clusterThreads = KMedoids.default.numThreads
    def seed = KMedoids.default.seed
  }

  /** Generate a RandomForestCluster object from a feature extraction function, with all other
    * parameters taking on default values.
    * @param extractor The feature extraction function to use on input objects
    * @return New RF clustering object with defaulted parameters
    */
  def apply[T](extractor: T => Seq[Double]): RandomForestCluster[T] = RandomForestCluster(
    extractor,
    default.categoryInfo(extractor),
    default.syntheticSS,
    default.rfNumTrees,
    default.rfMaxDepth,
    default.rfMaxBins,
    default.clusterK,
    default.clusterMaxIter,
    default.clusterEps,
    default.clusterFractionEps,
    default.clusterSS,
    default.clusterThreads,
    default.seed)
}
