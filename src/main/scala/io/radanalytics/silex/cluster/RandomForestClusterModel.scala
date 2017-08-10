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

package io.radanalytics.silex.cluster

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.{
  Vector => SparkVec,
  DenseVector => DenseSparkVec
}

import io.radanalytics.silex.util.vectors.implicits._
import ClusteringRandomForestModel._

/** Represents a Random Forest clustering model of some data objects
  * @param extractor A feature extraction function defined on data objects
  * @param randomForestModel RF model component
  * @param kMedoidsModel K-medoids clustering model component
  */
class RandomForestClusterModel[T](
  val extractor: T => Seq[Double],
  val randomForestModel: RandomForestModel,
  val kMedoidsModel: KMedoidsModel[Vector[Int]]) extends Serializable {

  /** Return the index of nearest cluster to the input point
    * @param point A data object
    * @return The index of the cluster closest to this point
    */
  def predict(point: T) = predictFromFv(extractor(point))

  /** Return the index of nearest cluster to the feature vector associated with some input object
    * @param fv A feature vector associated with some input data object.  Assumed to have been
    * obtained from the model's feature extractor.
    * @return The index of cluster closest to a corresponding data object
    */
  def predictFromFv(fv: Seq[Double]) =
    kMedoidsModel.predict(randomForestModel.predictLeafIds(fv.toSpark))

  /** Extracts a data object and a tag value from another data structure, and returns the
    * index of closest cluster, paired with the tag value
    * @param obj An object containing a data point and an associated tag value
    * @param f Function to extract data point and the tag value from 'obj'
    * @return A pair value (j, v) where (j) is index of closest cluster and (v) is the associated
    * tag value
    */
  def predictBy[O, V](obj: O)(f: O => (T, V)) = {
    val (t, v) = f(obj)
    val j = predict(t)
    (j, v)
  }

  /** Returns the index of closest cluster, paired with corresponding distance
    * @param point A data object
    * @return Pair (j, d) with (j) the closest cluster index and (d) the corresponding distance
    */
  def predictWithDistance(point: T) = predictWithDistanceFromFv(extractor(point))

  /** Returns the index of closest cluster for a data object associated with a given feature
    * vector, paired with the corresponding distance.
    * @param fv A feature vector associated with some input data object.  Assumed to have been
    * obtained from the model's feature extractor.
    * @return The index of cluster closest to the associated data object, paired with the
    * corresponding distance
    */
  def predictWithDistanceFromFv(fv: Seq[Double]) =
    kMedoidsModel.predictWithDistance(randomForestModel.predictLeafIds(fv.toSpark))

  /** Extracts a data object and a tag value from another data structure, and returns the
    * index of closest cluster, with the corresponding distance and associated tag value
    * @param obj An object containing a data point and an associated tag value
    * @param f Function to extract data point and tag value from 'obj'
    * @return A tuple (j, d, v) where (j) is index of closest cluster, (d) is corresponding
    * distance, and (v) is the associated tag value
    */
    def predictWithDistanceBy[O, V](obj: O)(f: O => (T, V)) = {
    val (t, v) = f(obj)
    val (j, d) = predictWithDistance(t)
    (j, d, v)
  }

  /** Extracts a feature vector and a tag value from another data structure, and returns the
    * index of closest cluster, paired with the corresponding distance.
    * @param obj An object containing a feature vector and an associated tag value
    * @param f Function to extract feature vector and tag value from 'obj'
    * @return A pair (j, v) where (j) is index of closest cluster and (v) is the associated
    * tag value
    */
  def predictFromFvBy[O, V](obj: O)(f: O => (Seq[Double], V)) = {
    val (fv, v) = f(obj)
    val j = predictFromFv(fv)
    (j, v)
  }

  /** Extracts a feature vector and a tag value from another data structure, and returns the
    * index of closest cluster, with the corresponding distance and associated tag value
    * @param obj An object containing a data point and an associated tag value
    * @param f Function to extract the feature vector and tag value from 'obj'
    * @return A tuple (j, d, v) where (j) is index of closest cluster, (d) is corresponding
    * distance, and (v) is the associated tag value
    */
  def predictWithDistanceFromFvBy[O, V](obj: O)(f: O => (Seq[Double], V)) = {
    val (fv, v) = f(obj)
    val (j, d) = predictWithDistanceFromFv(fv)
    (j, d, v)
  }
}

/** Factory functions and implicits for RandomForestClusterModel */
object RandomForestClusterModel {
}
