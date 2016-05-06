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

import org.apache.spark.rdd.RDD

/** Represents a K-Medoids clustering model
  *
  * @param medoids The collection of cluster medoids that embodies the model
  * @param metric The metric function over data elements asumed by the model
  */
class KMedoidsModel[T](
  val medoids: Seq[T],
  val metric: (T, T) => Double) extends Serializable {

  /** The model prediction function: maps an element to the index of the closest medoid */
  @transient lazy val predictor = KMedoidsModel.predictor(medoids, metric)

  /** The model distance function: maps an element to its distance to the closest medoid */
  @transient lazy val distance = KMedoidsModel.distance(medoids, metric)

  /** Returns index of closest medoid, paired with its distance to that medoid */
  @transient lazy val predictorWithDistance = KMedoidsModel.predictorWithDistance(medoids, metric)

  /** The number of medoids in the model */
  def k = medoids.length

  /** Return the index of the medoid closest to the input
    *
    * @param point An element of the data space
    * @return The index of the medoid closest to the input
    */
  def predict(point: T): Int = predictor(point)

  /** Return an RDD produced by predicting the closest medoid to each row
    *
    * @param points An RDD whose rows are elements of the data space
    * @return An RDD whose rows are the corresponding indices of the closest medoids
    */
  def predict(points: RDD[T]): RDD[Int] = points.map(predictor)

  /** Extracts a data object and a tag value from another data structure, and returns the
    * index of closest cluster, paired with the tag value
    * @param obj An object containing a data point and an associated tag value
    * @param f Function to extract data point and the tag value from 'obj'
    * @return A pair value (j, v) where (j) is index of closest cluster and (v) is the associated
    * tag value
    */
  def predictBy[O, V](obj: O)(f: O => (T, V)) = {
    val (t, v) = f(obj)
    val j = predictor(t)
    (j, v)
  }

  /** Returns the index of closest cluster, paired with corresponding distance
    * @param point A data object
    * @return Pair (j, d) with (j) the closest cluster index and (d) the corresponding distance
    */
  def predictWithDistance(point: T) = predictorWithDistance(point)

  /** Extracts a data object and a tag value from another data structure, and returns the
    * index of closest cluster, with the corresponding distance and associated tag value
    * @param obj An object containing a data point and an associated tag value
    * @param f Function to extract data point and tag value from 'obj'
    * @return A tuple (j, d, v) where (j) is index of closest cluster, (d) is corresponding
    * distance, and (v) is the associated tag value
    */
  def predictWithDistanceBy[O, V](obj: O)(f: O => (T, V)) = {
    val (t, v) = f(obj)
    val (j, d) = predictorWithDistance(t)
    (j, d, v)
  }

  /** Return the model cost with respect to the given data
    *
    * Model cost is defined as the sum of closest-distances over the data elements
    *
    * @param data The input data to compute the cost over
    * @param normalized If true, compute cost normalized by number of data elements.
    * Defaults to false.
    * @return The sum of closest-distances over the data elements
    */
  def cost(data: RDD[T], normalized: Boolean = false) = {
    if (normalized) {
      val n = data.count
      if (n > 0) data.map(distance).sum() / n.toDouble else 0.0
    } else {
      data.map(distance).sum()
    }
  }

  /** Return the model cost with respect to the given data
    *
    * Model cost is defined as the sum of closest-distances over the data elements
    *
    * @param data The input data to compute the cost over
    * @param normalized If true, compute cost normalized by number of data elements.
    * Defaults to false.
    * @return The sum of closest-distances over the data elements
    */
  def computeCost(data: RDD[T], normalized: Boolean = false) = cost(data, normalized)
}

/** Utility functions for KMedoidsModel */
object KMedoidsModel {

  /** Return a predictor function with respect to a collection of medoids and a metric
    *
    * @param medoids A collection of elements representing clustering medoids
    * @param metric The distance metric over the element space
    * @return A function that maps an element to the index of the nearest medoid
    */
  def predictor[T](medoids: Seq[T], metric: (T, T) => Double) = {
    val med = medoids.toVector
    val n = med.length
    (point: T) => {
      var mMin = Double.MaxValue  // distance from point to closest cluster medoid
      var jMin = -1               // index of closest medoid
      var j = 0
      while (j < n) {
        val m = metric(point, med(j))
        if (m < mMin) {
          mMin = m
          jMin = j
        }
        j += 1
      }
      jMin
    }
  }

  /** Return a function that yields index of closest medoid, paired with distance to that medoid
    *
    * @param medoids A collection of elements representing clustering medoids
    * @param metric The distance metric over the element space
    * @return A function that maps an element to (index, distance)
    */
  def predictorWithDistance[T](medoids: Seq[T], metric: (T, T) => Double) = {
    val med = medoids.toVector
    val n = med.length
    (point: T) => {
      var mMin = Double.MaxValue  // distance from point to closest cluster medoid
      var jMin = -1               // index of closest medoid
      var j = 0
      while (j < n) {
        val m = metric(point, med(j))
        if (m < mMin) {
          mMin = m
          jMin = j
        }
        j += 1
      }
      (jMin, mMin)
    }
  }

  /** Return a distance function with respect to a collection of medoids and a metric
    *
    * @param medoids A collection of elements representing clustering medoids
    * @param metric The distance metric over the element space
    * @return A function that maps an element to its distance to the closest medoid
    */
  def distance[T](medoids: Seq[T], metric: (T, T) => Double) = {
    val med = medoids.toVector
    val n = med.length
    (point: T) => {
      var mMin = Double.MaxValue  // distance from point to closest cluster medoids
      var j = 0
      while (j < n) {
        val m = metric(point, med(j))
        if (m < mMin) { mMin = m }
        j += 1
      }
      mMin
    }
  }
}
