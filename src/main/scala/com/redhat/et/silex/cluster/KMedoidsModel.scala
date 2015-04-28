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

import org.apache.spark.rdd.RDD

class KMedoidsModel[T](
  val medoids: Seq[T],
  val metric: (T, T) => Double) extends Serializable {

  @transient val predictor = KMedoidsModel.predictor(medoids, metric)
  @transient val distance = KMedoidsModel.distance(medoids, metric)

  def k = medoids.length

  def predict(point: T): Int = predictor(point)

  def predict(points: RDD[T]): RDD[Int] = points.map(predictor)

  def cost(data: RDD[T]) = data.map(distance).sum()

  def computeCost(data: RDD[T]) = cost(data)
}

object KMedoidsModel {
  def predictor[T](medoids: Seq[T], metric: (T, T) => Double) =
    (point: T) => medoids.iterator.map(e => metric(e, point)).zipWithIndex.minBy(_._1)._2

  def distance[T](medoids: Seq[T], metric: (T, T) => Double) =
    (point: T) => medoids.iterator.map(e => metric(e, point)).min
}
