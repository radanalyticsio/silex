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

package com.redhat.et.silex.util

object MedoidClustering {
  import scala.language.implicitConversions
  import scala.reflect.ClassTag
  import scala.util.Random
  import scala.collection.mutable
  import scala.collection.mutable.ArrayBuffer

  import org.apache.spark.rdd.RDD

  private def kMedoidsRefine[T, U >: T](
      data: Seq[T],
      metric: (U, U) => Double,
      initial: Seq[T],
      initialCost: Double,
      cost: (Seq[T], Seq[T]) => Double,
      medoidCost: (T, Seq[T]) => Double,
      maxIterations: Int): (Seq[T], Double, Int, Boolean) = {

    val k = initial.length
    val medoidIdx = (e: T, mv: Seq[T]) => mv.view.map(metric(e, _)).zipWithIndex.min._2
    val medoid = (data: Seq[T]) => data.view.minBy(medoidCost(_, data))

    var current = initial
    var currentCost = initialCost
    var converged = false

    var itr = 0
    var halt = itr >= maxIterations
    while (!halt) {
      val next = data.groupBy(medoidIdx(_, current)).toVector.sortBy(_._1).map(_._2).map(medoid)
      val nextCost = cost(next, data)

      if (nextCost >= currentCost) {
        converged = true
        halt = true
      } else {
        current = next
        currentCost = nextCost
      }

      itr += 1
      if (itr >= maxIterations) halt = true
    }

    (current, currentCost, itr, converged)
  }

  def kMedoids[T :ClassTag, U >: T :ClassTag](
      data: RDD[T],
      k: Int,
      metric: (U,U) => Double,
      sampleSize: Int = 1000,
      maxIterations: Int = 10,
      resampleInterval: Int = 3
    ): (Seq[T], Double) = {

    val n = data.count
    require(k > 0)
    require(n >= k)

    val ss = math.min(sampleSize, n).toInt
    val fraction = math.min(1.0, ss.toDouble / n.toDouble)
    var sample: Array[T] = data.sample(false, fraction).collect

    // initialize medoids to a set of (k) random and unique elements
    var medoids: Seq[T] = Random.shuffle(sample.toSeq.distinct).take(k)
    require(medoids.length >= k)

    val minDist = (e: T, mv: Seq[T]) => mv.view.map(metric(e, _)).min
    val cost = (mv: Seq[T], data: Seq[T]) => data.view.map(minDist(_, mv)).sum
    val medoidCost = (e: T, data: Seq[T]) => data.view.map(metric(e, _)).sum

    var itr = 1
    var halt = itr > maxIterations
    var lastCost = cost(medoids, sample)
    while (!halt) {
      println(s"\n\nitr= $itr")
      // update the sample periodically
      if (fraction < 1.0  &&  itr > 1  &&  (itr % resampleInterval) == 0) {
        sample = data.sample(false, fraction).collect
      }

      val (nxt, nxtCost, _, _) = 
      kMedoidsRefine(
        sample,
        metric,
        medoids,
        lastCost,
        cost,
        medoidCost,
        1)

      // todo: test some function of metric values over time as an optional halting condition
      // when improvement stops
      println(s"last= $lastCost  new= $nxtCost")
      lastCost = nxtCost
      medoids = nxt

      itr += 1
      if (itr > maxIterations) halt = true
    }

    // return most recent cluster medoids
    (medoids, lastCost)
  }
}
