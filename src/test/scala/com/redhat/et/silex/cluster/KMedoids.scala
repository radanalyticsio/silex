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
import org.scalatest._

import org.apache.spark.rdd.RDD

import com.redhat.et.silex.testing.PerTestSparkContext

class KMedoidsSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import com.redhat.et.silex.testing.matchers._

  def generateClusters(
    centers: Seq[Seq[Double]],
    n: Int,
    seed: Long = scala.util.Random.nextLong): Seq[Vector[Double]] = {

    val rng = new scala.util.Random(seed)
    val k = centers.length
    (0 until n).toSeq.map { t =>
      centers(t % k).map(_ + rng.nextGaussian()).toVector
    }
  }

  val vectorAbs =
    (x: Vector[Double], y: Vector[Double]) => x.zip(y).map(p => math.abs(p._1 - p._2)).sum

  def maxCenterDistance(model: Seq[Seq[Double]], centers: Seq[Seq[Double]]) = {
    model.permutations.map { e =>
      e.zip(centers).map { p =>
        val (m, c) = p
        math.sqrt(m.zip(c).map(q => math.pow(q._1 - q._2, 2)).sum)
      }.max
    }.min
  }

  it should "identify two simple clusters" in {
    val centers = List(
      Vector(0.0, 0.0),
      Vector(3.0, 3.0)
    )
    val data = context.parallelize(generateClusters(centers, 1000, seed=42))
    val km = new KMedoids(vectorAbs).setK(2).setSeed(73)
    val model = km.run(data)
    maxCenterDistance(model.medoids, centers) should be < (0.15)
  }

  it should "identify 5 clusters" in {
    val centers = List(
      Vector( 0.0,  0.0,  0.0),
      Vector(-3.0, -3.0, -3.0),
      Vector( 3.0,  3.0,  3.0),
      Vector(-3.0,  3.0, -3.0),
      Vector( 3.0, -3.0,  3.0)
    )
    val data = context.parallelize(generateClusters(centers, 1000, seed=42))
    val km = new KMedoids(vectorAbs)
      .setK(5)
      .setSeed(73)
      .setFractionEpsilon(0.0)
      .setSampleSize(1000)
      .setMaxIterations(20)
    val model = km.run(data)
    maxCenterDistance(model.medoids, centers) should be < (0.0)
  }
}
