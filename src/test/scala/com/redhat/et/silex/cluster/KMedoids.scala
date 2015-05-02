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
import com.redhat.et.silex.testing.KSTesting

object KMedoidsSpecSupport {
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

  val vectorAbs = (x: Vector[Double], y: Vector[Double]) => {
    val n = x.length
    var j = 0
    var s = 0.0
    while (j < n) {
      s += math.abs(x(j) - y(j))
      j += 1
    }
    s
  }

  def maxCenterDistance(model: Seq[Seq[Double]], centers: Seq[Seq[Double]]) = {
    model.permutations.map { e =>
      e.zip(centers).map { p =>
        val (m, c) = p
        math.sqrt(m.zip(c).map(q => math.pow(q._1 - q._2, 2)).sum)
      }.max
    }.min
  }

  // Reference sampler, known to be correct
  def refSample[T](data: TraversableOnce[T], f: Double): Iterator[T] =
    data.toIterator.filter(u => scala.util.Random.nextDouble() < f)

  // Returns iterator over gap lengths between samples.
  // This function assumes input data is integers sampled from the sequence of
  // increasing integers: {0, 1, 2, ...}.
  def gaps(data: TraversableOnce[Int]): Iterator[Int] = {
    data.toIterator.sliding(2).withPartial(false).map { x => x(1) - x(0) }
  }

  // Generate an endless sampling stream from a block that generates a TraversableOnce
  def sampleStream[T](blk: => TraversableOnce[T]) =
    Iterator.from(0).flatMap { u => blk.toIterator }
}

class SparklessKMedoidsSpec extends FlatSpec with Matchers {
  import com.redhat.et.silex.testing.matchers._
  import KMedoidsSpecSupport._

  scala.util.Random.setSeed(23571113)

  it should "except on bad inputs" in {
    val km = new KMedoids((a: Double, b: Double) => 0.0 )
    an [IllegalArgumentException] should be thrownBy (km.setK(-1))
    an [IllegalArgumentException] should be thrownBy (km.setMaxIterations(0))
    an [IllegalArgumentException] should be thrownBy (km.setEpsilon(-0.01))
    an [IllegalArgumentException] should be thrownBy (km.setFractionEpsilon(-0.01))
    an [IllegalArgumentException] should be thrownBy (km.setSampleSize(0))
  }

  it should "sample Seq by size" in {
    val data = (0 until 100).toVector

    KSTesting.medianKSD(
      sampleStream { KMedoids.sampleBySize(data, 25) },
      sampleStream { refSample(data, 0.25) }
    ) should be < KSTesting.D

    KSTesting.medianKSD(
      sampleStream { KMedoids.sampleBySize(data, 25).length :: Nil },
      sampleStream { refSample(data, 0.25).size :: Nil }
    ) should be < KSTesting.D

    val n = 100 * KSTesting.sampleSize
    KSTesting.medianKSD(
      gaps(KMedoids.sampleBySize((0 until n).toSeq, n / 10)),
      gaps(refSample((0 until n).toSeq, 0.1))
    ) should be < KSTesting.D
  }

  it should "sample Seq by size at boundaries" in {
    val data = (0 until 100).toVector
    an [IllegalArgumentException] should be thrownBy (KMedoids.sampleBySize(data, -1))
    KMedoids.sampleBySize(data, 0).length should be (0)
    KMedoids.sampleBySize(data, 100) should equal (data)
  }

  it should "sample distinct values" in {
    val data = (0 until 100).toVector
    KSTesting.medianKSD(
      sampleStream {
        val s = KMedoids.sampleDistinct(data, 10)
        s.length should be (10)
        s.toSet.size should be (10)
        s
      },
      sampleStream { refSample(data, 0.1) }
    ) should be < KSTesting.D

    KSTesting.medianKSD(
      sampleStream {
        val s = KMedoids.sampleDistinct(data, 90)
        s.length should be (90)
        s.toSet.size should be (90)
        s
      },
      sampleStream { refSample(data, 0.9) }
    ) should be < KSTesting.D

    KSTesting.medianKSD(
      sampleStream {
        val s = KMedoids.sampleDistinct(data, 99)
        s.length should be (99)
        s.toSet.size should be (99)
        s
      },
      sampleStream { refSample(data, 0.99) }
    ) should be < KSTesting.D
  }

  it should "sample distinct values at boundaries" in {
    val data = (0 until 100).toVector
    an [IllegalArgumentException] should be thrownBy (KMedoids.sampleDistinct(data, -1))
    an [IllegalArgumentException] should be thrownBy (KMedoids.sampleDistinct(data, 101))
    KMedoids.sampleDistinct(data, 0).length should be (0)
    KMedoids.sampleDistinct(data, 100).length should be (100)
    KMedoids.sampleDistinct(data, 100).toSet should equal (data.toSet)
  }  
}

class KMedoidsSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import com.redhat.et.silex.testing.matchers._
  import KMedoidsSpecSupport._

  scala.util.Random.setSeed(23571113)

  it should "sample RDD by size" in {
    val data = (0 until 1000).toVector
    val rdd = context.parallelize(data)

    KSTesting.medianKSD(
      sampleStream { KMedoids.sampleBySize(rdd, 200) },
      sampleStream { refSample(data, 0.2) }
    ) should be < KSTesting.D

/*
    This requires running an RDD sample a thousand times, which takes way too long

    KSTesting.medianKSD(
      sampleStream { KMedoids.sampleBySize(rdd, 300).length :: Nil },
      sampleStream { refSample(data, 0.1).size :: Nil }
    ) should be < KSTesting.D
*/

    val n = 100 * KSTesting.sampleSize
    KSTesting.medianKSD(
      gaps((KMedoids.sampleBySize(context.parallelize(0 until n), n / 10))),
      gaps(refSample((0 until n).toSeq, 0.1))
    ) should be < KSTesting.D
  }

  it should "sample RDD by size at boundaries" in {
    val data = (0 until 100).toVector
    val rdd = context.parallelize(data)
    an [IllegalArgumentException] should be thrownBy (KMedoids.sampleBySize(rdd, -1))
    KMedoids.sampleBySize(rdd, 0).length should be (0)
    KMedoids.sampleBySize(rdd, 100) should equal (data)
  }

  it should "identify 2 clusters" in {
    val centers = List(
      Vector(0.0, 0.0),
      Vector(3.0, 3.0)
    )
    val data = generateClusters(centers, 1000, seed=42)
    val km = new KMedoids(vectorAbs).setK(2).setSeed(73)
    val model = km.run(context.parallelize(data))
    model.k should be (2)
    maxCenterDistance(model.medoids, centers) should be < (0.15)

    val model2 = km.run(data)
    model2.k should be (model.k)
    maxCenterDistance(model.medoids, model2.medoids) should be (0.0)
  }

  it should "respect random seeds" in {
    val centers = List(
      Vector(0.0, 0.0),
      Vector(3.0, 3.0),
      Vector(0.0, 3.0)
    )
    val data = generateClusters(centers, 1000, seed=42)
    val rdd = context.parallelize(data)

    val km1 = new KMedoids(vectorAbs).setK(2).setMaxIterations(5).setSeed(73)
    val model1 = km1.run(rdd)

    val km2 = new KMedoids(vectorAbs).setK(2).setMaxIterations(5).setSeed(73)
    val model2 = km2.run(rdd)

    val km3 = new KMedoids(vectorAbs).setK(2).setMaxIterations(5).setSeed(37)
    val model3 = km3.run(rdd)

    // identical model with same random seed
    model1.medoids should equal (model2.medoids)
    maxCenterDistance(model1.medoids, model2.medoids) should be (0.0)

    // different model with a different seed
    maxCenterDistance(model1.medoids, model3.medoids) should be > 0.0
  }

  it should "identify 5 clusters" in {
    val centers = List(
      Vector( 0.0,  0.0,  0.0),
      Vector(-3.0, -3.0, -3.0),
      Vector( 3.0,  3.0,  3.0),
      Vector(-3.0,  3.0, -3.0),
      Vector( 3.0, -3.0,  3.0)
    )
    val data = generateClusters(centers, 3000, seed=42)
    val km = new KMedoids(vectorAbs)
      .setK(5)
      .setSeed(73)
      .setFractionEpsilon(0.0)
      .setSampleSize(3000)
      .setMaxIterations(20)
    val model = km.run(context.parallelize(data))
    model.k should be (5)
    maxCenterDistance(model.medoids, centers) should be < (0.25)
  }
}
