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

import org.scalatest._
import com.redhat.et.silex.testing.PerTestSparkContext
import com.redhat.et.silex.scalatest.matchers._

class ClusteringRandomForestModelSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import scala.language.implicitConversions
  import scala.util.Random

  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.tree.RandomForest
  import org.apache.spark.mllib.linalg.{
    Vector => SparkVec,
    DenseVector => DenseSparkVec
  }
  import org.apache.spark.mllib.regression.LabeledPoint

  import ClusteringRandomForestModel._
  import ClusteringTreeModel.Predicate
  import ClusteringTreeModel.Predicate._

  it should "provide clustering enhancements" in {
    Random.setSeed(23571113)

    val raw = Vector.fill(1000) { Array.fill(2) { Random.nextInt(2).toDouble } }

    val y = (x: Array[Double]) =>
      if (x(0) > 0.0  &&  x(1) > 0.0) 1.0 else 0.0

    val data = context.parallelize(raw)
    val trainData = data.map { x => LabeledPoint(y(x), new DenseSparkVec(x)) }

    val names = Vector("foo", "bar")
    val catInfo = Map((1 -> 2))

    val maxBins = 2

    val numClasses = 2

    val featureSubsetStrategy = "auto"
    val impurity = "gini"

    val numTrees = 5
    val maxDepth = 3

    val rf = RandomForest.trainClassifier(
      trainData,
      numClasses,
      catInfo,
      numTrees,
      featureSubsetStrategy,
      impurity,
      maxDepth,
      maxBins,
      Random.nextInt)

    val tdata1 = Vector(
      Array(0.0, 0.0),
      Array(1.0, 0.0),
      Array(0.0, 1.0),
      Array(1.0, 1.0)
    ).map(v => new DenseSparkVec(v) : SparkVec)

    tdata1.map(rf.predictLeafIds) should beEqSeq(Vector(
       Vector(2, 2, 2, 2, 2),
       Vector(6, 6, 2, 6, 6),
       Vector(2, 2, 6, 2, 2),
       Vector(7, 7, 7, 7, 7)
    ))

    context.parallelize(tdata1, 1).map(rf.predictLeafIds).collect.toSeq should beEqSeq(Vector(
       Vector(2, 2, 2, 2, 2),
       Vector(6, 6, 2, 6, 6),
       Vector(2, 2, 6, 2, 2),
       Vector(7, 7, 7, 7, 7)
    ))

    rf.countFeatureIndexes should be (Map((0 -> 5), (1 -> 5)))
    rf.countFeatures(names) should be (Map(("foo" -> 5), ("bar" -> 5)))
    rf.histFeatureIndexes should beEqSeq (Seq((0, 5), (1, 5)))
    rf.histFeatures(names) should beEqSeq (Seq(("foo", 5), ("bar", 5)))

    val rules = rf.rules(names, catInfo)
    rules.size should be (2)

    rules(0.0).map(_.toVector) should beEqSeq(Seq(
      Vector(LE("foo", 0.0)),
      Vector(GT("foo", 0.0), IN("bar", Seq(0.0))),
      Vector(LE("foo", 0.0)),
      Vector(GT("foo", 0.0), IN("bar", Seq(0.0))),
      Vector(IN("bar", Seq(0.0))),
      Vector(IN("bar", Seq(1.0)), LE("foo", 0.0)),
      Vector(LE("foo", 0.0)),
      Vector(GT("foo", 0.0), IN("bar", Seq(0.0))),
      Vector(LE("foo", 0.0)),
      Vector(GT("foo", 0.0), IN("bar", Seq(0.0)))
    ) : Seq[Vector[Predicate]])

    rules(1.0).map(_.toVector) should beEqSeq(Seq(
      Vector(GT("foo", 0.0), IN("bar", Seq(1.0))),
      Vector(GT("foo", 0.0), IN("bar", Seq(1.0))),
      Vector(IN("bar", Seq(1.0)), GT("foo", 0.0)),
      Vector(GT("foo", 0.0), IN("bar", Seq(1.0))),
      Vector(GT("foo", 0.0), IN("bar", Seq(1.0)))
    ) : Seq[Vector[Predicate]])
  }
}
