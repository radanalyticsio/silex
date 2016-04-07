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

import org.apache.commons.lang3.reflect.FieldUtils

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector => SparkVector }

import org.apache.spark.mllib.tree.model.{ DecisionTreeModel, RandomForestModel }

/** Enhance Spark RandomForestModel objects with methods for Random Forest Clustering */
class ClusteringRandomForestModel(self: RandomForestModel) extends Serializable {
  import ClusteringTreeModel._

  /** Evaluate an input feature vector and return a vector of the leaf-node ids that the
    * feature vector "landed" at for each tree in the RF ensemble
    * @param features The feature vector to evaluate
    * @return vector of leaf node ids, one from each tree in the ensemble
    */
  def predictLeafIds(features: SparkVector): Vector[Int] =
    dti.map(_.predictLeafId(features)).toVector

  /** Map an RDD of feature vectors to a corresponding RDD of leaf-node id vectors
    * @param data The RDD of feature vectors
    * @return An RDD of leaf-node id vectors
    */
  def predictLeafIds(data: RDD[SparkVector]): RDD[Vector[Int]] = data.map(this.predictLeafIds)

  /** Traverse the trees in a Random Forest ensemble and count the number of times each
    * feature index appears over all the trees
    * @return A mapping from feature index, to the number of times it appeared over the trees
    * in the ensemble
    */
  def countFeatureIndexes: Map[Int, Int] = {
    val raw = dti.flatMap(_.nodeIterator.filter(!_.isLeaf)).map(_.split.get.feature)
    raw.foldLeft(Map.empty[Int, Int]) { (h, x) =>
      val n = h.getOrElse(x, 0)
      h + (x -> (1 + n))
    }
  }

  /** Traverse the trees in a Random Forest ensemble and count the number of times each
    * feature index appears over all the trees
    * @return A sequence of pairs (idx, n) where 'idx' is a feature index and 'n' is the number of
    * times it was used in the ensemble's trees, sorted in descending order of counts.
    */
  def histFeatureIndexes: Seq[(Int, Int)] =
    countFeatureIndexes.toVector.sortWith { (a, b) => a._2 > b._2 }

  /** Traverse the trees in a Random Forest ensemble and count the number of times each
    * feature appears over all the trees
    * @param names A partial function that returns the name of a feature, given its index
    * @return A mapping from feature name to the number of times it appeared over the trees
    * in the ensemble
    */
  def countFeatures(names: PartialFunction[Int, String]): Map[String, Int] =
    countFeatureIndexes.map { case (idx, n) =>
      (names.applyOrElse(idx, defaultName), n)
    }

  /** Traverse the trees in a Random Forest ensemble and count the number of times each
    * feature appears over all the trees
    * @param names A partial function that returns the name of a feature, given its index
    * @return A sequence of pairs (name, n) where 'name' is a feature name and 'n' is the number of
    * times it was used in the ensemble's trees, sorted in descending order of counts.
    */
  def histFeatures(names: PartialFunction[Int, String]): Seq[(String, Int)] =
    histFeatureIndexes.map { case (idx, n) =>
      (names.applyOrElse(idx, defaultName), n)
    }

  /** Traverse the trees in a Random Forest ensemble and convert each path from root to a leaf
    * into a "rule" that is a sequence of individual predicates representing the decision made
    * at each internal node.
    * @param names a partial function that returns name of a feature given its index
    * @param catInfo a partial function from feature index to number of categories.  If an index
    * is not present then it is assumed to be numeric
    * @return a map from leaf-node prediction values to a collection of all rules that will
    * yield that value.
    */
  def rules(
    names: PartialFunction[Int, String],
    catInfo: PartialFunction[Int, Int]): Map[Double, Seq[Seq[Predicate]]] = {
    val dtr = dti.map(_.rules(names, catInfo))
    dtr.foldLeft(Map.empty[Double, Seq[Seq[Predicate]]]) { (m, x) =>
      x.keys.foldLeft(m) { (m, k) =>
        val s = m.getOrElse(k, Seq.empty[Seq[Predicate]])
        m + (k -> (s ++ x(k)))
      }
    }
  }

  private def dti: Iterator[DecisionTreeModel] = {
    val t = FieldUtils.readField(self, "trees", true).asInstanceOf[Array[DecisionTreeModel]]
    t.iterator
  }
}

object ClusteringRandomForestModel {
  implicit def fromRFM(self: RandomForestModel): ClusteringRandomForestModel =
    new ClusteringRandomForestModel(self)
}
