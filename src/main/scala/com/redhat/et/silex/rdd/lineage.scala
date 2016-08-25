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
 * limitations under the License
 */

package com.redhat.et.silex.rdd.lineage

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

/**
 * Encodes an RDD dependency arc in the lineage DAG of a target RDD.
 * @param rdd The RDD dependency of the target
 * @param depth The depth along the current path from the target RDD to this dependency.
 * It is possible for an RDD to be reachable along multiple paths starting from the target.
 * Therefore one dependency RDD may appear multiple times in the lineage, with differing values
 * for depth depending on the particular path.
 * @param successor The RDD whose immediate dependency this is.
 */
class Dependency(val rdd: RDD[_], val depth: Int, val successor: RDD[_])

/** Enhances RDDs with lineage methods */
class LineageRDDFunctions[T :ClassTag](self: RDD[T]) extends Serializable {

  /**
   * Obtain an iterator over an RDD's dependencies in its lineage DAG.  Iteration is
   * in breadth-first order.
   * @return An iterator over the RDD's dependency lineage.
   */
  def lineage = new Iterator[Dependency] {
    import scala.collection.mutable.Queue

    private val q = Queue(parents(self, 1) :_*)
 
    def hasNext = !q.isEmpty

    def next = {
      val n = q.dequeue()
      q.enqueue(parents(n.rdd, n.depth + 1) :_*)
      n
    }

    private def parents(rdd: RDD[_], depth: Int) =
      rdd.dependencies.map(_.rdd).map { r => new Dependency(r, depth, rdd) }
  }
}

/** Implicit conversions to enhance RDDs with lineage methods */
object implicits {
  import scala.language.implicitConversions
  implicit def toLineageRDDFunctions[T :ClassTag](rdd: RDD[T]): LineageRDDFunctions[T] =
    new LineageRDDFunctions(rdd)
}
