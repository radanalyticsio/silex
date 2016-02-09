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

package com.redhat.et.silex.rdd.ancestry

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

/**
 * Encodes an ancestor in the dependency lineage of a target RDD.
 * @param rdd The ancestor RDD itself
 * @param ply The ply, or depth, of this RDD in the lineage.  What is the path length from
 * the target RDD to this RDD, in the orginal's lineage?
 * @param child The RDD whose dependency this RDD represents.
 * @note It is possible for an RDD to be reachable along multiple lineage paths from the target,
 * and so one ancestor RDD may appear multiple times in a target RDD's ancestry.
 */
class RDDAncestor(val rdd: RDD[_], val ply: Int, val child: RDD[_])

/** Enhances RDDs with ancestry methods */
class AncestryRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {

  /**
   * Obtain an iterator over an RDD's ancestors in it's dependency lineage.  Iteration is
   * in breadth-first order.
   * @return An iterator over the RDD's lineage ancestors.
   */
  def ancestry = new Iterator[RDDAncestor] {
    import scala.collection.mutable.Queue

    private val q = Queue(parents(self, 1) :_*)
 
    def hasNext = !q.isEmpty

    def next = {
      val n = q.dequeue()
      q.enqueue(parents(n.rdd, n.ply + 1) :_*)
      n
    }

    private def parents(rdd: RDD[_], ply: Int) =
      rdd.dependencies.map(_.rdd).map { r => new RDDAncestor(r, ply, rdd) }
  }
}

/** Implicit conversions to enhance RDDs with ancestry methods */
object implicits {
  import scala.language.implicitConversions
  implicit def fromRDD[T :ClassTag](rdd: RDD[T]): AncestryRDDFunctions[T] =
    new AncestryRDDFunctions(rdd)
}
