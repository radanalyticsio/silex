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

class RDDAncestor(val rdd: RDD[_], val ply: Int, val child: RDD[_])

class AncestryRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {

  def ancestry = new Iterator[RDDAncestor] {
    import scala.collection.mutable.Queue

    val q = Queue(parents(self, 1) :_*)
 
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

object implicits {
  import scala.language.implicitConversions
  implicit def fromRDD[T :ClassTag](rdd: RDD[T]): AncestryRDDFunctions[T] =
    new AncestryRDDFunctions(rdd)
}
