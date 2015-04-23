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

package com.redhat.et.silex.rdd.promise

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging, Partition, TaskContext, 
                         Dependency, NarrowDependency}

import com.redhat.et.silex.rdd.util

private[rdd]
class FanOutDep[T: ClassTag](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  // Assuming child RDD type having only one partition
  override def getParents(pid: Int) = (0 until rdd.partitions.length)
}

private[rdd]
class PromisePartition extends Partition {
  // A PromiseRDD has exactly one partition, by construction:
  override def index = 0
}


/** Represents the concept of a promised expression as an RDD, so that it
  * can operate naturally inside the lazy-transform formalism
  */
class PromiseRDD[V: ClassTag](expr: => (TaskContext => V),
                              context: SparkContext, deps: Seq[Dependency[_]])
  extends RDD[V](context, deps) {

  // This RDD has exactly one partition by definition, since it will contain
  // a single row holding the 'promised' result of evaluating 'expr' 
  override def getPartitions = Array(new PromisePartition)

  // compute evaluates 'expr', yielding an iterator over a sequence of length 1:
  override def compute(p: Partition, ctx: TaskContext) = List(expr(ctx)).iterator
}


/** A partition that augments a standard RDD partition with a list of PromiseRDD arguments,
  * so that they are available at partition compute time
  */
private[rdd]
class PromiseArgPartition(p: Partition, argv: Seq[PromiseRDD[_]]) extends Partition {
  override def index = p.index

  /**
   * obtain the underlying partition
   */
  def partition: Partition = p

  /**
   * Compute the nth PromiseRDD argument's expression and return its value
   * The return type V must be provided explicitly, and be compatible with the
   * actual type of the PromiseRDD.
   */
  def arg[V](n: Int, ctx: TaskContext): V = 
    argv(n).iterator(new PromisePartition, ctx).next.asInstanceOf[V]
}


/** Enriched methods on RDD for creating a [[PromiseRDD]]
  *
  * To enable these methods, import:
  * {{{
  * import com.redhat.et.silex.rdd.promise.implicits._
  * }}}
  */
class PromiseRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {

  /** Obtain a PromiseRDD by applying a function 'f' to the partitions of this RDD
    *
    * @tparam V The return value type of the promised-value function
    * @param f Function that maps RDD partitions to some promised value
    * @return An RDD that will contain a single row having the promised value
    */
  def promiseFromPartitions[V :ClassTag](f: Seq[Iterator[T]] => V): RDD[V] = {
    val rdd = self
    val plist = rdd.partitions
    val expr = util.clean(
      self.context,
      (ctx: TaskContext) => f(plist.map(s => rdd.iterator(s, ctx))))
    new PromiseRDD[V](expr, rdd.context, List(new FanOutDep(rdd))) 
  }

  /** Obtain a PromiseRDD by applying function 'f' to a partition array.
    *
    * @tparam V The return value type of the promised-value function
    * @param f Function that maps an array of partitions, the RDD and the current task context
    * to the promised value
    * @return An RDD that will contain a single row having the promised value
    * @note This function is most useful in a developer context.  Consider using
    * [[promiseFromPartitions]] unless lower-level access is needed.
    */
  def promiseFromPartitionArray[V :ClassTag](f: (Array[Partition], 
                                             RDD[T], TaskContext) => V): RDD[V] = {
    val rdd = self
    val plist = rdd.partitions
    val expr = util.clean(self.context, (ctx: TaskContext) => f(plist, rdd, ctx))
    new PromiseRDD[V](expr, rdd.context, List(new FanOutDep(rdd))) 
  }
}

/** Provides implicit enrichment of an RDD with methods from [[PromiseRDDFunctions]]
  *
  * {{{
  * import com.redhat.et.silex.rdd.promise.implicits._
  * }}}
  */
object implicits {
  import scala.language.implicitConversions
  implicit def rddToPromiseRDD[T :ClassTag](rdd: RDD[T]) = new PromiseRDDFunctions(rdd)
}
