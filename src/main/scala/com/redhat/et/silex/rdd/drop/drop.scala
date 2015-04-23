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

package com.redhat.et.silex.rdd.drop

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging, Partition, TaskContext}
import org.apache.spark.{Dependency, NarrowDependency, OneToOneDependency}

import com.redhat.et.silex.rdd.promise.{ PromiseRDD, PromiseArgPartition }
import com.redhat.et.silex.rdd.promise.implicits._

private [rdd]
class FanInDep[T: ClassTag](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  // Assuming parent RDD type having only one partition
  override def getParents(pid: Int) = List(0)
}


/** Enriched methods for providing the RDD analogs of Scala drop, dropRight and dropWhile,
  * which return an RDD as a result
  * {{{
  * import com.redhat.et.silex.rdd.drop.implicits._
  * }}}
  */
class DropRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {

  /** Obtain a new RDD formed by dropping the first (n) elements of the input RDD
    *
    * @param n The number of rows to drop
    * @return an RDD formed from dropping the first 'n' rows of the input
    */
  def drop(n: Int): RDD[T] = {
    if (n <= 0) return self

    // locate partition that includes the nth element
    val locate = (partitions: Array[Partition], input: RDD[T], ctx: TaskContext) => {
      var rem = n
      var p = 0
      var np = 0
      while (rem > 0  &&  p < partitions.length) {
        np = input.iterator(partitions(p), ctx).length
        rem -= np
        p += 1
      }

      if (rem > 0  ||  (rem == 0  &&  p >= partitions.length)) {
        // all elements were dropped
        (p, 0)
      } else {
        // (if we get here, note that rem <= 0)
        (p - 1, np + rem)
      }
    }

    val locRDD = self.promiseFromPartitionArray(locate).asInstanceOf[PromiseRDD[(Int,Int)]]

    new RDD[T](self.context, List(new OneToOneDependency(self), new FanInDep(locRDD))) {
      override def getPartitions: Array[Partition] = 
        self.partitions.map(p => new PromiseArgPartition(p, List(locRDD)))

      override val partitioner = self.partitioner

      override def compute(split: Partition, ctx: TaskContext):Iterator[T] = {
        val dp = split.asInstanceOf[PromiseArgPartition]
        val (pFirst, pDrop) = dp.arg[(Int,Int)](0, ctx)
        val input = firstParent[T]
        if (dp.index > pFirst) return input.iterator(dp.partition, ctx)
        if (dp.index == pFirst) return input.iterator(dp.partition, ctx).drop(pDrop)
        Iterator.empty
      }
    }
  }

  /** Obtain a new RDD formed by dropping the last (n) elements of the input RDD
    *
    * @param n The number of rows to drop
    * @return an RDD formed from dropping the last 'n' rows of the input
    */
  def dropRight(n: Int):RDD[T] = {
    if (n <= 0) return self

    val locate = (partitions: Array[Partition], input: RDD[T], ctx: TaskContext) => {
      var rem = n
      var p = partitions.length-1
      var np = 0
      while (rem > 0  &&  p >= 0) {
        np = input.iterator(partitions(p), ctx).length
        rem -= np
        p -= 1
      }

      if (rem > 0  ||  (rem == 0  &&  p < 0)) {
        // all elements were dropped
        (p, 0)
      } else {
        // (if we get here, note that rem <= 0)
        (p + 1, -rem)
      }
    }

    val locRDD = self.promiseFromPartitionArray(locate).asInstanceOf[PromiseRDD[(Int,Int)]]

    new RDD[T](self.context, List(new OneToOneDependency(self), new FanInDep(locRDD))) {
      override def getPartitions: Array[Partition] = 
        self.partitions.map(p => new PromiseArgPartition(p, List(locRDD)))

      override val partitioner = self.partitioner

      override def compute(split: Partition, ctx: TaskContext):Iterator[T] = {
        val dp = split.asInstanceOf[PromiseArgPartition]
        val (pFirst, pTake) = dp.arg[(Int,Int)](0, ctx)
        val input = firstParent[T]
        if (dp.index < pFirst) return input.iterator(dp.partition, ctx)
        if (dp.index == pFirst) return input.iterator(dp.partition, ctx).take(pTake)
        Iterator.empty
      }
    }
  }  

  /** Obtain a new RDD formed by dropping leading rows until predicate function (f) returns false
    *
    * @param f Predicate function.  Input rows are dropped until f returns false
    * @return An RDD formed by dropping leading rows until predicate function (f) returns false
    */
  def dropWhile(f: T=>Boolean):RDD[T] = {

    val locate = (partitions: Array[Partition], input: RDD[T], ctx: TaskContext) => {
      var p = 0
      var np = 0
      while (np <= 0  &&  p < partitions.length) {
        np = input.iterator(partitions(p), ctx).dropWhile(f).length
        p += 1
      }

      if (np <= 0  &&  p >= partitions.length) {
        // all elements were dropped
        p
      } else {
        p - 1
      }
    }

    val locRDD = self.promiseFromPartitionArray(locate).asInstanceOf[PromiseRDD[(Int,Int)]]

    new RDD[T](self) {
      override def getPartitions: Array[Partition] = 
        self.partitions.map(p => new PromiseArgPartition(p, List(locRDD)))

      override val partitioner = self.partitioner

      override def compute(split: Partition, ctx: TaskContext):Iterator[T] = {
        val dp = split.asInstanceOf[PromiseArgPartition]
        val pFirst = dp.arg[Int](0, ctx)
        val input = firstParent[T]
        if (dp.index > pFirst) return input.iterator(dp.partition, ctx)
        if (dp.index == pFirst) return input.iterator(dp.partition, ctx).dropWhile(f)
        Iterator.empty
      }
    }    
  }
}

/** Provides implicit enrichment of an RDD with methods from [[DropRDDFunctions]]
  *
  * {{{
  * import com.redhat.et.silex.rdd.drop.implicits._
  * }}}
  */
object implicits {
  import scala.language.implicitConversions
  implicit def rddToDropRDD[T :ClassTag](rdd: RDD[T]) = new DropRDDFunctions(rdd)
}
