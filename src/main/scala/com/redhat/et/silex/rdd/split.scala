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

package com.redhat.et.silex.rdd.split

import scala.reflect.ClassTag

import org.apache.spark.storage.StorageLevel

import org.apache.spark.rdd.RDD

/**
 * Enhances RDDs with methods for splitting RDDs based on predicates or other functions
 */
class SplitRDDFunctions[T :ClassTag](self: RDD[T]) extends Serializable {
  import scala.collection.mutable.ArrayBuffer
  import com.redhat.et.silex.rdd.multiplex.implicits._
  import SplitRDDFunctions.defaultSL

  /**
   * Split an RDD into two output RDDs, using a predicate function
   * @param f The predicate function to split with
   * @param persist The storage level to use for the intermediate result.
   * @return A pair of RDDs.  The first output contains rows for which the predicate was true, and 
   * the second contains rows for which the predicate was false.
   */
  def splitFilter(f: T => Boolean,
    persist: StorageLevel = defaultSL): (RDD[T], RDD[T]) = {
    self.flatMux2Partitions((data: Iterator[T]) => {
      val (pass, fail) = (ArrayBuffer.empty[T], ArrayBuffer.empty[T])
      data.foreach { e => (if (f(e)) pass else fail) += e }
      (pass, fail)
    }, persist)
  }

  /**
   * Split an RDD into two output RDDs, using a function that returns an Either[L, R]
   * @param f A function that returns an Either[L, R]
   * @param persist The storage level to use for the intermediate result.
   * @return A pair of RDDs.  The first output contains rows for which the function output was Left[L],
   * and the second contains rows for which the function output was Right[R]
   */
  def splitEither[L :ClassTag, R :ClassTag](f: T => Either[L, R],
    persist: StorageLevel = defaultSL): (RDD[L], RDD[R]) = {
    self.flatMux2Partitions((data: Iterator[T]) => {
      val (left, right) = (ArrayBuffer.empty[L], ArrayBuffer.empty[R])
      data.foreach { e => f(e).fold(lv => left += lv, rv => right += rv) }
      (left, right)
    }, persist)
  }
}

/** Definitions used by the SplitRDDFunctions instances */
object SplitRDDFunctions {
  /** The default storage level used for intermediate splitting results */
  val defaultSL = StorageLevel.MEMORY_ONLY
}

/** Implicit conversions to enhance RDDs with splitting methods */
object implicits {
  import scala.language.implicitConversions
  implicit def splitRDDFunctions[T :ClassTag](rdd: RDD[T]): SplitRDDFunctions[T] =
    new SplitRDDFunctions(rdd)
}
