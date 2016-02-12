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
import org.apache.spark.Logging

class SplitRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {
  import com.redhat.et.silex.rdd.multiplex.implicits._

  import SplitRDDFunctions.defaultSL

  def splitFilter(f: T => Boolean,
    persist: StorageLevel = defaultSL): (RDD[T], RDD[T]) = {
    self.flatMux2Partitions((data: Iterator[T]) => {
      val pass = scala.collection.mutable.ArrayBuffer.empty[T]
      val fail = scala.collection.mutable.ArrayBuffer.empty[T]
      data.foreach { e => (if (f(e)) pass else fail) += e }
      (pass, fail)
    })
  }

  def splitEither[L :ClassTag, R :ClassTag](f: T => Either[L, R],
    persist: StorageLevel = defaultSL): (RDD[L], RDD[R]) = {
    self.flatMux2Partitions((data: Iterator[T]) => {
      val left = scala.collection.mutable.ArrayBuffer.empty[L]
      val right = scala.collection.mutable.ArrayBuffer.empty[R]
      data.foreach { e => f(e).fold(lv => left += lv, rv => right += rv) }
      (left, right)
    })
  }
}

object SplitRDDFunctions {
  val defaultSL = StorageLevel.MEMORY_ONLY
}

object implicits {
  import scala.language.implicitConversions
  implicit def splitRDDFunctions[T :ClassTag](rdd: RDD[T]): SplitRDDFunctions[T] =
    new SplitRDDFunctions(rdd)
}
