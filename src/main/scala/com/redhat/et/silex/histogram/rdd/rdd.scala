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

package com.redhat.et.silex.histogram.rdd

import scala.reflect.ClassTag
import scala.collection.mutable

import org.apache.spark.AccumulableParam
import org.apache.spark.rdd.RDD

import com.redhat.et.silex.histogram.HistogramMethods
import com.redhat.et.silex.histogram.details._

/** Enriched RDD methods for histogramming and counting from [[HistogramMethods]]
  * {{{
  * import com.redhat.et.silex.histogram.implicits._
  * rdd.histBy(f)  // or other methods from HistogramMethods interface
  * }}}
  */
class HistogramMethodsRDD[T :ClassTag](data: RDD[T]) extends HistogramMethods[T] {
  private type Counts[T] = mutable.Map[T, Long]
  private def empty[T] = mutable.Map.empty[T, Long]
  private val val0 = 0L
  private val val1 = 1L

  private class AccumulableCounts[T] extends AccumulableParam[Counts[T], T] {
    def addAccumulator(c: Counts[T], v: T): Counts[T] = {
      c(v) = val1 + c.getOrElse(v, val0)
      c
    }
    def addInPlace(c1: Counts[T], c2: Counts[T]): Counts[T] = {
      for { v <- c2.keys } {
        c1(v) = c2(v) + c1.getOrElse(v, val0)
      }
      c1
    }
    def zero(h: Counts[T]): Counts[T] = empty[T]
  }

  def countBy[U](f: T => U): Map[U, Long] = {
    val hacc = data.sparkContext.accumulable(empty[U])(new AccumulableCounts[U])
    data.foreach { r => hacc += f(r) }
    hacc.value.toMap
  }

  def histBy[U](
      f: T => U,
      normalized: Boolean = false,
      cumulative: Boolean = false
      ): Seq[(U, Double)] = {
    var hist = countBy(f).toSeq.sortWith((a, b) => a._2 > b._2)
      .iterator.map(x => (x._1, x._2.toDouble))
    if (normalized) hist = toNormalized(hist)
    if (cumulative) hist = toCumulative(hist)
    hist.toVector
  }

  def countByFlat[U](f: T => Iterable[U]): Map[U, Long] = {
    val hacc = data.sparkContext.accumulable(empty[U])(new AccumulableCounts[U])
    data.foreach { r =>
      for { e <- f(r) } {
        hacc += e
      }
    }
    hacc.value.toMap
  }

  def histByFlat[U](
      f: T => Iterable[U],
      normalized: Boolean = false,
      cumulative: Boolean = false
      ): Seq[(U, Double)] = {
    var hist = countByFlat(f).toSeq.sortWith((a, b) => a._2 > b._2)
      .iterator.map(x => (x._1,x._2.toDouble))
    if (normalized) hist = toNormalized(hist)
    if (cumulative) hist = toCumulative(hist)
    hist.toVector
  }
}
