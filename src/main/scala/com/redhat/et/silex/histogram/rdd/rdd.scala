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
  def countBy[U](f: T => U): Map[U, Long] = {
    data.aggregate(Map.empty[U, Long])(
      (m, row) => {
        val u = f(row)
        m + ((u, 1L + m.getOrElse(u, 0L)))
      },
      (m1, m2) => m2.foldLeft(m1)((m, e) => m + ((e._1, e._2 + m.getOrElse(e._1, 0L)))))
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

  def countByFlat[U](f: T => TraversableOnce[U]): Map[U, Long] = {
    data.aggregate(Map.empty[U, Long])(
      (m, row) => f(row).foldLeft(m)((m2, u) => m2 + ((u, 1L + m2.getOrElse(u, 0L)))),
      (m1, m2) => m2.foldLeft(m1)((m, e) => m + ((e._1, e._2 + m.getOrElse(e._1, 0L)))))
  }

  def histByFlat[U](
      f: T => TraversableOnce[U],
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
