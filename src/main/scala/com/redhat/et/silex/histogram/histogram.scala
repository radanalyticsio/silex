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

package com.redhat.et.silex.histogram

import scala.reflect.ClassTag

/** Interface for enriched histogramming and counting methods on sequence-like objects
  *
  * @tparam T The element type of the sequence
  */
abstract class HistogramMethods[T] extends Serializable {
  /** Count the occurrences of values returned by a function applied to RDD rows
    *
    * @param f The function to apply to each RDD row
    * @return A Map from occurring values to their counts
    * @tparam U The return type of the function
    */
  def countBy[U](f: T => U): Map[U, Long]

  /** Histogram the occurrences of values returned by a function applied to RDD rows
    * @param f The function to apply to each RDD row
    * @param normalized When true, histogram counts are normalized to sum to 1
    * @param cumulative When true, histogram counts are cumulative
    * @return A sequence ((u0, n0), (u1, n1), ...), sorted in decreasing order of counts nj
    * @tparam U The return type of the function
    * @note When both cumulative and normalized are true, "counts" are analoglous to a sampled CDF
    */
  def histBy[U](
      f: T => U,
      normalized: Boolean = false,
      cumulative: Boolean = false
      ): Seq[(U, Double)]

  /** Count the occurrences of values in a sequence returned by a function applied to RDD rows
    *
    * @param f The function to apply to each RDD row: returns a sequence of values
    * @return A Map from occurring values to their counts
    * @tparam U The element type of the sequence returned by the function
    */
  def countByFlat[U](f: T => Iterable[U]): Map[U, Long]

  /** Histogram the occurrences of values in a sequence returned by a function applied to RDD rows
    * @param f The function to apply to each RDD row: returns a sequence of values
    * @param normalized When true, histogram counts are normalized to sum to 1
    * @param cumulative When true, histogram counts are cumulative
    * @return A sequence ((u0, n0), (u1, n1), ...), sorted in decreasing order of counts nj
    * @tparam U The element type of the sequence returned by the function
    * @note When both cumulative and normalized are true, "counts" are analoglous to a sampled CDF
    */
  def histByFlat[U](
      f: T => Iterable[U],
      normalized: Boolean = false,
      cumulative: Boolean = false
      ): Seq[(U, Double)]
}

/** Implicit conversions for enriching objects with counting and histogramming from 
  * [[HistogramMethods]]
  */
object implicits {
  import scala.language.implicitConversions
  import org.apache.spark.rdd.RDD

  // to-do: implicit for Scala collection/traversable histogramming

  implicit def fromRDDtoHistogramMethods[T :ClassTag](data: RDD[T]): HistogramMethods[T] =
    new rdd.HistogramMethodsRDD(data)
}

private[histogram] object details {
  def toNormalized[V](iter: Iterator[(V, Double)]) = {
    if (iter.isEmpty) iter
    else {
      val (zi, si) = iter.duplicate
      val z = zi.map(_._2).sum
      si.map(x => (x._1, x._2 / z))
    }
  }

  def toCumulative[V](iter: Iterator[(V, Double)]) = {
    if (iter.isEmpty) iter
    else {
      val h = iter.next
      iter.scanLeft(h)((cum, nxt) => (nxt._1, cum._2 + nxt._2))
    }
  }
}
