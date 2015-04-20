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

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.collection.mutable

import org.apache.spark.AccumulableParam
import org.apache.spark.rdd.RDD

object implicits {
  implicit class EnrichRDDforHistogramming[T :ClassTag](data: RDD[T]) {
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

    def countBy[U :ClassTag](f: T => U): Map[U, Long] = {
      val hacc = data.sparkContext.accumulable(empty[U])(new AccumulableCounts[U])
      data.foreach { r => hacc += f(r) }
      hacc.value.toMap
    }

    def histBy[U :ClassTag](
        f: T => U,
        normalize: Boolean = false,
        cumulative: Boolean = false
        ): Iterable[(Double, U)] = {
      var hist = countBy(f).toSeq.map(x => (x._2.toDouble, x._1)).sortWith((a, b) => a._1 > b._1)
      if (normalize && (hist.length > 0)) {
        val z = hist.map(_._1).sum
        hist = hist.map(x => (x._1 / z, x._2))
      }
      if (cumulative && (hist.length > 0)) {
        hist = hist.scan((0.0, hist.head._2))((cum, nxt) => (cum._1 + nxt._1, nxt._2)).tail
      }
      hist
    }

    def countByFlat[U :ClassTag](f: T => Iterable[U]): Map[U, Long] = {
      val hacc = data.sparkContext.accumulable(empty[U])(new AccumulableCounts[U])
      data.foreach { r =>
        for { e <- f(r) } {
          hacc += e 
        }
      }
      hacc.value.toMap
    }

    def histByFlat[U :ClassTag](
        f: T => Iterable[U],
        normalize: Boolean = false,
        cumulative: Boolean = false
        ): Iterable[(Double, U)] = {
      var hist = countByFlat(f).toSeq.map(x => (x._2.toDouble, x._1)).sortWith((a, b) => a._1 > b._1)
      if (normalize && (hist.length > 0)) {
        val z = hist.map(_._1).sum
        hist = hist.map(x => (x._1 / z, x._2))
      }
      if (cumulative && (hist.length > 0)) {
        hist = hist.scan((0.0, hist.head._2))((cum, nxt) => (cum._1 + nxt._1, nxt._2)).tail
      }
      hist
    }
  }
}
