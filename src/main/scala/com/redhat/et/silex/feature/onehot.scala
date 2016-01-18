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
 * limitations under the License.
 */

package com.redhat.et.silex.feature.onehot

import scala.reflect.ClassTag
import scala.collection.immutable.SortedSet
import scala.collection.immutable.SortedMap

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{
  DenseVector => DenseSV,
  SparseVector => SparseSV
}

import com.redhat.et.silex.feature.extractor._
import com.redhat.et.silex.feature.extractor.spark.implicits._
import com.redhat.et.silex.feature.indexfunction._
import com.redhat.et.silex.util.OptionalArg
import com.redhat.et.silex.util.OptionalArg.fullOptionSupport
import com.redhat.et.silex.histogram.implicits._

/** A model for generating Extractor objects from a histogram of values
  *
  * @tparam V The value type of elements in histogram
  * @param histogram A histogram of (value, frequency) elements, assumed to be sorted in descending order of frequency.
  *
  * @define namePrefixDoc @param namePrefix a string prepended to feature names.
  * The name of feature corresponding to value v is namePrefix + v.toString.
  * @define undefNameDoc @param undefName a "virtual" value corresponding to any value that is 
  * not in the histogram. If not set, then undefined values result in an all-zero output vector.
  * @define minFreqDoc @param minFreq If set, then histogram values with frequency < minFreq
  * will not be defined in the extractor mapping.
  * @define maxFreqDoc @param maxFreq If set, then histogram values with frequency > maxFreq
  * will not be defined in the extractor mapping.
  * @define minProbDoc @param minProb If set, then histogram values with probability < minProb
  * will not be defined in the extractor mapping.
  * @define maxProbDoc @param maxProb If set, then histogram values with probability > maxProb
  * will not be defined in the extractor mapping.
  * @define maxSizeDoc @param maxSize If set, then the subsequence of histogram values 
  * that pass any of the above filters is clipped to maxSize elements, and the remaining
  * elements are the ones defined in the extractor mapping.
  * @define orderNote @note the histogram filters above are applied in the following order: minFreq, maxFreq, minProb, maxProb, maxSize.
  */
case class OneHotModel[V](histogram: Seq[(V, Double)]) {

  /** Generate a "one-hot" extractor that maps a value of type V to a vector where the position
    * corresponding to that value is 1, and all others elements are 0.
    *
    * $namePrefixDoc
    * $undefNameDoc
    * $minFreqDoc
    * $maxFreqDoc
    * $minProbDoc
    * $maxProbDoc
    * $maxSizeDoc
    * @return An extractor that implements a one-hot encoding of input values.
    * $orderNote
    */
  def oneHotExtractor(
    namePrefix: String = "v=",
    undefName: OptionalArg[String] = None,
    minFreq: OptionalArg[Int] = None,
    maxFreq: OptionalArg[Int] = None,
    minProb: OptionalArg[Double] = None,
    maxProb: OptionalArg[Double] = None,
    maxSize: OptionalArg[Int] = None) = {
    val v = filtered(minFreq, maxFreq, minProb, maxProb, maxSize).map(_._1).toVector
    val undef = undefName.isDefined
    val width = v.length + (if (undef) 1 else 0)
    val v2i = InvertibleIndexFunction(v).inverse
    val nvec = v2i.domain.map(namePrefix + _.toString).toVector
      .++(if (undef) Vector(namePrefix + undefName.get) else Vector[String]())
    val names = InvertibleIndexFunction(nvec)
    val function =
      if (undef) {
        (v: V) => {
          val j = if (v2i.isDefinedAt(v)) v2i(v) else width - 1
          FeatureSeq(new SparseSV(width, Array(j), Array(1.0)))
        }
      } else {
        (v: V) => {
          if (v2i.isDefinedAt(v))
            FeatureSeq(new SparseSV(width, Array(v2i(v)), Array(1.0)))
          else
            FeatureSeq(new SparseSV(width, Array[Int](), Array[Double]()))
        }
      }
    Extractor(width, function, names, IndexFunction.constant(2, width))
  }

  /** Generate a "multi-hot" extractor that maps a sequence of type V to a vector where the position
    * corresponding to each value present in the sequence is 1, and all others elements are 0.
    *
    * $namePrefixDoc
    * $undefNameDoc
    * $minFreqDoc
    * $maxFreqDoc
    * $minProbDoc
    * $maxProbDoc
    * $maxSizeDoc
    * @return An extractor that implements a multi-hot encoding of values in an input sequence.
    * $orderNote
    */
  def multiHotExtractor(
    namePrefix: String = "v=",
    undefName: OptionalArg[String] = None,
    minFreq: OptionalArg[Int] = None,
    maxFreq: OptionalArg[Int] = None,
    minProb: OptionalArg[Double] = None,
    maxProb: OptionalArg[Double] = None,
    maxSize: OptionalArg[Int] = None) = {
    val v = filtered(minFreq, maxFreq, minProb, maxProb, maxSize).map(_._1).toVector
    val undef = undefName.isDefined
    val width = v.length + (if (undef) 1 else 0)
    val v2i = InvertibleIndexFunction(v).inverse
    val nvec = v2i.domain.map(namePrefix + _.toString).toVector
      .++(if (undef) Vector(namePrefix + undefName.get) else Vector[String]())
    val names = InvertibleIndexFunction(nvec)
    val function =
      if (undef) {
        (data: TraversableOnce[V]) => {
          val idx = data.toIterator.map(v => if (v2i.isDefinedAt(v)) v2i(v) else width - 1)
          val sorted = idx.foldLeft(SortedSet.empty[Int])((ss, e) => ss + e)
          FeatureSeq(new SparseSV(width, sorted.toArray, Array.fill(sorted.size)(1.0)))
        }
      } else {
        (data: TraversableOnce[V]) => {
          val idx = data.toIterator.filter(v2i.isDefinedAt(_)).map(v2i)
          val sorted = idx.foldLeft(SortedSet.empty[Int])((ss, e) => ss + e)
          FeatureSeq(new SparseSV(width, sorted.toArray, Array.fill(sorted.size)(1.0)))
        }
      }
    Extractor(width, function, names, IndexFunction.constant(2, width))
  }

  /** Generate a histogram extractor that maps a sequence of type V to a vector where the position
    * corresponding to each value present in the sequence is the frequency of that value in the 
    * sequence.
    *
    * $namePrefixDoc
    * $undefNameDoc
    * $minFreqDoc
    * $maxFreqDoc
    * $minProbDoc
    * $maxProbDoc
    * $maxSizeDoc
    * @return An extractor that implements a histogram of the values in an input sequence.
    * $orderNote
    */
  def histExtractor(
    namePrefix: String = "v=",
    undefName: OptionalArg[String] = None,
    minFreq: OptionalArg[Int] = None,
    maxFreq: OptionalArg[Int] = None,
    minProb: OptionalArg[Double] = None,
    maxProb: OptionalArg[Double] = None,
    maxSize: OptionalArg[Int] = None) = {
    val v = filtered(minFreq, maxFreq, minProb, maxProb, maxSize).map(_._1).toVector
    val undef = undefName.isDefined
    val width = v.length + (if (undef) 1 else 0)
    val v2i = InvertibleIndexFunction(v).inverse
    val nvec = v2i.domain.map(namePrefix + _.toString).toVector
      .++(if (undef) Vector(namePrefix + undefName.get) else Vector[String]())
    val names = InvertibleIndexFunction(nvec)
    val function =
      if (undef) {
        (data: TraversableOnce[V]) => {
          val idx = data.toIterator.map(v => if (v2i.isDefinedAt(v)) v2i(v) else width - 1)
          val hist = idx.foldLeft(SortedMap.empty[Int, Double]) { (h, e) =>
            h + ((e, 1.0 + h.getOrElse(e, 0.0)))
          }
          FeatureSeq(new SparseSV(width, hist.keys.toArray, hist.values.toArray))
        }
      } else {
        (data: TraversableOnce[V]) => {
          val idx = data.toIterator.filter(v2i.isDefinedAt(_)).map(v2i)
          val hist = idx.foldLeft(SortedMap.empty[Int, Double]) { (h, e) =>
            h + ((e, 1.0 + h.getOrElse(e, 0.0)))
          }
          FeatureSeq(new SparseSV(width, hist.keys.toArray, hist.values.toArray))
        }
      }
    Extractor(width, function, names, IndexFunction.undefined[Int](width))
  }

  private def filtered(
    minFreq: OptionalArg[Int],
    maxFreq: OptionalArg[Int],
    minProb: OptionalArg[Double],
    maxProb: OptionalArg[Double],
    maxSize: OptionalArg[Int]
  ) = {
    val n = if (minProb.isEmpty && maxProb.isEmpty) 0.0 else histogram.iterator.map(_._2).sum
    var h = histogram.iterator
    // Filter order matters here: by frequency, then by probability, then by max size
    minFreq.foreach { f => h = h.filter(_._2 >= f.toDouble) }
    maxFreq.foreach { f => h = h.filter(_._2 <= f.toDouble) }
    minProb.foreach { p => h = h.filter(_._2 / n >= p) }
    maxProb.foreach { p => h = h.filter(_._2 / n <= p) }
    maxSize.foreach { s => h = h.take(s) }
    h
  }
}

/** define oneHotBy family of methods on Spark RDDs */
class OneHotMethodsRDD[D :ClassTag](rdd: RDD[D]) {
  /** Generate a one-hot model from the values obtained by applying 'f' to each RDD row
    *
    * @param f The function to apply to each row to obtain histogram values
    * @return A one-hot model corresponding to the resulting histogram
    */
  def oneHotBy[V](f: D => V) = new OneHotModel(rdd.histBy(f))

  /** Generate a one-hot model from the values in each sequence obtained by applying 'f' 
    * to the RDD rows
    *
    * @param f The function to apply to each row to obtain a sequence of histogram values
    * @return A one-hot model corresponding to the resulting histogram
    */
  def oneHotByFlat[V](f: D => TraversableOnce[V]) = new OneHotModel(rdd.histByFlat(f))
}

/** implicit conversions for enhanced methods in the OneHotMethods family */ 
object implicits {
  import scala.language.implicitConversions
  implicit def toOneHotMethodsRDD[D :ClassTag](rdd: RDD[D]) = new OneHotMethodsRDD(rdd)
}
