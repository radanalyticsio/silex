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

/** Provides a feature extraction type system that supports concatenation of feature extraction
  * functions in a logically constant and type safe manner.  Extraction functions are represented by
  * instances of [[com.redhat.et.silex.feature.extractor.Extractor]].
  */
package com.redhat.et.silex.feature.extractor

import com.redhat.et.silex.feature.indexfunction._

/** An immutable sequence of Double values representing feature vectors extracted by an
  * [[Extractor]].  [[FeatureSeq]] is a monoid with respect to the concatenation operator [[++]].
  */
abstract class FeatureSeq extends scala.collection.immutable.Seq[Double] with Serializable {
  /** The length of the feature sequence
    *
    * @return The number of feature values in the sequence
    */
  def length: Int

  /** The jth element of the feature sequence
    *
    * @param j element index
    * @return The value of jth feature value
    */
  def apply(j: Int): Double

  /** Obtain a new iterator over the elements of the feature sequence
    *
    * @return An iterator over the feature values
    */
  def iterator: Iterator[Double]

  /** Obtain a new iterator over indices of explicitly-stored feature values.  Equivalent
    * to [[keysIterator]] when the feature sequence is dense.
    *
    * @return An iterator over indices of explicitly-stored feature values.
    */
  def activeKeysIterator: Iterator[Int]

  /** Obtain a new iterator over explicitly-stored feature values.  Equivalent to [[valuesIterator]]
    * when the feature sequence is dense.
    *
    * @return An iterator over explicitly-stored values
    */
  def activeValuesIterator: Iterator[Double]

  /** The density of explicitly-stored feature values. Defined as the ratio of explicitly-stored
    * values to total [[length]].
    *
    * @return The density of explicitly-stored values
    */
  def density: Double

  /** Concatenate two feature sequences.
    *
    * @param that The right-argument.  Feature values are appended to the result.
    * @return A new feature sequence consisting of left-argument followed by the right-argument.
    */
  final def ++(that: FeatureSeq): FeatureSeq = new ConcatFS(this, that)

  /** Obtain a new iterator over the indices of the feature sequence.
    *
    * @return An iterator over all the indices defined by the feature sequence.
    */
  final def keysIterator = Iterator.range(0, length)

  /** Obtain a new iterator over the feature values in a feature sequence.
    *
    * @return An iterator over all feature values in the sequence.
    */
  final def valuesIterator = iterator
}

/** Subclass of [[FeatureSeq]] representing the concatenation of two feature sequences.  The
  * type returned by [[++]] operator
  */
sealed class ConcatFS(fb1: FeatureSeq, fb2: FeatureSeq) extends FeatureSeq {
  def length = length_
  private lazy val length_ = fb1.length + fb2.length

  def apply(j: Int) = if (j < fb1.length) fb1(j) else fb2(j - fb1.length)

  def density = {
    val (l1, l2) = (fb1.length.toDouble, fb2.length.toDouble)
    val d = l1 + l2
    if (d <= 0.0) 1.0 else (l1 * fb1.density + l2 * fb2.density) / d
  }

  def iterator = fb1.iterator ++ fb2.iterator
  def activeKeysIterator = fb1.activeKeysIterator ++ (fb2.activeKeysIterator.map(_ + fb1.length))
  def activeValuesIterator = fb1.activeValuesIterator ++ fb2.activeValuesIterator  

  override def toString = s"ConcatFS(${fb1}, ${fb2})"
}

/** Subclass of [[FeatureSeq]] representing a single Scala [[Seq]] object. */
sealed class SeqFS(seq: Seq[Double]) extends FeatureSeq {
  def length = seq.length
  def apply(j: Int) = seq(j)
  def iterator = seq.iterator
  def activeKeysIterator = Iterator.range(0, length)
  def activeValuesIterator = iterator
  def density = 1.0
  override def toString = s"SeqFS(${seq})"
}

/** Provides factory methods for [[FeatureSeq]] instances. */
object FeatureSeq {
  import scala.language.implicitConversions

  /** Obtain an empty feature sequence
    *
    * @return a feature sequence of length zero.
    */
  def empty: FeatureSeq = new FeatureSeq {
    def length = 0
    def apply(j: Int) = (Seq.empty)(j)
    def iterator = Iterator.empty
    def activeKeysIterator = Iterator.empty
    def activeValuesIterator = Iterator.empty
    def density = 1.0
  }

  implicit def fromSeqToSeqFS(seq: Seq[Double]): FeatureSeq = new SeqFS(seq)
  implicit def fromArrayToSeqFS(a: Array[Double]): FeatureSeq =
    new SeqFS(a:scala.collection.mutable.WrappedArray[Double])

  /** Obtain a feature sequence from a Scala [[Seq]], [[Array]], or any other object with
    * a defined conversion to [[FeatureSeq]].
    *
    * @param obj The object to create a feature sequence from
    * @return A new feature sequence created from the argument object
    */
  def apply[T <% FeatureSeq](obj: T) = obj:FeatureSeq

  /** Create a feature sequence from the given sequence of zero or more feature values.
    *
    * @param values A list of zero or more feature values to be contained in the sequence
    * @return A new feature sequence containing the given values, in the order supplied.
    */
  def apply(values: Double*): FeatureSeq = new SeqFS(values.toVector)
}

// a feature extractor is a function from some domain D to an intermediate
// representation that (a) supports sparse and dense representations and (b) is easily 
// converted to 3rd-party representations such as Spark mllib vectors.
abstract class Extractor[D] extends Function[D, FeatureSeq] with Serializable { self =>
  // abstract methods
  def width: Int
  def function: D => FeatureSeq

  // by default, feature name function is empty - defined on no indexes
  def names: InvertableIndexFunction[String] = names_
  private lazy val names_ = InvertableIndexFunction.undefined[String](width)

  // By default, no category information is supplied.  If used with Spark MLLib, this
  // signifies "no features are categorical"
  // May be overridden if desired to supply an explicit category information map,
  // to a feature extractor that doesn't have one, or to override its default
  def categoryInfo: IndexFunction[Int] = catInfo_
  private lazy val catInfo_ = IndexFunction.undefined[Int](width)

  // use this to plug an extractor into custom shim/adaptor function
  // extractor compose shimFunction
  final override def compose[G](g: G => D) = new Extractor[G] {
    def width = self.width
    def function = function_
    private lazy val function_ = self.function.compose(g)
    override def categoryInfo = self.categoryInfo
    override def names = self.names
  }

  final def apply(data: D): FeatureSeq = function_ (data)
  private lazy val function_ = this.function

  // this is where you can concatenate extractors
  final def ++(that: Extractor[D]) = new Extractor[D] {
    def width = self.width + that.width

    def function = function_
    private lazy val function_ = {
      val (f1, f2) = (self.function, that.function)
      (data: D) => f1(data) ++ f2(data)
    }

    override def categoryInfo = catInfo_
    private lazy val catInfo_ = self.categoryInfo ++ that.categoryInfo

    override def names = names_
    private lazy val names_ = self.names ++ that.names
  }

  // A way to augment an existing feature extractor with a categoryInfo map, or
  // to override its existing map
  final def withCategoryInfo(info: IndexFunction[Int]): Extractor[D] = new Extractor[D] {
    require(info.width == self.width)
    def width = self.width
    def function = self.function
    override def categoryInfo = info
    override def names = self.names
  }
  final def withCategoryInfo(pairs: (String, Int)*): Extractor[D] = {
    val n2i = self.names.inverse
    val cif = IndexFunction(
      self.width,
      pairs.filter(p => n2i.isDefinedAt(p._1)).map(p => (n2i(p._1), p._2)):_*)
    self.withCategoryInfo(cif)
  }

  final def withNames(nf: InvertableIndexFunction[String]): Extractor[D] = new Extractor[D] {
    require(nf.width == self.width)
    def width = self.width
    def function = self.function
    override def categoryInfo = self.categoryInfo
    override def names = nf
  }
  final def withNames(fnames: String*): Extractor[D] = {
    withNames(InvertableIndexFunction(fnames.toVector))
  }
}

// defines various feature extraction functions
// intent is to make definition of feature extraction component functions easy, and easy to compose
object Extractor {
  // the empty feature extractor
  def empty[D] = new Extractor[D] {
    def width = 0
    def function = function_
    private lazy val function_ = (data: D) => FeatureSeq.empty
  }

  def constant[D](vList: Double*) = {
    val v = vList.toArray
    new Extractor[D] {
      def width = v.length
      def function = function_
      private lazy val function_ = (data: D) => v:FeatureSeq
    }
  }

  // apply zero or more functions to some data object
  def apply[D](fList: (D => Double)*) = {
    val w = fList.length
    new Extractor[D] {
      def width = w
      def function = function_
      private lazy val function_ = {
        (data: D) => {
          val v = new Array[Double](w)
          var j = 0
          fList.foreach { f =>
            v(j) = f(data)
            j += 1
          }
          v:FeatureSeq
        }
      }
    }
  }

  // select zero or more numeric values by index, cast to Double
  def numeric[N :Numeric](jList: Int*) = {
    val jv = jList.toArray
    val w = jv.length
    val num = implicitly[Numeric[N]]
    new Extractor[PartialFunction[Int, N]] {
      def width = w
      def function = function_
      private lazy val function_ = 
        (data: PartialFunction[Int, N]) => (jv.map(j => num.toDouble(data(j)))):FeatureSeq
    }
  }

  // select zero or more string values by index, cast to Double
  def string(jList: Int*) = {
    val jv = jList.toArray
    val w = jv.length
    new Extractor[PartialFunction[Int, String]] {
      def width = w
      def function = function_
      private lazy val function_ = 
        (data: PartialFunction[Int, String]) => (jv.map(j => data(j).toDouble)):FeatureSeq
    }
  }

  // load an entire sequence of numeric values
  def numericSeq[N :Numeric](w: Int) = {
    val num = implicitly[Numeric[N]]
    new Extractor[Seq[N]] {
      def width = w
      def function = function_
      private lazy val function_ = (data: Seq[N]) => {
        require(data.length == w)
        (data.view.map(x => num.toDouble(x))):FeatureSeq    
      }
    }
  }
}
