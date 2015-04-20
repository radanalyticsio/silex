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

package com.redhat.et.silex.feature.extractor

import com.redhat.et.silex.feature.indexfunction._

/** An immutable sequence of Double values representing feature vectors extracted by an
  * [[Extractor]].  [[FeatureSeq]] is a monoid with respect to the concatenation operator
  * [[FeatureSeq!.++]].
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

/** Subclass of [[FeatureSeq]] representing a single Scala Seq object. */
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

  /** Obtain a feature sequence from a Scala
    * [[http://www.scala-lang.org/api/current/index.html#scala.collection.Seq Seq]],
    * [[http://www.scala-lang.org/api/current/index.html#scala.Array Array]], or any other
    * object with a defined conversion to [[FeatureSeq]].
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

/** A Scala Function from a domain type D, to a [[FeatureSeq]], extended with 
  * a monoidal concatenation operator [[Extractor!.++]].
  *
  * @tparam D The domain type of the function.
  * This is the type of object features are to be extracted from.
  */
abstract class Extractor[D] extends Function[D, FeatureSeq] with Serializable { self =>

  /** The width, or dimension, of the output feature space
    *
    * @return The dimension of the output feature space.  i.e. the length of the output
    * feature sequence.
    */
  def width: Int

  /** Obtain the function from domain D to a feature sequence of length [[width]].
    *
    * @return A function that maps values of domain type D to a feature sequence.
    */
  def function: D => FeatureSeq

  /** Obtain an invertable function from feature indices to corresponding feature names.
    *
    * By default, no index => name mappings are defined, unless set via [[withNames]].
    * Any subset of feature indices may be defined with a name.  Names must be unique
    * to maintain invertability.
    *
    * @return An invertable function from indices to feature names.
    */
  def names: InvertableIndexFunction[String] = names_
  private lazy val names_ = InvertableIndexFunction.undefined[String](width)

  /** Obtain a function from feature indices to number of feature category values.
    *
    * By default, no index => num-category mappings are defined, unless set via [[withCategoryInfo]].
    * Any subset of feature indices may be defined with a number of categories.
    *
    * @return A mapping from feature indices to numbers of categorical values.
    */
  def categoryInfo: IndexFunction[Int] = catInfo_
  private lazy val catInfo_ = IndexFunction.undefined[Int](width)

  /** Compose an extractor with another function.
    *
    * An Extractor[D] composed with a function G => D results in a new Extractor[G].  May be used
    * to "plug" an existing extractor into a new data type.
    *
    * @param g Some function G => D to compose with
    * @return A new extractor with domain type G equivalent to applying g and then applying
    * the original extractor.
    */
  final override def compose[G](g: G => D) = new Extractor[G] {
    def width = self.width
    def function = function_
    private lazy val function_ = self.function.compose(g)
    override def categoryInfo = self.categoryInfo
    override def names = self.names
  }

  /** Evaluate the extractor on an input argument.
    *
    * Note: extractor(d) is equivalent to extractor.function(d)
    *
    * @param d The input argument
    * @return The feature sequence resulting from applying the extractor [[function]]
    */
  final def apply(d: D): FeatureSeq = function_ (d)
  private lazy val function_ = this.function

  /** Concatenate two extractors.
    *
    * A concatenation of extractors, e1 ++ e2, is defined as follows:
    *
    * (e1 ++ e2)(d) = e1(d) ++ e2(d)
    *
    * Associated feature names and category info are concatenated similarly.
    *
    * @param that The right hand side of the concatenation.
    * @return The concatenation of left and right extractors.
    */
  final def ++(that: Extractor[D]): Extractor[D] = new Extractor[D] {
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

  /** Obtain a new extractor with new [[categoryInfo]] mappings
    *
    * Creates a copy of the current extractor with a new [[categoryInfo]] function.
    *
    * @param info The new [[categoryInfo]] function to use
    * @return A new extractor that is a copy of the current, except for new categoryInfo function.
    */
  final def withCategoryInfo(info: IndexFunction[Int]): Extractor[D] = new Extractor[D] {
    require(info.width == self.width)
    def width = self.width
    def function = self.function
    override def categoryInfo = info
    override def names = self.names
  }

  /** Obtain a new extractor with new [[categoryInfo]] mappings
    *
    * Creates a copy of the current extractor with a new [[categoryInfo]] function.
    *
    * @param pairs Zero or more pairs (name, num-categories).  Feature names that are not defined
    * in [[names]] are ignored.
    * @return A new extractor that is a copy of the current, except for new categoryInfo function.
    */
  final def withCategoryInfo(pairs: (String, Int)*): Extractor[D] = {
    val n2i = self.names.inverse
    val cif = IndexFunction(
      self.width,
      pairs.filter(p => n2i.isDefinedAt(p._1)).map(p => (n2i(p._1), p._2)):_*)
    self.withCategoryInfo(cif)
  }

  /** Obtain a new extractor with new feature [[names]] mappings
    *
    * Creates a copy of the current extractor with a new [[names]] function.
    *
    * @param nf The new [[names]] function to use
    * @return A new extractor that is a copy of the current, except for new [[names]] function.
    */
  final def withNames(nf: InvertableIndexFunction[String]): Extractor[D] = new Extractor[D] {
    require(nf.width == self.width)
    def width = self.width
    def function = self.function
    override def categoryInfo = self.categoryInfo
    override def names = nf
  }

  /** Obtain a new extractor with new feature [[names]] mappings
    *
    * Creates a copy of the current extractor with a new [[names]] function.
    *
    * @param fnames List of names.  Must be same length as extractor [[width]].
    * @return A new extractor that is a copy of the current, except for new feature names.
    */
  final def withNames(fnames: String*): Extractor[D] = {
    withNames(InvertableIndexFunction(fnames.toVector))
  }
}

/** Factory methods for creating various flavors of feature [[Extractor]]. */
object Extractor {
  /** Obtain a new empty extractor: an extractor returning [[FeatureSeq]] of zero length
    *
    * @return An extractor that returns feature sequences of zero length.
    */
  def empty[D] = new Extractor[D] {
    def width = 0
    def function = function_
    private lazy val function_ = (data: D) => FeatureSeq.empty
  }

  /** Obtain a new extractor that always returns the same constant features
    *
    * @param vList Zero or more values that will be returned as a constant [[FeatureSeq]]
    * @return A constant-valued extractor that always returns feature sequence (v0, v1, ...)
    */
  def constant[D](vList: Double*) = {
    val v = vList.toArray
    new Extractor[D] {
      def width = v.length
      def function = function_
      private lazy val function_ = (data: D) => v:FeatureSeq
    }
  }

  /** Obtain a new extractor that computes its values from zero or more functions
    *
    * @param fList Zero or more functions from domain to individual feature values.
    * @return An extractor that returns feature sequence (f0(d), f1(d), ...)
    */
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

  /** Obtain a new extractor that selects numeric values by a list of indices
    *
    * @param jList Zero or more indices
    * @return An extractor that returns feature sequence (d(j0), d(j1), ...)
    */
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


  /** Obtain a new extractor that selects numeric strings by a list of indices, and
    * converts them to Double.
    *
    * @param jList Zero or more indices
    * @return An extractor that returns feature sequence (d(j0).toDouble, d(j1).toDouble, ...)
    */
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

  /** Obtain a new extractor that loads an entire numeric sequence into a feature sequence
    *
    * @param w The [[Extractor!.width width]] of the extractor, and required input sequence length
    * @return an extractor that will load sequences of numeric values into a feature sequence
    */
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
