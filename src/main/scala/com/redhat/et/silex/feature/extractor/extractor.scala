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

package io.radanalytics.silex.feature.extractor

import io.radanalytics.silex.feature.indexfunction._

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
  final def ++(that: FeatureSeq): FeatureSeq =
    if (this.length == 0) that else if (that.length == 0) this else new ConcatFS(this, that)

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
sealed class SeqFS(sq: Seq[Double]) extends FeatureSeq {
  def length = sq.length
  def apply(j: Int) = sq(j)
  def iterator = sq.iterator
  def activeKeysIterator = Iterator.range(0, length)
  def activeValuesIterator = iterator
  def density = 1.0
  override def toString = s"SeqFS(${sq})"
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
  def apply[T](obj: T)(implicit toFS: T => FeatureSeq) = obj:FeatureSeq

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
  *
  * @param width The logical width, aka dimension, of the output feature space
  * @param function The function that maps values of domain type D to a feature sequence
  * @param names A function from feature indices to number of feature category values. By default, names is undefined over the feature space.  The function may define names for none, some, or all of the feature indices.
  * @param categoryInfo A mapping from feature indices to numbers of categorical values.  By default, undefined over space of features. The function may define category info for none, some, or all of the feature indices.
  */
case class Extractor[D](
    width: Int,
    function: D => FeatureSeq,
    names: InvertibleIndexFunction[String],
    categoryInfo: IndexFunction[Int])
  extends Function[D, FeatureSeq] with Serializable {

  require(width >= 0, "Extractor width must be non-negative")
  require(names.width == width, "names width does not match extractor width")
  require(categoryInfo.width == width, "categoryInfo width does not match extractor width")

  /** Evaluate the extractor on an input argument.
    *
    * Note: extractor(d) is equivalent to extractor.function(d)
    *
    * @param d The input argument
    * @return The feature sequence resulting from applying the extractor [[function]]
    */
  final def apply(d: D): FeatureSeq = function(d)

  /** Compose an extractor with another function.
    *
    * An Extractor[D] composed with a function G => D results in a new Extractor[G].  May be used
    * to "plug" an existing extractor into a new data type.
    *
    * @param g Some function G => D to compose with
    * @return A new extractor with domain type G equivalent to applying g and then applying
    * the original extractor.
    */
  final override def compose[G](g: G => D) =
    Extractor(width, function.compose(g), names, categoryInfo)

  /** Apply another extractor to the output of this one
    *
    * f.andThenExtractor(g) = g.compose(f), in other words: (f.andThenExtractor(g))(x) => g(f(x))
    * @param g An Extractor whose domain is the output of this extractor
    * @return A new extractor equivalent to applying 'g' to the output of this extractor
    */
  final def andThenExtractor(g: Extractor[FeatureSeq]) = g.compose(this)

  /** Fold another extractor over this one
    *
    * A fold is defined as: f.fold(g) = f ++ g.compose(f), in other words: (f.fold(g))(x) => f(x) ++ g(f(x))
    *
    * When folding multiple arguments: (f.fold(g, h, ...))(x) => f(x) ++ g(f(x)) ++ h(f(x)) ...
    * @param extr the extractor to fold
    * @param rest additional extractors (if any) to fold
    * @return A new extractor that obeys the folding definition above
    */
  final def fold(extr: Extractor[FeatureSeq], rest: Extractor[FeatureSeq]*) = {
    val eseq = extr +: rest
    Extractor(
      width + eseq.map(_.width).sum,
      (d: D) => {
        val v = function(d)
        (v +: eseq.map(_.function(v))).fold(FeatureSeq.empty)(_ ++ _)
      },
      (names +: eseq.map(_.names)).fold(InvertibleIndexFunction.empty[String])(_ ++ _),
      (categoryInfo +: eseq.map(_.categoryInfo)).fold(IndexFunction.empty[Int])(_ ++ _)
    )
  }

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
  final def ++(that: Extractor[D]) =
    if (this.width == 0) that
    else if (that.width == 0) this
    else Extractor(
      this.width + that.width,
      (d: D) => this.function(d) ++ that.function(d),
      this.names ++ that.names,
      this.categoryInfo ++ that.categoryInfo)

  /** Obtain a new extractor with new feature [[names]] mappings
    *
    * Creates a copy of the current extractor with a new [[names]] function.
    *
    * @param nf The new [[names]] function to use
    * @return A new extractor that is a copy of the current, except for new [[names]] function.
    */
  final def withNames(nf: InvertibleIndexFunction[String]): Extractor[D] = {
    require(nf.width == width, "names width does not match extractor width")
    this.copy(names = nf)
  }

  /** Obtain a new extractor with new feature [[names]] mappings
    *
    * Creates a copy of the current extractor with a new [[names]] function.
    *
    * @param fnames List of names.  Must be same length as extractor [[width]].
    * @return A new extractor that is a copy of the current, except for new feature names.
    */
  final def withNames(fnames: String*): Extractor[D] = {
    withNames(InvertibleIndexFunction(fnames.toVector))
  }

  /** Obtain a new extractor with new [[categoryInfo]] mappings
    *
    * Creates a copy of the current extractor with a new [[categoryInfo]] function.
    *
    * @param info The new [[categoryInfo]] function to use
    * @return A new extractor that is a copy of the current, except for new categoryInfo function.
    */
  final def withCategoryInfo(info: IndexFunction[Int]): Extractor[D] = {
    require(info.width == width, "categoryInfo width does not match extractor width")
    this.copy(categoryInfo = info)
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
    val n2i = names.inverse
    val cif = IndexFunction(
      width,
      pairs.filter(p => n2i.isDefinedAt(p._1)).map(p => (n2i(p._1), p._2)):_*)
    this.copy(categoryInfo = cif)
  }
}

/** Factory methods for creating various flavors of feature [[Extractor]]. */
object Extractor {
  def apply[D](width: Int, function: D => FeatureSeq): Extractor[D] =
    Extractor(
     width,
     function,
     InvertibleIndexFunction.undefined[String](width),
     IndexFunction.undefined[Int](width))

  /** Obtain a new empty extractor: an extractor returning [[FeatureSeq]] of zero length
    *
    * @return An extractor that returns feature sequences of zero length.
    */
  def empty[D] = Extractor(0, (data: D) => FeatureSeq.empty)

  /** Obtain a new extractor that always returns the same constant features
    *
    * @param vList Zero or more values that will be returned as a constant [[FeatureSeq]]
    * @return A constant-valued extractor that always returns feature sequence (v0, v1, ...)
    */
  def constant[D](vList: Double*) = {
    val v = vList.toArray
    Extractor(v.length, (data: D) => v:FeatureSeq)
  }

  /** Obtain a new extractor that computes its values from zero or more functions
    *
    * @param fList Zero or more functions from domain to individual feature values.
    * @return An extractor that returns feature sequence (f0(d), f1(d), ...)
    */
  def apply[D](fList: (D => Double)*): Extractor[D] = {
    val fv = fList.toArray
    Extractor(
      fv.length,
      (data: D) => {
        val v = fv.map(f => f(data))
        v:FeatureSeq
      })
  }

  /** Obtain a new extractor that selects numeric values by a list of indices
    *
    * @param jList Zero or more indices
    * @return An extractor that returns feature sequence (d(j0), d(j1), ...)
    */
  def numeric[N :Numeric](jList: Int*) = {
    val jv = jList.toArray
    val num = implicitly[Numeric[N]]
    Extractor(
      jList.length,
      (data: PartialFunction[Int, N]) => (jv.map(j => num.toDouble(data(j)))):FeatureSeq)
  }

  /** Obtain a new extractor that selects numeric strings by a list of indices, and
    * converts them to Double.
    *
    * @param jList Zero or more indices
    * @return An extractor that returns feature sequence (d(j0).toDouble, d(j1).toDouble, ...)
    */
  def string(jList: Int*) = {
    val jv = jList.toArray
    Extractor(
      jv.length,
      (data: PartialFunction[Int, String]) => (jv.map(j => data(j).toDouble)):FeatureSeq)
  }

  /** Obtain a new extractor that loads an entire numeric sequence into a feature sequence
    *
    * @param w The [[Extractor!.width width]] of the extractor, and required input sequence length
    * @return an extractor that will load sequences of numeric values into a feature sequence
    */
  def numericSeq[N :Numeric](w: Int) = {
    val num = implicitly[Numeric[N]]
    Extractor(
      w,
      (data: Seq[N]) => {
        require(data.length == w)
        (data.map(x => num.toDouble(x))):FeatureSeq    
      })
  }

  /** Generate a quadratic expansion over a set of named features.
    * @param extr The Extractor whose output features are to be expanded.
    * @param features A list of feature names to expand.
    * @param diag If true, expand features with themselves (square them).  Otherwise, only
    * generate products with other features.  Defaults to false.
    * @return An extractor whose domain is the output of this extractor, which computes
    * the pairwise products of the given features.
    */
  def quadraticByName[D](extr: Extractor[D], features: Seq[String], diag: Boolean = false) =
    quadraticByIndex(extr, features.map(extr.names.inverse), diag)

  /** Generate a quadratic expansion over a set of features by their index.
    * @param extr The Extractor whose output features are to be expanded.
    * @param indexes A list of the feature indexes to expand.
    * @param diag If true, expand features with themselves (square them).  Otherwise, only
    * generate products with other features.  Defaults to false.
    * @return An extractor whose domain is the output of this extractor, which computes
    * the pairwise products of the given features.
    */
  def quadraticByIndex[D](extr: Extractor[D], indexes: Seq[Int], diag: Boolean = false) = {
    require(indexes.forall(j => (j >= 0 && j < extr.width)), s"indexes out of range [0, ${extr.width})")

    val tuples = () => for {
      j <- 0 until indexes.length;
      k <- (j + (if (diag) 0 else 1)) until indexes.length
    } yield (indexes(j), indexes(k))

    val width = tuples().length
    val ndef = tuples().count { case (j, k) => extr.names.isDefinedAt(j) && extr.names.isDefinedAt(k) }
    val names =
      if (ndef == width) {
        InvertibleIndexFunction(
          tuples().map { case (j, k) => extr.names(j) + "*" + extr.names(k) }.toVector)
      } else {
        InvertibleIndexFunction(
          width,
          tuples().zipWithIndex
            .filter { case ((j, k), _) => extr.names.isDefinedAt(j) && extr.names.isDefinedAt(k) }
            .map { case ((j, k), idx) => (idx, extr.names(j) + "*" + extr.names(k)) }:_*)
      }

    Extractor(
      width,
      (s: FeatureSeq) => (tuples().map { case (j, k) => s(j) * s(k) }.toVector):FeatureSeq,
      names,
      IndexFunction.undefined[Int](width))
  }
}
