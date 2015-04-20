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

package com.redhat.et.silex.feature.indexfunction

import scala.language.implicitConversions

/** An index function is a (partial) function from some subset of collection indices to 
  * some values, and which can be concatenated with other such functions such that they 
  * now cover an expanded index range, and the "right" side function has had its domain
  * shifted.  An index function is defined on an index interval with a particular width,
  * and its domain is some subset of that interval.
  *
  * @tparam V The type of value returned by this function.
  */
abstract class IndexFunction[V] extends PartialFunction[Int, V] with Serializable { self =>

  /** The width of the integer index interval that contains the [[domain]].  All indices this
    * function is defined on are on the interval [0, width)
    *
    * @return The width of the interval [0, width) containing the function [[domain]].
    */
  def width: Int

  /** Obtain a new iterator over the indices that the function is defined on.
    *
    * @return An iterator over the function domain
    */
  def domain: Iterator[Int]

  /** Obtain a new iterator over the function range values
    *
    * @return An iterator over values that may be returned by the function
    */
  def range: Iterator[V]

  /** Obtain a Scala Map from values in the [[domain]] to their corresponding [[range]] values.
    *
    * @return A Scala Map that is equivalent to this function
    */
  final def toMap: Map[Int, V] = {
    val m = scala.collection.mutable.Map.empty[Int, V]
    domain.foreach { j =>
      m += (j -> this(j))
    }
    m.toMap
  }

  /** Concatenate two index functions.
    *
    * @param that The right-hand function of the concatenation
    * @return The concatenation of left and right index functions, defined as:
    * {{{
    * (f1 ++ f2)(j) = if (j < f1.width) f1(j) else f2(j - f1.width)
    * }}}
    */
  final def ++(that: IndexFunction[V]) = new IndexFunction[V] {
    def width = self.width + that.width
    def domain = self.domain ++ that.domain.map(_ + self.width)
    private lazy val rangeLazy = {
      val uniq = scala.collection.mutable.Set.empty[V]
      (self.range ++ that.range).foreach { v =>
         uniq += v
      }
      uniq
    }
    def range = rangeLazy.iterator
    def apply(j: Int) = if (j < self.width) self.apply(j) else that.apply(j - self.width)
    def isDefinedAt(j: Int) = if (j < self.width) self.isDefinedAt(j) else that.isDefinedAt(j - self.width)
  }
}

/** The inverse of an [[InvertableIndexFunction]].
  *
  * @tparam V The [[domain]] type of this function.  Also the
  * [[InvertableIndexFunction!.range range]] of the parent [[InvertableIndexFunction]].
  */
abstract class InverseIndexFunction[V] extends PartialFunction[V, Int] with Serializable {

  /** The width of index range. Corresponds to the [[InvertableIndexFunction!.width width]]
    * of the parent [[InvertableIndexFunction]].
    */
  def width: Int

  /** Obtain a new iterator over the domain of this function.
    *
    * @return An iterator over the domain values of this function.  Equivalent to the range of the
    * parent [[InvertableIndexFunction]].
    */
  def domain: Iterator[V]

  /** Obtain a new iterator over the range values of this function.
    *
    * @return An iterator over the range values of this function.  Equivalent to the domain of
    * the parent [[InvertableIndexFunction]].
    */
  final def range: Iterator[Int] = domain.map(this)

  /** Obtain a Scala Map that is equivalent to this function.
    *
    * @return A Map that maps domain values into their corresponding index values
    */
  final def toMap: Map[V, Int] = {
    val m = scala.collection.mutable.Map.empty[V, Int]
    domain.foreach { v =>
      m += (v -> this(v))
    }
    m.toMap
  }
}

/** An [[IndexFunction]] that is also invertable (and so 1-1).
  *
  * @tparam V The [[range]] value type of this function.
  */
abstract class InvertableIndexFunction[V] extends IndexFunction[V] { self =>

  /** Obtain the inverse of this function.
    *
    * @return The inverse function that maps [[range]] values to their correponding indices
    */
  def inverse: InverseIndexFunction[V]

  final def range = domain.map(this)

  /** Concatenate two invertable index functions.
    *
    * @note The [[range]] values of the argument functions must be disjoint, in order to
    * preserve the invertability of their concatenation.
    *
    * @param that The right-hand function of the concatenation
    * @return The concatenation of left and right index functions, defined as:
    * {{{
    * (f1 ++ f2)(j) = if (j < f1.width) f1(j) else f2(j - f1.width)
    * }}}
    */
  final def ++(that: InvertableIndexFunction[V]) = {
    // check up front that ranges are disjoint, such that concatenation is invertable
    val iself = self.inverse
    val ithat = that.inverse
    val cv = findCommon(iself, ithat)
    if (!cv.isEmpty) throw new Exception(s"Value ${cv.get} present in both ranges")

    new InvertableIndexFunction[V] {
      def width = self.width + that.width
      def domain = self.domain ++ that.domain.map(_ + self.width)
      def apply(j: Int) = if (j < self.width) self.apply(j) else that.apply(j - self.width)
      def isDefinedAt(j: Int) = if (j < self.width) self.isDefinedAt(j) else that.isDefinedAt(j - self.width)
      def inverse = lazyInverse
      private lazy val lazyInverse = new InverseIndexFunction[V] {
        def width = self.width + that.width
        def domain = iself.domain ++ ithat.domain
        def apply(v: V) = if (iself.isDefinedAt(v)) iself(v) else self.width + ithat(v)
        def isDefinedAt(v: V) = iself.isDefinedAt(v) || ithat.isDefinedAt(v)
      }
    }
  }

  private def findCommon(fi1: InverseIndexFunction[V], fi2: InverseIndexFunction[V]): Option[V] = {
    // might consider supporting some kind of heuristics on how to do this fastest, but
    // not sure how much low hanging fruit there is in that direction
    // this ought to be O(n log n) or better for the structures I have in mind
    fi1.domain.find(fi2.isDefinedAt(_))
  }
}

/** Factory methods for generating index functions */
object IndexFunction {

  /** Obtain an empty index function.
    *
    * @return An index function over the empty interval [0, 0), defined over no values
    */
  def empty[V] = undefined[V](0)

  /** Obtain an index function that is undefined over its interval
    *
    * @param wid The function's index interval width
    * @return An index function that is not defined over any indices on its interval [0, wid)
    */
  def undefined[V](wid: Int): IndexFunction[V] = {
    require(wid >= 0)
    new IndexFunction[V] {
      var dummy: V = _
      def width = wid
      def domain: Iterator[Int] = Iterator.empty
      def range: Iterator[V] = Iterator.empty
      def isDefinedAt(j: Int) = false
      def apply(j: Int) = dummy
    }
  }

  /** Obtain an index function that is constant over its interval
    *
    * @param v The constant value
    * @param wid The interval width
    * @return an index function that returns (v) over all index values [0, wid)
    */
  def constant[V](v: V, wid: Int): IndexFunction[V] = {
    require(wid >= 0)
    new IndexFunction[V] {
      def width = wid
      def domain = (0 until wid).iterator
      def range = rangeLazy.iterator
      private lazy val rangeLazy = if (width > 0) List(v) else List()
      def isDefinedAt(j: Int) = (j >= 0)  &&  (j < wid)
      def apply(j: Int) = v
    }
  }

  /** Create an index function from a Scala IndexedSeq
    *
    * @param vals The indexed sequence of values
    * @return An index function over interval [0, vals.length) where f(j) = vals(j)
    */
  def apply[V](vals: IndexedSeq[V]): IndexFunction[V] = new IndexFunction[V] {
    def width = vals.length
    def domain = (0 until width).iterator
    def range = rangeLazy.iterator
    private lazy val rangeLazy = vals.distinct
    def isDefinedAt(j: Int) = (j >= 0)  &&  (j < width)
    def apply(j: Int) = vals(j)
  }

  /** Create an index function from ordered pairs (j0, v0), (j1, v1), ...
    *
    * @param wid The index interval width
    * @param pairs Zero or more pairs (j0, v0), (j1, v1), ...
    * @return An index function with [[domain]] { j0, j1, ...} a subset of [0, wid), where
    * f(j0) = v0, f(j1 = v1), ...  and undefined elsewhere
    */
  def apply[V](wid: Int, pairs: (Int, V)*): IndexFunction[V] = this(wid, Map(pairs:_*))

  /** Create an index function from a Scala Map of indices to values
    *
    * @param wid The index inverval width
    * @param map The Scala Map
    * @return An index function with [[domain]] equivalent to map.keysIterator, and subset of 
    * [0, wid), where f(j) = map(j), for any j defined on the map, and undefined elsewhere
    */
  def apply[V](wid: Int, map: Map[Int, V]): IndexFunction[V] = new IndexFunction[V] {
    require(wid >= 0)
    require(!map.keysIterator.exists(k => (k < 0  ||  k >= wid)))
    def width = wid
    def domain = map.keysIterator
    def range = rangeLazy.iterator
    def isDefinedAt(j: Int) = (j >= 0) && (j < wid) && map.isDefinedAt(j)
    def apply(j: Int) = map(j)
    private lazy val rangeLazy = map.valuesIterator.toVector
  }
}

object InvertableIndexFunction {
  /** Obtain an empty invertable index function.
    *
    * @return An invertable index function over the empty interval [0, 0), defined over no values
    */
  def empty[V] = undefined[V](0)

  /** Obtain an invertable index function that is undefined over its interval
    *
    * @param wid The function's index interval width
    * @return An invertable index function that is not defined over any indices on its
    * interval [0, wid)
    */
  def undefined[V](wid: Int): InvertableIndexFunction[V] = {
    require(wid >= 0)
    new InvertableIndexFunction[V] {
      var dummy: V = _
      def width = wid
      def domain: Iterator[Int] = Iterator.empty
      def isDefinedAt(j: Int) = false
      def apply(j: Int) = dummy
      def inverse = lazyInverse
      private lazy val lazyInverse = new InverseIndexFunction[V] {
        def width = wid
        def domain: Iterator[V] = Iterator.empty
        def isDefinedAt(v: V) = false
        def apply(v: V) = 0
      }
    }
  }

  /** Create an invertable index function from a Scala IndexedSeq
    *
    * @param vals The indexed sequence of unique values
    * @return An invertable index function over interval [0, vals.length) where f(j) = vals(j)
    */
  def apply[V](vals: IndexedSeq[V]): InvertableIndexFunction[V] = {
    checkRange(vals.iterator)
    val v2i = v2iMap(vals)
    new InvertableIndexFunction[V] {
      def width = vals.length
      def domain = (0 until width).iterator
      def isDefinedAt(j: Int) = (j >= 0)  &&  (j < width)
      def apply(j: Int) = vals(j)
      def inverse = lazyInverse
      private lazy val lazyInverse = new InverseIndexFunction[V] {
        def width = vals.length
        def domain = vals.iterator
        def isDefinedAt(v: V) = v2i.isDefinedAt(v)
        def apply(v: V) = v2i(v)
      }
    }
  }

  /** Create an invertable index function from ordered pairs (j0, v0), (j1, v1), ...
    *
    * @param wid The index interval width
    * @param pairs Zero or more pairs (j0, v0), (j1, v1), ...
    * @return An invertable index function with [[domain]] { j0, j1, ...} a subset of [0, wid),
    * where f(j0) = v0, f(j1 = v1), ...  and undefined elsewhere
    */
  def apply[V](wid: Int, pairs: (Int, V)*): InvertableIndexFunction[V] = this(wid, Map(pairs:_*))

  /** Create an invertable index function from a Scala Map of indices to values
    *
    * @param wid The index inverval width
    * @param map The Scala Map
    * @return An invertable index function with [[domain]] equivalent to map.keysIterator, and
    * subset of [0, wid), where f(j) = map(j), for any j defined on the map, and undefined elsewhere
    */
  def apply[V](wid: Int, map: Map[Int, V]): InvertableIndexFunction[V] = {
    checkRange(map.valuesIterator)
    new InvertableIndexFunction[V] { self =>
      require(wid >= 0)
      require(!map.keysIterator.exists(k => (k < 0  ||  k >= wid)))
      def width = wid
      def domain = map.keysIterator
      def isDefinedAt(j: Int) = (j >= 0) && (j < wid) && map.isDefinedAt(j)
      def apply(j: Int) = map(j)
      def inverse = inverseLazy
      private lazy val inverseLazy = new InverseIndexFunction[V] {
        val imap = Map(self.domain.map(d => (map(d), d)).toSeq:_*)
        def width = wid
        def domain = imap.keysIterator
        def isDefinedAt(v: V) = imap.isDefinedAt(v)
        def apply(v: V) = imap(v)
      }
    }
  }

  // if baseName == "foo", then f(5) => "foo5"
  // f.inverse("foo5") => 5
  def serialName(baseName: String, wid: Int) = {
    require(wid >= 0)
    new InvertableIndexFunction[String] { self =>
      def width = wid
      def domain = (0 until width).iterator
      def isDefinedAt(j: Int) = (j >= 0)  &&  (j < width)
      def apply(j: Int) = baseName + j.toString
      def inverse = lazyInverse
      private lazy val lazyInverse = new InverseIndexFunction[String] {
        val regex = (baseName + """(\d+)""").r
        def width = wid
        def domain = self.domain.map(self)
        def isDefinedAt(v: String) = v match {
          case regex(n) => ((n.head != '0')  ||  (n.length == 1)) && (n.toInt < width)
          case _ => false
        }
        def apply(v: String) = v match {
          case regex(n) => n.toInt
        }
      }
    }
  }

  private def checkRange[V](range: Iterator[V]) {
    val uniq = scala.collection.mutable.Set.empty[V]
    range.foreach { v =>
      if (uniq.contains(v)) throw new Exception(s"Range contains non-unique value $v")
      uniq += v
    }
  }

  private def v2iMap[V](vals: IndexedSeq[V]): Map[V, Int] = {
    val m = scala.collection.mutable.Map.empty[V, Int]
    var j = 0
    vals.foreach { v =>
      m += (v -> j)
      j += 1
    }
    m.toMap
  }
}
