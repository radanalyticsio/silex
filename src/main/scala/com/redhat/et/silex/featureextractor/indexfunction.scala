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

package com.redhat.et.silex.indexfunction

import scala.language.implicitConversions

class IteratorWrapper[V](self: Iterator[V]) {
  def ++(that: Iterator[V]) = new Iterator[V] {
    def hasNext = self.hasNext || that.hasNext
    def next: V = {
      if (self.hasNext) self.next else that.next
    }
  }
}

object IteratorWrapper {
  implicit def toIteratorWrapper[V](iter: Iterator[V]) = new IteratorWrapper(iter)
}

import IteratorWrapper._

// An index function is a (partial) function from some subset of collection indices to 
// some values, and which can be concatenated with other such functions such that they 
// now cover an expanded index range, and the "right" side function has had its domain
// shifted.  An index function is defined on an index interval with a particular width,
// and its domain is some subset of that interval
abstract class IndexFunction[V] extends PartialFunction[Int, V] with Serializable { self =>
  def width: Int
  def domain: Iterator[Int]
  def range: Iterator[V]

  final def toMap: Map[Int, V] = {
    val m = scala.collection.mutable.Map.empty[Int, V]
    domain.foreach { j =>
      m += (j -> this(j))
    }
    m.toMap
  }

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

abstract class InverseIndexFunction[V] extends PartialFunction[V, Int] with Serializable {
  def width: Int
  def domain: Iterator[V]

  final def range: Iterator[Int] = domain.map(this)

  final def toMap: Map[V, Int] = {
    val m = scala.collection.mutable.Map.empty[V, Int]
    domain.foreach { v =>
      m += (v -> this(v))
    }
    m.toMap
  }
}

abstract class InvertableIndexFunction[V] extends IndexFunction[V] { self =>
  def inverse: InverseIndexFunction[V]

  final def range = domain.map(this)

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


object IndexFunction {
  def empty[V] = undefined[V](0)

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

  def apply[V](vals: IndexedSeq[V]): IndexFunction[V] = new IndexFunction[V] {
    def width = vals.length
    def domain = (0 until width).iterator
    def range = rangeLazy.iterator
    private lazy val rangeLazy = vals.distinct
    def isDefinedAt(j: Int) = (j >= 0)  &&  (j < width)
    def apply(j: Int) = vals(j)
  }

  def apply[V](wid: Int, pairs: (Int, V)*): IndexFunction[V] = this(wid, Map(pairs:_*))

  def apply[V](wid: Int, map: Map[Int, V]): IndexFunction[V] = new IndexFunction[V] {
    require(wid >= 0)
    def width = wid
    def domain = map.keysIterator.filter(k => (k >= 0  &&  k < wid))
    def range = rangeLazy.iterator
    def isDefinedAt(j: Int) = (j >= 0) && (j < wid) && map.isDefinedAt(j)
    def apply(j: Int) = map(j)
    private lazy val rangeLazy = domain.map(map).toSeq.distinct
  }
}

object InvertableIndexFunction {
  def empty[V] = undefined[V](0)

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

  def apply[V](values: IndexedSeq[V]): InvertableIndexFunction[V] = {
    val vals: IndexedSeq[V] = values.distinct
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

  def apply[V](wid: Int, pairs: (Int, V)*): InvertableIndexFunction[V] = this(wid, Map(pairs:_*))

  def apply[V](wid: Int, map: Map[Int, V]): InvertableIndexFunction[V] = {
    checkRange(map.valuesIterator)
    new InvertableIndexFunction[V] {
      require(wid >= 0)
      def width = wid
      def domain = map.keysIterator
      def isDefinedAt(j: Int) = (j >= 0) && (j < wid) && map.isDefinedAt(j)
      def apply(j: Int) = map(j)
      def inverse = inverseLazy
      private lazy val inverseLazy = new InverseIndexFunction[V] {
        val imap = Map(map.iterator.map(_.swap).toSeq:_*)
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
