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

package com.redhat.et.silex.featureextractor

import com.redhat.et.silex.indexfunction._

sealed abstract class FeatureVector(sz: Int) extends Serializable {
  // abstract methods
  private[featureextractor] def set(i: Int, v: Double): Unit
  private[featureextractor] def setAll(beg: Int, vList: TraversableOnce[Double]): Unit
  private[featureextractor] def get(j: Int): Double

  private var nxt = 0
  private var beg = 0
  private var end = 0
  private[featureextractor] def release() {
    beg = end
    nxt = end
  }
  private[featureextractor] def reserve(r: Int) {
    require(r >= 0)
    end = beg + r
    require(end <= this.size)
  }

  final def size: Int = sz
  final def length: Int = sz

  final def pad(width: Int) {
    require(width >= 0)
    nxt += width
  }

  final def +=(v: Double) {
    require(nxt < end)
    set(nxt, v)
    nxt += 1
  }

  final def +=(v1: Double, v2: Double, vRest: Double*) {
    require((nxt + 2 + vRest.length) <= end)
    set(nxt, v1)
    nxt += 1
    set(nxt, v2)
    nxt += 1
    setAll(nxt, vRest)
    nxt += vRest.length
  }

  final def append(vList: Double*) {
    require(nxt + vList.length <= end)
    setAll(nxt, vList)
    nxt += vList.length
  }

  final def ++=(vList: TraversableOnce[Double]) {
    require(vList.hasDefiniteSize && ((nxt + vList.size) <= end))
    setAll(nxt, vList)
    nxt += vList.size
  }

  final def appendAll(vList: TraversableOnce[Double]) {
    require(vList.hasDefiniteSize && ((nxt + vList.size) <= end))
    setAll(nxt, vList)
    nxt += vList.size
  }

  final def apply(j: Int): Double = get(j)
}

private[featureextractor] class DenseVec(val sz: Int) extends FeatureVector(sz) {
  val data = Array.fill[Double](sz)(0.0)
  def set(i: Int, v: Double) { data(i) = v }
  def setAll(beg: Int, vList: TraversableOnce[Double]) {
    var j = beg
    vList.foreach { v =>
      data(j) = v
      j += 1
    }
  }
  def get(j: Int) = data(j)
  override def toString = "[" + data.map(_.toString).mkString(",") + "]"
}

private[featureextractor] class SparseVec(val sz: Int) extends FeatureVector(sz) {
  val idx = scala.collection.mutable.ArrayBuffer.empty[Int]
  val data = scala.collection.mutable.ArrayBuffer.empty[Double]
  def set(i: Int, v: Double) {
    if (v != 0.0) {
      idx += i
      data += v
    }
  }
  def setAll(beg: Int, vList: TraversableOnce[Double]) {
    var j = beg
    vList.foreach { v =>
      if (v != 0.0) {
        idx += j
        data += v
      }
      j += 1
    }
  }
  def get(j: Int) = {
    val n = idx.length
    if (n <= 4) {
      val jj = idx.indexOf(j)
      if (jj < 0) 0.0 else data(jj)
    } else {
      var b = 0
      var e = n
      var v = 0.0
      while (b < e) {
        val m = (b+e)/2
        if (j == idx(m)) {
          v = data(m)
          e = b
        } else if (j < idx(m)) {
          e = m
        } else {
          b = m + 1
        }
      }
      v
    }
  }
  override def toString =
    "[" + idx.zip(data).map(e => e._1.toString + " -> " + e._2.toString).mkString(", ") + "]"
}

object DenseFeatureVector {
  def unapply(vec: DenseVec): Option[Array[Double]] = Some(vec.data)
}
object SparseFeatureVector {
  def unapply(vec: SparseVec): Option[(Int, Array[Int], Array[Double])] =
    Some((vec.sz, vec.idx.toArray, vec.data.toArray))
}

// a feature extractor is a function from some domain D to an intermediate vector
// representation that (a) supports sparse and dense representations and (b) is easily 
// converted to 3rd-party representations such as Spark mllib vectors.
abstract class FeatureExtractor[D] extends Function[D, FeatureVector] with Serializable { self =>
  // abstract methods
  def size: Int
  def density: Double
  def fill(data: D, fv: FeatureVector): Unit

  // by default, feature name function is empty - defined on no indexes
  private lazy val lazyNames = InvertableIndexFunction.undefined[String](size)
  def names: InvertableIndexFunction[String] = lazyNames

  // By default, no category information is supplied.  If used with Spark MLLib, this
  // signifies "no features are categorical"
  // May be overridden if desired to supply an explicit category information map,
  // to a feature extractor that doesn't have one, or to override its default
  private lazy val lazyCatInfo = IndexFunction.undefined[Int](size)
  def categoryInfo: IndexFunction[Int] = lazyCatInfo

  // this is overridden for concatenated extractors, which are not atomic
  private[featureextractor] def atomic = true

  // use this to plug an extractor into custom shim/adaptor function
  // featureExtractor compose shimFunction
  final override def compose[G](g: G => D) = new FeatureExtractor[G] {
    def size = self.size
    def density = self.density
    def fill(data: G, fv: FeatureVector) { self.fill(g(data), fv) }
    override def atomic = self.atomic
    override def categoryInfo = self.categoryInfo
    override def names = self.names
  }

  final def apply(data: D): FeatureVector = {
    val fv = if (this.density < 0.5) new SparseVec(this.size) else new DenseVec(this.size)
    if (this.atomic) fv.reserve(this.size)
    this.fill(data, fv)
    fv
  }

  // this is where you can concatenate extractors
  final def ++(that: FeatureExtractor[D]) = new FeatureExtractor[D] {
    def size = self.size + that.size
    def density = densityLazy
    def fill(data: D, fv: FeatureVector) {
      if (self.atomic) fv.reserve(self.size)
      self.fill(data, fv)
      if (self.atomic) fv.release()

      if (that.atomic) fv.reserve(that.size)
      that.fill(data, fv)
      if (that.atomic) fv.release()
    }
    override def atomic = false
    override def categoryInfo = self.categoryInfo ++ that.categoryInfo
    override def names = self.names ++ that.names
    private lazy val densityLazy = {
      val selfSize = self.size.toDouble
      val thatSize = that.size.toDouble
      val z = selfSize + thatSize
      if (z > 0.0) ((selfSize * self.density) + (thatSize * that.density)) / z else 1.0
    }
  }

  // A way to augment an existing feature extractor with a categoryInfo map, or
  // to override its existing map
  final def withCategoryInfo(info: IndexFunction[Int]): FeatureExtractor[D] =
    new FeatureExtractor[D] {
    require(info.width == self.size)
    def size = self.size
    def density = self.density
    def fill(data: D, fv: FeatureVector) { self.fill(data, fv) }
    override def atomic = self.atomic
    override def categoryInfo = info
    override def names = self.names
  }
  final def withCategoryInfo(pairs: (String, Int)*): FeatureExtractor[D] = {
    val n2i = self.names.inverse
    val cif = IndexFunction(
      self.size,
      pairs.filter(p => n2i.isDefinedAt(p._1)).map(p => (n2i(p._1), p._2)):_*)
    self.withCategoryInfo(cif)
  }

  final def withNames(nf: InvertableIndexFunction[String]): FeatureExtractor[D] = new FeatureExtractor[D] {
    require(nf.width == self.size)
    def size = self.size
    def density = self.density
    def fill(data: D, fv: FeatureVector) { self.fill(data, fv) }
    override def atomic = self.atomic
    override def categoryInfo = self.categoryInfo
    override def names = nf
  }
  final def withNames(fnames: String*): FeatureExtractor[D] = {
    withNames(InvertableIndexFunction(fnames.toVector))
  }
}

// defines various feature extraction functions
// intent is to make definition of feature extraction component functions easy, and easy to compose
object FeatureExtractor {
  // the empty feature extractor
  def empty[D] = new FeatureExtractor[D] {
    def size = 0
    def density = 1.0
    def fill(data: D, fv: FeatureVector) {}
  }

  def constant[D](vList: Double*) = new FeatureExtractor[D] {
    def size = vList.length
    def density = 1.0
    def fill(data: D, fv: FeatureVector) {
      fv ++= vList
    }
  }

  // apply zero or more functions to some data object
  def apply[D](fList: (D => Double)*) = new FeatureExtractor[D] {
    def size = fList.length
    def density = 1.0
    def fill(data: D, fv: FeatureVector) {
      fList.foreach { f =>
        fv += f(data)
      }
    }
  }

  // select zero or more numeric values by index, cast to Double
  def numeric[N :Numeric](jList: Int*) = new FeatureExtractor[PartialFunction[Int, N]] {
    def size = jList.length
    def density = 1.0
    def fill(data: PartialFunction[Int, N], fv: FeatureVector) {
      val num = implicitly[Numeric[N]]
      jList.foreach { j =>
        fv += num.toDouble(data(j))
      }
    }
  }

  // select zero or more string values by index, cast to Double
  def string(jList: Int*) = new FeatureExtractor[PartialFunction[Int, String]] {
    def size = jList.length
    def density = 1.0
    def fill(data: PartialFunction[Int, String], fv: FeatureVector) {
      jList.foreach { j =>
        fv += data(j).toDouble
      }
    }
  }

  // load an entire sequence of numeric values, with expected size and density
  def numericSeq[N :Numeric](sz: Int, rho: Double) = new FeatureExtractor[Seq[N]] {
    def size = sz
    def density = rho
    def fill(data: Seq[N], fv: FeatureVector) {
      require(data.length == sz)
      val num = implicitly[Numeric[N]]
      fv ++= data.view.map(v => num.toDouble(v))
    }
  }
}
