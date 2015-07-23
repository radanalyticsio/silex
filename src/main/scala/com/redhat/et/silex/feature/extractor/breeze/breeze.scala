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

package com.redhat.et.silex.feature.extractor.breeze

import com.redhat.et.silex.feature.extractor.FeatureSeq

import _root_.breeze.linalg.{
  Vector => BV,
  DenseVector => DenseBV,
  SparseVector => SparseBV,
  HashVector => HashBV
}

/** Subclass of [[FeatureSeq]] for supporting Breeze Vector data */
sealed class BreezeFS(bv: BV[Double]) extends FeatureSeq {
  def length = bv.length
  def apply(j: Int) = bv(j)
  def iterator = bv.valuesIterator
  def activeKeysIterator = bv.activeKeysIterator
  def activeValuesIterator = bv.activeValuesIterator
  def density = bv match {
    case v: SparseBV[_] if (v.length > 0) => v.activeSize.toDouble / v.length.toDouble 
    case v: HashBV[_] if (v.length > 0) => v.activeSize.toDouble / v.length.toDouble 
    case _ => 1.0
  }
  override def toString = s"BreezeFS(${bv})"
}

/** Implicit conversions from Breeze vectors to [[FeatureSeq]].
  *
  * {{{
  * import com.redhat.et.silex.feature.extractor.{ FeatureSeq, Extractor }
  * import com.redhat.et.silex.feature.extractor.breeze
  * import com.redhat.et.silex.feature.extractor.breeze.implicits._
  * import _root_.breeze.linalg.DenseVector
  *
  * val bv = new DenseVector(Array(1.0, 2.0))
  * val featureSeq = FeatureSeq(bv)
  * val bv2 = featureSeq.toBreeze
  * }}}
  */
object implicits {
  import scala.language.implicitConversions

  /** Convert Breeze vectors to [[FeatureSeq]] */
  implicit def fromBVtoBreezeFS(bv: BV[Double]): FeatureSeq = new BreezeFS(bv)

  /** Supply [[toBreeze]] enriched method on [[FeatureSeq]] objects */
  implicit class enrichBreezeFS(@transient fs: FeatureSeq) extends Serializable {
    def toBreeze: BV[Double] = {
      if (fs.density > 0.5) new DenseBV(fs.toArray)
      else {
        new SparseBV(
         fs.activeKeysIterator.toArray,
         fs.activeValuesIterator.toArray,
         fs.length)
      }
    }
  }
}
