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

object implicits {
  import scala.language.implicitConversions
  implicit def fromBVtoBreezeFS(bv: BV[Double]): FeatureSeq = new BreezeFS(bv)
  implicit class enrichBreezeFS(val fs: FeatureSeq) extends AnyVal {
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
