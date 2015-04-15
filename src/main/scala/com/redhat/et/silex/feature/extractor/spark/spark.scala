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

package com.redhat.et.silex.feature.extractor.spark

import com.redhat.et.silex.feature.extractor.FeatureSeq

import org.apache.spark.mllib.linalg.{
  Vector => SV,
  DenseVector => DenseSV,
  SparseVector => SparseSV
}

sealed class SparkFS(v: SV) extends FeatureSeq {
  def length = v.size
  def apply(j: Int) = v(j)
  def iterator = v match {
    case w: DenseSV => w.values.iterator 
    case _ => Iterator.range(0, length).map(j=>v(j))
  }
  def activeKeysIterator = v match {
    case w: SparseSV => w.indices.iterator
    case _ => Iterator.range(0, length)
  }
  def activeValuesIterator = v match {
    case w: SparseSV => w.values.iterator
    case _ => iterator    
  }
  def density = v match {
    case w: SparseSV if (length > 0) => w.indices.length / length.toDouble 
    case _ => 1.0
  }
  override def toString = s"SparkFS(${v})"
}

object implicits {
  import scala.language.implicitConversions
  import org.apache.spark.mllib.regression.LabeledPoint
  implicit def fromSVtoSparkFS(v: SV): FeatureSeq = new SparkFS(v)
  implicit def fromLPtoSparkFS(p: LabeledPoint) = new SparkFS(p.features)
  implicit class enrichSparkFS(@transient fs: FeatureSeq) extends Serializable {
    def toSpark: SV = {
      if (fs.density > 0.5) new DenseSV(fs.toArray)
      else {
        new SparseSV(
          fs.length,
          fs.activeKeysIterator.toArray,
          fs.activeValuesIterator.toArray)
      }
    }
    def toLabeledPoint(label: Double) = new LabeledPoint(label, toSpark)
  }
}
