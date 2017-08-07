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

package io.radanalytics.silex.feature.extractor.spark

import io.radanalytics.silex.feature.extractor.FeatureSeq

import org.apache.spark.mllib.linalg.{
  Vector => SV,
  DenseVector => DenseSV,
  SparseVector => SparseSV
}

/** Subclass of [[FeatureSeq]] for supporting Spark vector data */
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

/** Implicit conversions from Spark vectors to [[FeatureSeq]], and vice versa.
  * {{{
  * import io.radanalytics.silex.feature.extractor.{ FeatureSeq, Extractor }
  * import io.radanalytics.silex.feature.extractor.spark
  * import io.radanalytics.silex.feature.extractor.spark.implicits._
  * import org.apache.spark.mllib.linalg.DenseVector
  * import org.apache.spark.mllib.regression.LabeledPoint
  *
  * val sv = new DenseVector(Array(1.0, 2.0))
  * val featureSeq = FeatureSeq(sv)
  * val sv2 = featureSeq.toSpark
  *
  * val label = 1.0
  * val lp = new LabeledPoint(label, sv)
  * val fs2 = FeatureSeq(lp)
  * val lp2 = fs2.toLabeledPoint(label)
  * }}}
  */
object implicits {
  import scala.language.implicitConversions
  import org.apache.spark.mllib.regression.LabeledPoint

  /** Convert Spark vectors to [[FeatureSeq]] */
  implicit def fromSVtoSparkFS(v: SV): FeatureSeq = new SparkFS(v)

  /** Convert Spark LabeledPoint objects to [[FeatureSeq]] */
  implicit def fromLPtoSparkFS(p: LabeledPoint) = new SparkFS(p.features)

  /** Provides [[toSpark]] and [[toLabeledPoint]] enriched methods on [[FeatureSeq]] */
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
