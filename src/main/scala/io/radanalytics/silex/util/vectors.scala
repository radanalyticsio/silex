/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2016 Red Hat, Inc.
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

package io.radanalytics.silex.util.vectors

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{
  Vector => SparkVector,
  DenseVector => DenseSV,
  SparseVector => SparseSV
}

import breeze.linalg.{
  Vector => BreezeVector,
  DenseVector => DenseBV,
  SparseVector => SparseBV,
  HashVector => HashBV
}

import io.radanalytics.silex.feature.extractor.FeatureSeq

/** Implicit method enhancements for vector library interoperability */
object implicits {
  /** provides conversions from various Seq[Double] subclasses to Spark vectors */
  implicit class enrichSeqConversions(@transient seq: Seq[Double]) extends Serializable {

    /** Convert a sequence of doubles to an equivalent Spark Vector.  Conversion is aware of
      * whether underlying sequence types are sparse or dense.
      * @return A new Spark Vector equivalent to the input sequence
      */
    def toSpark: SparkVector = {
      seq match {
        case fs: FeatureSeq if (fs.density < 0.5) => new SparseSV(
          fs.length,
          fs.activeKeysIterator.toArray,
          fs.activeValuesIterator.toArray)
        case bv: SparseBV[_] => new SparseSV(
          bv.length,
          bv.activeKeysIterator.toArray,
          bv.asInstanceOf[SparseBV[Double]].activeValuesIterator.toArray)
        case bv: HashBV[_] => new SparseSV(
          bv.length,
          bv.activeKeysIterator.toArray,
          bv.asInstanceOf[SparseBV[Double]].activeValuesIterator.toArray)
        case _ => new DenseSV(seq.toArray)
      }
    }

    /** Convert a sequence and a given label to an equivalent Spark LabeledPoint object.
      * Conversion is aware of whether underlying sequence types are sparse or dense.
      * @param lab A label to use for the LabeledPoint object
      * @returns A new LabeledPoint object whose feature data is equivalent to the input sequence
      */
    def toLabeledPoint(lab: Double) = new LabeledPoint(lab, this.toSpark)
  }

  /** Conversions of Spark Vector object into other formats  */
  implicit class enrichSparkVecConversions(@transient sv: SparkVector) extends Serializable {
    /** Use a given label to create a new LabeledPoint object from a Spark Vector
      * @param lab A label to use for the LabeledPoint object
      * @return A new LabeledPoint object using the input Spark Vector for feature data
      */
    def toLabeledPoint(lab: Double) = new LabeledPoint(lab, sv)
  }
}
