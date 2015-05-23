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

package com.redhat.et.silex.sample.iid

import org.apache.spark.rdd.RDD
import com.redhat.et.silex.feature.extractor.FeatureSeq

/** Interface for enriched i.i.d. feature sampling methods on sequence-like collections of
  * feature vectors.  A feature vector is some sequential collection of Double values, whose
  * values may be i.i.d sampled to generate new synthetic feature vectors having the same
  * marginal distributions as the input features.  The underlying feature vector representation
  * is not assumed by this interface; multiple representations might be supported
  */
abstract class IIDFeatureSamplingMethods extends Serializable {
  /** Generate a new synthetic RDD whose rows are i.i.d sampled from input feature vectors
    */
  def iidFeatureSeqRDD(
      n: Int,
      iSS: Int = 1000,
      oSS: Int = 1000
      ): RDD[FeatureSeq]
}

object implicits {
  import scala.language.implicitConversions

  implicit def fromRDDtoIIDFSM(data: RDD[Seq[Double]]): IIDFeatureSamplingMethods =
    new rdd.IIDFeatureSamplingMethodsRDD(data)
}
