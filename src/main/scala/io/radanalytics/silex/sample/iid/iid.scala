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

package io.radanalytics.silex.sample.iid

import org.apache.spark.rdd.RDD

import io.radanalytics.silex.feature.extractor.FeatureSeq

/** Interface for enriched iid feature sampling methods on sequence-like collections of
  * feature vectors.  A feature vector is some sequential collection of Double values, whose
  * values may be iid sampled to generate new synthetic feature vectors having the same
  * marginal distributions as the input features.  The underlying feature vector representation
  * is not assumed by this interface; multiple representations might be supported
  *
  * {{{
  * import io.radanalytics.silex.sample.iid.implicits._
  *
  * rdd.iidFeatureSeqRDD(100000)
  * }}}
  */
abstract class IIDFeatureSamplingMethods extends Serializable {
  /** Generate a new synthetic RDD whose rows are iid sampled from input feature vectors
    *
    * @param n The number of iid samples to generate.
    * @param iSS The input sample size.  Input is periodically sampled and 
    * the sample is used to generate iid output data. Defaults to 10000.
    * @param oSS The output sample size.  Each input sample is used to generate this number
    * of output samples. Defaults to 10000.
    * @return An RDD of [[FeatureSeq]] where each 'column' in the feature sequence is statistically
    * independent of the others, but shares the marginal distribution of the corresponding
    * input column.
    */
  def iidFeatureSeqRDD(
      n: Int,
      iSS: Int = 10000,
      oSS: Int = 10000
      ): RDD[FeatureSeq]
}

/** Implicit conversions to support enriched iid feature sampling methods.
  * {{{
  * import io.radanalytics.silex.sample.iid.implicits._
  *
  * rdd.iidFeatureSeqRDD(100000)
  * }}}
  */
object implicits {
  import scala.language.implicitConversions

  // to-do: add support for Scala sequence i.i.d. sampling
  implicit def fromRDDtoIIDFSM(data: RDD[Seq[Double]]): IIDFeatureSamplingMethods =
    new rdd.IIDFeatureSamplingMethodsRDD(data)
}
