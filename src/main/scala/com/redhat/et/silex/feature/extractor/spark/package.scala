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

package com.redhat.et.silex.feature.extractor

/** Provides conversions from Spark vectors to [[FeatureSeq]], and vice versa.
  * {{{
  * import com.redhat.et.silex.feature.extractor.{ FeatureSeq, Extractor }
  * import com.redhat.et.silex.feature.extractor.spark
  * import com.redhat.et.silex.feature.extractor.spark.implicits._
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
package object spark {}
