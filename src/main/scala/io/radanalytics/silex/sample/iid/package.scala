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

package io.radanalytics.silex.sample

/** A package that provides 'iid' sampling functionality on various collections whose elements
  * constitute some kind of feature vector representation, eg Seq, [[FeatureSeq]], etc.  The
  * acronym 'iid' stands for 'independent, identically distributed', and refers to the property
  * that sampled columns share the identical marginal distribution with corresponding input
  * colums, but are statistically independent of any other columns, which is not necessarily true
  * of the input.
  * {{{
  * import io.radanalytics.silex.sample.iid.implicits._
  *
  * rdd.iidFeatureSeqRDD(100000)
  * }}}
  */
package object iid {}
