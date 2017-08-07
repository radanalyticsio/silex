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
 * limitations under the License
 */
package io.radanalytics.silex.rdd.split

import org.scalatest._

import io.radanalytics.silex.testing.PerTestSparkContext

class SplitRDDFunctionsSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import io.radanalytics.silex.testing.matchers._
  import io.radanalytics.silex.rdd.split.implicits._

  it should "provide splitFilter method" in {
    val data = Vector(1, 0, 2, 0, 3, 0, 4, 0, 5, 0)
    val rdd = context.parallelize(data, 2)
    val (pass, fail) = rdd.splitFilter(_ > 0)
    pass.collect.toSeq should beEqSeq(Seq(1, 2, 3, 4, 5))
    fail.collect.toSeq should beEqSeq(Seq(0, 0, 0, 0, 0))
  }

  it should "provide splitEither method" in {
    val data = Vector(1, 0, 2, 0, 3, 0, 4, 0, 5, 0)
    val rdd = context.parallelize(data, 2)
    val (left, right) = rdd.splitEither { e =>
      (if (e > 0) Right(e) else Left(e.toString)) :Either[String, Int] }
    left.collect.toSeq should beEqSeq(Seq("0", "0", "0", "0", "0"))
    right.collect.toSeq should beEqSeq(Seq(1, 2, 3, 4, 5))
  }
}
