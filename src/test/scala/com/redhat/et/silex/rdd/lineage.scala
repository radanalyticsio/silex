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
package io.radanalytics.silex.rdd.lineage

import org.scalatest._

import io.radanalytics.silex.testing.PerTestSparkContext

class LineageRDDFunctionsSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import io.radanalytics.silex.testing.matchers._
  import io.radanalytics.silex.rdd.lineage.implicits._

  it should "iterate correctly over lineage" in {
    val rdd1 = context.parallelize(1 to 10)
    val rdd2 = context.parallelize(1 to 10)
    val rdd3 = rdd1 ++ rdd2
    val rdd4 = rdd1 ++ rdd3
    rdd1.name = "rdd1"
    rdd2.name = "rdd2"
    rdd3.name = "rdd3"
    rdd4.name = "rdd4"

    val lineage = rdd4.lineage.toVector

    lineage.map(_.rdd.name) should beEqSeq(Seq("rdd1", "rdd3", "rdd1", "rdd2"))
    lineage.map(_.depth) should beEqSeq(Seq(1, 1, 2, 2))
    lineage.map(_.successor.name) should beEqSeq(Seq("rdd4", "rdd4", "rdd3", "rdd3"))
  }
}
