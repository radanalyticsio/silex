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

package io.radanalytics.silex.som

import io.radanalytics.silex.testing.{PerTestSparkContext, TempDirFixtures}

import org.scalatest._

class SomSpec extends FlatSpec with Matchers with TempDirFixtures with PerTestSparkContext {
  it should "train a SOM against a data frame" in {
    val sqlc = sqlContext
    import sqlc.implicits._
    
    Example.applyDF(20, 20, 20, sqlc.sparkContext, 100000)
  }
}
