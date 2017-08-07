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

package io.radanalytics.silex.frame

import io.radanalytics.silex.testing.{PerTestSparkContext, TempDirFixtures}

import org.scalatest._

case class NumberBox(i: Int) {}

class FrameDirSpec extends FlatSpec with Matchers with TempDirFixtures with PerTestSparkContext {
  import org.apache.spark.sql.Row
  
  it should "successfully load the right number of records from a directory of Parquet files" in {
    val sqlc = sqlContext
    import sqlc.implicits._
    
    val frames = (1 to 10) map { i => (i, context.parallelize((1 to 100) map {j => NumberBox(i * j)}).toDF) }
    frames foreach { case (id, frame) =>
      frame.write.parquet("%s/test%d.parquet".format(tempPath, id))
    }
    
    val union = frames map { case (_, frame) => frame } reduce { (l, r) => l union r }
    val loaded = FrameDir.loadDir(sqlc, tempPath)
    
    assert(union.count() == loaded.count())
    assert(union.distinct().join(loaded.distinct(), loaded("i") === union("i")).count() == loaded.distinct().count())
  }
}
