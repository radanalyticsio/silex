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

package com.redhat.et.silex.frame

import com.redhat.et.silex.app.TestConsoleApp

import org.scalatest._

private[frame] case class LPExample1(label: Double, v1: Double, v2: Double) {}
private[frame] case class LPExample2(a: Int, b: Int, c: Int) {}

class LabeledPointSpec extends FlatSpec with Matchers with BeforeAndAfterEach {
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  
  import org.apache.spark.mllib.regression.LabeledPoint
  
  private var app: TestConsoleApp = null

  override def beforeEach() {
    app = new TestConsoleApp()
    app.sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    
    System.clearProperty("spark.master.port")
    
    app.context
  }
  
  override def afterEach() {
    app.context.stop
  }

  it should "construct vectors from RDDs with Double-valued vector columns" in {
    val sqlc = app.sqlContext
    import sqlc.implicits._
    
    val df = app.context.parallelize((1 to 100).map { i => LPExample1(i * 1.0d, i * 2.0d, i * 4.0d)}).toDF()
    val lps = FrameToVector.toDenseVectors(df, "v1", "v2")
    
    assert(lps.count() == 100)
    
    lps.collect.foreach { vec => {
        assert(vec(1) == vec(0) * 2.0d)
      }
    }
  }
  
  it should "construct vectors from RDDs with Int-valued vector columns" in {
    val sqlc = app.sqlContext
    import sqlc.implicits._
    
    val df = app.context.parallelize((1 to 100).map { i => LPExample2(i, i * 2, i * 4)}).toDF()
    val lps = FrameToVector.toDenseVectors(df, "a", "b")
    
    assert(lps.count() == 100)
    
    lps.collect.foreach { vec => {
        assert(vec(1) == vec(0) * 2.0d)
      }
    }
  }
  
  it should "construct labeled points from RDDs with Double-valued label and vector columns" in {
    val sqlc = app.sqlContext
    import sqlc.implicits._
    
    val df = app.context.parallelize((1 to 100).map { i => LPExample1(i * 1.0d, i * 2.0d, i * 4.0d)}).toDF()
    val lps = FrameToVector.toLabeledPoints(df, "label", "v1", "v2")
    
    assert(lps.count() == 100)
    
    lps.collect.foreach {
      case LabeledPoint(l, vec) => {
        assert(vec(0) == l * 2.0d)
        assert(vec(1) == l * 4.0d)
      }
    }
  }
  
}
