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

package com.redhat.et.silex.frame

import com.redhat.et.silex.testing.PerTestSparkContext

import org.scalatest._

private[frame] case class Example1(a: Int, b: Int, c: Int) {}
private[frame] case class Example2(a: Int, b: Int, d: Int) {}
private[frame] case class Example3(a: Int, e: Int) {}
private[frame] case class Example4(f: Int) {}

class NatJoinSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import org.apache.spark.sql.Row
  
  it should "produce an empty frame after joining two frames with no columns in common" in {
    val sqlc = sqlContext
    import sqlc.implicits._
    
    val frame1 = context.parallelize((1 to 10).map { i => Example1(i, i * 2, i * 3)}).toDF()
    val frame2 = context.parallelize((1 to 10).map { i => Example4(i)}).toDF()
    
    assert(NaturalJoin.natjoin(frame1, frame2).collect.length == 0)    
  }
  
  it should "produce an empty frame after joining two frames with no values in common" in {
    val sqlc = sqlContext
    import sqlc.implicits._
    
    val frame1 = context.parallelize((1 to 10).map { i => Example1(i, i * 2, i * 3)}).toDF()
    val frame2 = context.parallelize((100 to 150).map { i => Example2(i, i * 2, i * 3)}).toDF()
    
    assert(NaturalJoin.natjoin(frame1, frame2).collect.length == 0)
  }
  
  it should "produce frames with the appropriate schema after joining" in {
    val sqlc = sqlContext
    import sqlc.implicits._
    
    val frame1 = context.parallelize((1 to 10).map { i => Example1(i, i * 2, i * 3)}).toDF()
    val frame2 = context.parallelize((1 to 10).map { i => Example2(i, i * 2, i * 4)}).toDF()
    val join = NaturalJoin.natjoin(frame1, frame2)
    
    assert(join.columns.length == 4)
    assert(join.columns.toSet == Set("a", "b", "c", "d"))
  }
  
  it should "produce frames with the appropriate values after joining" in {
    val sqlc = sqlContext
    import sqlc.implicits._
    
    val frame1 = context.parallelize((1 to 9).map { i => Example1(i, i * 2, i * 3)}).toDF()
    val frame2 = context.parallelize((1 to 12).map { i => Example2(i, i * 2, i * 4)}).toDF()
    val join = NaturalJoin.natjoin(frame1, frame2)
    
    assert(join.count == 9)
    join.collect.foreach {
      case Row(a: Int, b: Int, c: Int, d: Int) => {
        assert(d == a * 4 && c == a * 3 && b == a * 2)
      }
    }
  }
  
  it should "work whether using NaturalJoin or implicitly enriched DataFrame" in {
    val sqlc = sqlContext
    import sqlc.implicits._
    import NaturalJoin.implicits._
    
    val frame1 = context.parallelize((1 to 9).map { i => Example1(i, i * 2, i * 3)}).toDF()
    val frame2 = context.parallelize((1 to 12).map { i => Example2(i, i * 2, i * 4)}).toDF()
    val join = frame1.natjoin(frame2)
    
    assert(join.count == 9)
    join.collect.foreach {
      case Row(a: Int, b: Int, c: Int, d: Int) => {
        assert(d == a * 4 && c == a * 3 && b == a * 2)
      }
    }
  }
}
