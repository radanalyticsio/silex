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

import com.redhat.et.silex.app.ConsoleApp

import org.scalatest._

private[frame] case class Example1(a: Int, b: Int, c: Int) {}
private[frame] case class Example2(a: Int, b: Int, d: Int) {}
private[frame] case class Example3(a: Int, e: Int) {}
private[frame] case class Example4(f: Int) {}

class NatJoinSpec extends FlatSpec with Matchers with BeforeAndAfterEach {
  private var app: ConsoleApp = null

  override def beforeEach() {
    app = new ConsoleApp()
    System.clearProperty("spark.master.port")
    
    app.context
  }
  
  override def afterEach() {
    app.context.stop
  }

  it should "produce an empty frame after joining two frames with no columns in common" in {
    val sqlc = app.sqlContext
    import sqlc.implicits._
    
    val frame1 = app.context.parallelize((1 to 10).map { i => Example1(i, i * 2, i * 3)}).toDF()
    val frame2 = app.context.parallelize((1 to 10).map { i => Example4(i)}).toDF()
    
    assert(NaturalJoin.natjoin(frame1, frame2).collect.length == 0)    
  }
  
  it should "produce an empty frame after joining two frames with no values in common" in {
    val sqlc = app.sqlContext
    import sqlc.implicits._
    
    val frame1 = app.context.parallelize((1 to 10).map { i => Example1(i, i * 2, i * 3)}).toDF()
    val frame2 = app.context.parallelize((100 to 150).map { i => Example2(i, i * 2, i * 3)}).toDF()
    
    assert(NaturalJoin.natjoin(frame1, frame2).collect.length == 0)
  }
  
}
