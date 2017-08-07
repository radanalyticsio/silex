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

package io.radanalytics.silex.testing

import org.scalatest._

import io.radanalytics.silex.app.TestConsoleApp

trait AbstractBeforeAndAfter extends BeforeAndAfterEach {
  self: BeforeAndAfterEach with Suite =>
  override def beforeEach() {}
  override def afterEach() {}
}

trait PerTestSparkContext extends AbstractBeforeAndAfter {
  self: BeforeAndAfterEach with Suite =>
  
  private var app: TestConsoleApp = null
  
  def context = app.context
  def sqlContext = app.sqlContext
  
  override def beforeEach() {
    super.beforeEach()
    app = new TestConsoleApp()
    System.clearProperty("spark.master.port")
    
    app.sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    
    app.context
  }
  
  override def afterEach() {
    super.afterEach()
    app.context.stop
  }
}