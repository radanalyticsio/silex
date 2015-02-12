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

package com.redhat.et.silex.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

trait AppCommon {
  private var exitHooks: List[() => Unit] = List(() => this.context.stop)
  private var configHooks: List[SparkConf => SparkConf] = Nil
    
  private lazy val _conf = { 
    val initialConf = new SparkConf()
     .setMaster(master)
     .setAppName(appName)
     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    configHooks.reverse.foldLeft(initialConf) {(c, f) => f(c)}
  }
    
  private lazy val _context = { 
    new SparkContext(_conf)
  }

  private lazy val _sqlContext = {
    new org.apache.spark.sql.SQLContext(context)
  }
  
  def master = sys.env.getOrElse("SPARK_MASTER", "local[8]")
  def appName: String
    
  def main(args: Array[String]) = {
    appMain(args)
    runExitHooks
  }
    
  def addConfig(xform: SparkConf => SparkConf) {
    configHooks = xform :: configHooks
  }
    
  def addExitHook(thunk: => Unit) {
    exitHooks = {() => thunk} :: exitHooks
  }
    
  def runExitHooks() {
    for (hook <- exitHooks) {
      hook()
    }
  }
    
  def appMain(args: Array[String]): Unit
  
  def context: SparkContext = _context

  def sqlContext = _sqlContext
}

class ConsoleApp extends AppCommon { 
  override def appName = "console"
  def appMain(args: Array[String]) {
    // this never runs
  }
}
