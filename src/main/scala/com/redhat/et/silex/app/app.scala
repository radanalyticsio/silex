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

import scala.reflect.runtime.universe._


/** Base trait for data-processing applications. 
  *
  * Contains references to Spark and SQL contexts, a basic configuration, and callback interfaces for app configuration and cleanup.
  */
trait AppCommon {
  private var exitHooks: List[() => Unit] = List(() => this.context.stop)
  private var configHooks: List[SparkConf => SparkConf] = Nil
  private var functionRegistry: Map[String, Any] = Map[String, Any]()
  private lazy val _conf = { 
    val initialConf = new SparkConf()
     .setMaster(master)
     .setAppName(appName)
     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .set("spark.kryoserializer.buffer.mb", "256")

    configHooks.reverse.foldLeft(initialConf) {(c, f) => f(c)}
  }
    
  private lazy val _context = { 
    new SparkContext(_conf)
  }

  private lazy val _sqlContext = {
    new org.apache.spark.sql.SQLContext(context)
  }
  
  /** The Spark master URL; defaults to the value set in <code>SPARK_MASTER</code> in the environment or <code>local[*]</code> */
  def master = sys.env.getOrElse("SPARK_MASTER", "local[*]")
  
  /** A method that returns your application's name */
  def appName: String
    
  def main(args: Array[String]) = {
    appMain(args)
    runExitHooks
  }
  
  /** 
    * Adds Spark configuration (in the form of a function that takes a <code>SparkConf</code> and returns a new <code>SparkConf</code>.)
    *
    * These are executed in the order that they are added.
    */

  def addConfig(xform: SparkConf => SparkConf) {
    configHooks = xform :: configHooks
  }
  
  /** Adds a thunk to be run when your application exits */
  def addExitHook(thunk: => Unit) {
    exitHooks = {() => thunk} :: exitHooks
  }
    
  private def runExitHooks() {
    for (hook <- exitHooks) {
      hook()
    }
  }
  
  def isFunctionRegistered(fun: String) = functionRegistry.isDefinedAt(fun)
  
  /** Registers a function with the given name.  
    *
    * Subsequent attempts to register a function with the same name will have no effect. */
  def registerUnaryFunction[A : TypeTag, B : TypeTag](fn: String, fb: A => B) {
    if (! isFunctionRegistered(fn) ) {
      val rf = sqlContext.udf.register(fn, fb)
      functionRegistry = functionRegistry + (fn -> fb)
    }
  }
  
  /** Override this to provide a <code>main</code> method for your app. */
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

object ReplApp {
  import scala.tools.nsc.interpreter._
  import scala.tools.nsc.Settings
  
  def main(args: Array[String]) {
    val repl = new ILoop {
      override def loop(): Unit = {
        val app = new com.redhat.et.silex.app.ConsoleApp()
        intp.addImports("org.apache.spark.SparkConf")
        intp.addImports("org.apache.spark.SparkContext")
        intp.addImports("org.apache.spark.SparkContext._")
        intp.addImports("org.apache.spark.rdd.RDD")
        
        intp.addImports("org.apache.spark.sql.DataFrame")
        intp.addImports("org.apache.spark.sql.functions._")
        intp.addImports("org.apache.spark.sql.types._")
        
        intp.bind("app", app)
        intp.bind("spark", app.context)
        intp.bind("sqlc", app.sqlContext)
        intp.addImports("sqlc._")
        intp.addImports("sqlc.implicits._")
        
        super.loop()
      }
    }
    
    val settings = new Settings
    settings.Yreplsync.value = true
    
    settings.usejavacp.value = true
    // settings.embeddedDefaults[ReplApp.type]
    
    repl.process(settings)
  }
}
