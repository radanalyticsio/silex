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

package com.redhat.et.silex.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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
    if(java.lang.System.getProperty("spark.master", null) == null) {
      java.lang.System.setProperty("spark.master", master)
    }
    
    val initialConf = new SparkConf()
     .setAppName(appName)
     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .set("spark.kryoserializer.buffer", "16mb")

    configHooks.reverse.foldLeft(initialConf) {(c, f) => f(c)}
  }
  
  private lazy val _session = {
    SparkSession.builder.config(_conf).getOrCreate()
  }

  private lazy val _context = { 
    _session.sparkContext
  }

  private lazy val _sqlContext = {
    _session.sqlContext
  }
  
  /** The Spark master URL.
    *
    * This defaults to the value set in <code>SPARK_MASTER</code> in the environment or <code>local[*]</code>.  The <code>spark.master</code> property will take precedence over this. */
  def master = sys.env.getOrElse("SPARK_MASTER", "local[*]")
  
  /** A method that returns your application's name */
  def appName: String
  
  /** The main method for your application.  Do not override this; override [[AppCommon.appMain]] instead */
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
  
  /** Adds a thunk to be run when your application exits.
   *
   * These are executed in the order that they are added.
   */
  def addExitHook(thunk: => Unit) {
    exitHooks = {() => thunk} :: exitHooks
  }
    
  private def runExitHooks() {
    for (hook <- exitHooks.reverse) {
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
  
  def spark: SparkSession = _session

  def context: SparkContext = _context

  def sqlContext = _sqlContext
}

class ConsoleApp extends AppCommon { 
  override def appName = "console"
  def appMain(args: Array[String]) {
    // this never runs
  }
}

private [silex] class TestConsoleApp(val suppliedMaster: String = "local[2]") extends AppCommon { 
  override def master = suppliedMaster
  override def appName = "console"
  
  addConfig( {(conf: SparkConf) => conf.set("spark.kryoserializer.buffer", "2mb")})
  
  def appMain(args: Array[String]) {
    // this never runs
  }
}

trait ReplAppLike {
  import scala.tools.nsc.interpreter._
  import scala.tools.nsc.Settings
  
  def makeApp: AppCommon = new com.redhat.et.silex.app.ConsoleApp()
  
  def main(args: Array[String]) {
    val repl = new ILoop {
      override def createInterpreter(): Unit = {
	super.createInterpreter()
        val app = makeApp
        intp.interpret("import org.apache.spark.SparkConf")
        intp.interpret("import org.apache.spark.SparkContext")
        intp.interpret("import org.apache.spark.SparkContext._")
        intp.interpret("import org.apache.spark.rdd.RDD")
        
        intp.interpret("import org.apache.spark.sql.DataFrame")
        intp.interpret("import org.apache.spark.sql.functions._")
        intp.interpret("import org.apache.spark.sql.types._")
        
        intp.bind("app", app)
        intp.bind("spark", app.spark)
        intp.bind("sc", app.context)
        intp.bind("sqlc", app.sqlContext)
        intp.interpret("import sqlc._")
        intp.interpret("import sqlc.implicits._")
      }
    }
    
    val settings = new Settings
    settings.Yreplsync.value = true
    
    settings.usejavacp.value = true
    // settings.embeddedDefaults[ReplApp.type]
    
    repl.process(settings)
  }
}

object ReplApp extends ReplAppLike {}
