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

package com.redhat.et.silex.frame.json

import org.apache.spark.rdd._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.implicitConversions

/**
  * Utility methods for massaging JSON-encoded data.
  */
trait JSONTransformer {

  /**
    * Apply the given PartialFunction to values in serialized, JSON records.  New
    * records are returned with the original values replaced by the results of the 
    * PartialFunction for values on which the PartialFunction matched. See unit the
    * the for examples.
    */
  def transform(data: RDD[String], transformer: PartialFunction[JValue, JValue]): RDD[String] = {
    data.map { recordString =>
      val jsonAST = parse(recordString)
      val transformedAST = jsonAST.transform(transformer)
      compact(render(transformedAST))
    }
  }

  /**
    * Apply the given PartialFunction to object fields in serialized, JSON records.
    * New records are returned with the original fields replaced by the results of the 
    * PartialFunction for fields on which the PartialFunction matched. See unit the
    * the for examples.
    */
  def transformField(data: RDD[String], transformer: PartialFunction[JField, JField]): RDD[String] = {
    data.map { recordString =>
      val jsonAST = parse(recordString)
      val transformedAST = jsonAST.transformField(transformer)
      compact(render(transformedAST))
    }
  }
}

private[json] case class RDDWithTransform(data: RDD[String]) extends JSONTransformer {
  /**
    * Apply the given PartialFunction to values in serialized, JSON records.  New
    * records are returned with the original values replaced by the results of the 
    * PartialFunction for values on which the PartialFunction matched. See unit the
    * the for examples.
    */
  def transform(transform: PartialFunction[JValue, JValue]): RDD[String] =
    super.transform(data, transform)

  /**
    * Apply the given PartialFunction to object fields in serialized, JSON records.
    * New records are returned with the original fields replaced by the results of the 
    * PartialFunction for fields on which the PartialFunction matched. See unit the
    * the for examples.
    */
  def transformField(transform: PartialFunction[JField, JField]): RDD[String] =
    super.transformField(data, transform)
}

object JSONTransform extends JSONTransformer {
  object implicits {
    implicit def rddWithTransform(data: RDD[String]) = RDDWithTransform(data)
  }
}
