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

package io.radanalytics.silex.rdd.json

import io.radanalytics.silex.rdd.json.JSONTransform.implicits._
import io.radanalytics.silex.testing.PerTestSparkContext

import org.scalatest._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.sql._

class JSONTransformerSpec extends FlatSpec with Matchers with PerTestSparkContext {
  private[json] val carsStringExample = JArray(List(
    JObject(List(
      JField("make", JString("Jeep")),
      JField("model", JString("Grand Cherokee")),
      JField("color", JString("silver")),
      JField("year", JString("2005")))),
    JObject(List(
      JField("make", JString("Ford")),
      JField("model", JString("Fiesta")),
      JField("color", JString("blue")),
      JField("year", JString("2011"))))
  ))

  private[json] val carsIntExample = JArray(List(
    JObject(List(
      JField("make", JString("Jeep")),
      JField("model", JString("Grand Cherokee")),
      JField("color", JString("silver")),
      JField("year", JInt(2005)))),
    JObject(List(
      JField("make", JString("Ford")),
      JField("model", JString("Fiesta")),
      JField("color", JString("blue")),
      JField("year", JInt(2011))))
  ))


  "JSONTransformer.transform" should "convert years to numbers" in {
    val exampleString = compact(render(carsStringExample))
    val exampleInt = compact(render(carsIntExample))

    val exampleStringRDD = context.parallelize((List(exampleString)))
    val exampleIntRDD = context.parallelize((List(exampleInt)))

    val transformer: PartialFunction[JValue, JValue] = 
    { case jo: JObject =>
      jo.transformField {
        case JField("year", orig @ JString(year)) =>
          JField("year", scala.util.Try(year.toInt).map(JInt(_)).getOrElse(orig))
      }
    }

    val transformedRDD = exampleStringRDD.transform(transformer)

    assert(transformedRDD.collect().toSet == exampleIntRDD.collect().toSet)
  }

  "JSONTransformer.transformField" should "convert years to numbers" in {
    val exampleString = compact(render(carsStringExample))
    val exampleInt = compact(render(carsIntExample))

    val exampleStringRDD = context.parallelize((List(exampleString)))
    val exampleIntRDD = context.parallelize((List(exampleInt)))

    val transformer: PartialFunction[JField, JField] = 
    {   case JField("year", orig @ JString(year)) =>
          JField("year", scala.util.Try(year.toInt).map(JInt(_)).getOrElse(orig))
    }

    val transformedRDD = exampleStringRDD.transformField(transformer)

    assert(transformedRDD.collect().toSet == exampleIntRDD.collect().toSet)
  }
  
  "JSONTransformer" should "not conflict with Spark SQL's use of JSON" in {
    val sqlc = SparkSession.builder.master(context.master).getOrCreate().sqlContext
    
    import sqlc.implicits._
    
    val df = context.parallelize(1 to 100).toDF("i")
    df.collect()
  }
}

