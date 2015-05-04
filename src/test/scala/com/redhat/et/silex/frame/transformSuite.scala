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

import com.redhat.et.silex.testing.PerTestSparkContext

import org.scalatest._

import org.json4s._
import org.json4s.jackson.JsonMethods._

class TransformerSpec extends FlatSpec with Matchers with PerTestSparkContext {
  private[frame] val carsExample = JArray(List(
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

  private[frame] val carsDescriptionExample = JArray(List(
    JObject(List(
      JField("make", JString("Jeep")),
      JField("model", JString("Grand Cherokee")),
      JField("color", JString("silver")),
      JField("year", JInt(2005)),
      JField("description", JString("Jeep Grand Cherokee - Silver - 2005")))),
    JObject(List(
      JField("make", JString("Ford")),
      JField("model", JString("Fiesta")),
      JField("color", JString("blue")),
      JField("year", JInt(2011)),
      JField("description", JString("Ford Fiesta - Blue - 2011"))))
  ))


  "JSONTransformer" should "convert years to numbers and add description fields" in {
    val exampleString = compact(render(carsExample))
  }
}

