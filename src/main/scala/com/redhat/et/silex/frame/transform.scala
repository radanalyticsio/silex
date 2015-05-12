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

package com.redhat.et.silex.frame.transform

import org.apache.spark.rdd._

import org.json4s._
import org.json4s.jackson.JsonMethods._

object JSONTransformer {
  def transform(data: RDD[String], transformer: PartialFunction[JValue, JValue]): RDD[String] = {
    data.map { recordString =>
      val jsonAST = parse(recordString)
      val transformedAST = jsonAST.transform(transformer)
      compact(render(transformedAST))
    }
  }
}

