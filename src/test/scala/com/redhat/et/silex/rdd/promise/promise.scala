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

package com.redhat.et.silex.rdd.promise

import com.redhat.et.silex.testing.PerTestSparkContext

import org.scalatest._

import org.apache.spark.{ TaskContext }

class PromiseRDDSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import com.redhat.et.silex.rdd.promise.implicits._

  it should "construct a simple promise" in {
    // a promise RDD having no RDD dependencies
    val rdd = new PromiseRDD((ctx: TaskContext) => 42, context, Nil)
    rdd.collect().toSeq should equal (Seq(42))
  }

  it should "promise a sum" in {
    val data = context.parallelize(1 to 100)
    val rdd = data.promiseFromPartitions((s:Seq[Iterator[Int]]) => s.map(_.sum).sum)
    rdd.collect().toSeq should equal (Seq(5050))
  }
}
