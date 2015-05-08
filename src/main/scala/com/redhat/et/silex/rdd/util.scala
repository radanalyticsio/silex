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

package com.redhat.et.silex.rdd

private[silex] object util {
  import org.apache.commons.lang3.reflect.{ FieldUtils, MethodUtils }
  import org.apache.spark.SparkContext

  def clean[F <: AnyRef](sc: SparkContext, f: F): F = {
    val r = MethodUtils.invokeMethod(sc, "clean", f.asInstanceOf[Object], true.asInstanceOf[Object])
    r.asInstanceOf[F]
  }
}

