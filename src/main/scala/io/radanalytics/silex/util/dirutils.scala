/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2014, 2016 Red Hat, Inc.
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

package io.radanalytics.silex.util

object DirUtils {
  import scala.util.Try
  
  def maybeReaddir(dirname: String, flt: String => Boolean = (_ => true)): Option[Array[String]] = {
    val dirOption = Try(new java.io.File(dirname).getCanonicalFile).toOption
    dirOption
      .filter { dir => dir.exists && dir.isDirectory }
      .map { dir => dir.listFiles collect { case (file) if flt(file.getName) => file.getCanonicalPath } }
  }
  
  def readdir(dirname: String, flt: String => Boolean = (_ => true)): Array[String] = {
    maybeReaddir(dirname, flt).getOrElse(Array())
  }
}

