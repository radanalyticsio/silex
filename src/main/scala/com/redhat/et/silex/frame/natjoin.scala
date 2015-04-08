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

import org.apache.spark.sql.DataFrame

trait NaturalJoining {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._  
  
  def natjoin(left: DataFrame, right: DataFrame): DataFrame = {
    val leftCols = left.columns
    val rightCols = right.columns

    val commonCols = leftCols.toSet intersect rightCols.toSet

    left
      .join(right, commonCols.map {col => left(col) === right(col) }.reduce(_ && _))
      .select( leftCols.filter { c => commonCols.contains(c) }.map { c => left(c) } ++ 
               leftCols.filter { c => !commonCols.contains(c) }.map { c => left(c) } ++ 
               rightCols.filter { c => !commonCols.contains(c) }.map { c => right(c) } : _*)
  }
}