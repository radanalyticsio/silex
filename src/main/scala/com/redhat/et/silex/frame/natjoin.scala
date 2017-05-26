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

package com.redhat.et.silex.frame

import org.apache.spark.sql.DataFrame
import scala.language.implicitConversions

trait NaturalJoining {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._  
  
  /**
   * Performs a natural join of two data frames.
   *
   * The frames are joined by equality on all of the columns they have in common.
   * The resulting frame has the common columns (in the order they appeared in <code>left</code>), 
   * followed by the columns that only exist in <code>left</code>, followed by the columns that 
   * only exist in <code>right</code>.
   */
  def natjoin(left: DataFrame, right: DataFrame): DataFrame = {
    val leftCols = left.columns
    val rightCols = right.columns

    val commonCols = leftCols.toSet intersect rightCols.toSet
    
    if(commonCols.isEmpty)
      left.limit(0).crossJoin(right.limit(0))
    else
      left
        .join(right, commonCols.map {col => left(col) === right(col) }.reduce(_ && _))
        .select(leftCols.collect { case c if commonCols.contains(c) => left(c) } ++ 
                leftCols.collect { case c if !commonCols.contains(c) => left(c) } ++ 
                rightCols.collect { case c if !commonCols.contains(c) => right(c) } : _*)
  }
}

private[frame] case class DFWithNatJoin(df: DataFrame) extends NaturalJoining {
  def natjoin(other: DataFrame): DataFrame = super.natjoin(df, other)
}

/** 
 * Module for natural join functionality.  Import <code>NaturalJoin._</code> for static access 
 * to the <code>natjoin</code> method, or import <code>NaturalJoin.implicits._</code> to pimp 
 * Spark DataFrames with a <code>natjoin</code> member method. 
 */
object NaturalJoin extends NaturalJoining {  
  object implicits {
    implicit def dfWithNatJoin(df: DataFrame) = DFWithNatJoin(df)
  }
}
