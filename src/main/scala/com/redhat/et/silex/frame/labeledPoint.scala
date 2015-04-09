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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

trait VectorExtracting {
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.Vectors.{dense => denseVec}
  import org.apache.spark.mllib.linalg.{Vector => SparkVec}
  import org.apache.spark.sql.{Column, Row}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  /**
   * Converts a data frame to an [[RDD]] of [[SparkVec]]s, using the values in the specified columns.
   */
  def toDenseVectors(df: DataFrame, col: String, cols: String*): RDD[SparkVec] = {
    toDenseVectors(df, (col :: cols.toList).map(cn => df(cn)) : _*)
  }
  
  /**
   * Converts a data frame to an [[RDD]] of [[SparkVec]]s, using the values in the specified columns.
   */
  def toDenseVectors(df: DataFrame, columns: Column*): RDD[SparkVec] = {
    val castedColumns = columns.map { c => c.cast(DoubleType) }
    df.select(castedColumns : _*).map { case r: Row => 
      denseVec(Array((0 until r.length).map { pos => r.getDouble(pos)} : _*))
    }
  }
  
  /**
   * Converts a data frame to an [[RDD]] of [[LabeledPoint]]s, taking labels and vector values from the specified columns.
   */
  def toLabeledPoints(df: DataFrame, labelCol: String, vecCol: String, vecCols: String*): RDD[LabeledPoint] = {
    toLabeledPoints(df, df(labelCol), (vecCol :: vecCols.toList).map(cn => df(cn)) : _*)
  }
  
  /**
   * Converts a data frame to an [[RDD]] of [[LabeledPoint]]s, taking labels and vector values from the specified columns.
   */
  def toLabeledPoints(df: DataFrame, labelCol: Column, vecCols: Column*): RDD[LabeledPoint] = {
    val features = toDenseVectors(df, vecCols : _*)
    val labels = df.select(labelCol.cast(DoubleType)).map { case Row(label: Double) => label }
    labels.zip(features).map { 
      case (label, vec) => LabeledPoint(label, vec)
    }
  }
}

private[frame] case class DFWithVectorExtracting(df: DataFrame) extends VectorExtracting {
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.{Vector => SparkVec}
  import org.apache.spark.sql.Column

  def toDenseVectors(c: String, cs: String*): RDD[SparkVec] = super.toDenseVectors(df, c, cs : _*)
  def toDenseVectors(cs: Column*): RDD[SparkVec] = super.toDenseVectors(df, cs : _*)
  def toLabeledPoints(label: String, vc: String, vcs: String*): RDD[LabeledPoint] = 
    super.toLabeledPoints(df, label, vc, vcs : _*)
  
  def toLabeledPoints(label: Column, vcs: Column*): RDD[LabeledPoint] = 
    super.toLabeledPoints(df, label, vcs : _*)
}

object FrameToVector extends VectorExtracting {
  /** Import the contents of <code>implicits</code> to have all of the methods of [[VectorExtracting]] available on any [[DataFrame]] */
  object implicits {
    implicit def toVectorExtracting(df: DataFrame) = DFWithVectorExtracting(df)
  }
}