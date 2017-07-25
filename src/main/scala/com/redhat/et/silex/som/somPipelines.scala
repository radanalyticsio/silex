/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2017 Red Hat, Inc.
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

package com.redhat.et.silex.som

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.hacks.Hacks
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DataType, StructType, StructField}

private[som] trait SOMParams extends Params with DefaultParamsWritable /* with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol */ { 
  final val x: IntParam = new IntParam(this, "x", "width of self-organizing map (>= 1)", ParamValidators.gtEq(1))
  final val y: IntParam = new IntParam(this, "y", "height of self-organizing map (>= 1)", ParamValidators.gtEq(1))

  final def getX: Int = $(x)
  final def getY: Int = $(y)
  
  final def setX(value: Int): this.type = set(x, value)
  final def setY(value: Int): this.type = set(y, value)
  final def setEpochs(value: Int): this.type = set(epochs, value)
  final def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  final def setPredictionCol(value: String): this.type = set(predictionCol, value)
  final def setProbabilityCol(value: String): this.type = set(probabilityCol, value)
  final def setSeed(value: Int): this.type = set(seed, value)

  /**
   * Param for number of epochs (&gt;= 0).
   * @group param
   */
  final val epochs: IntParam = new IntParam(this, "epochs", "number of epochs (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getEpochs: Int = $(epochs)
  
  /**
   * Param for features column name.
   * @group param
   */
  final val featuresCol: Param[String] = new Param[String](this, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  /** @group getParam */
  final def getFeaturesCol: String = $(featuresCol)
  
  /**
   * Param for prediction column name.
   * @group param
   */
  final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "prediction column name")

  setDefault(predictionCol, "prediction")

  /** @group getParam */
  final def getPredictionCol: String = $(predictionCol)
  
  /**
   * Param for Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities.
   * @group param
   */
  final val probabilityCol: Param[String] = new Param[String](this, "probabilityCol", "Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities")

  setDefault(probabilityCol, "probability")

  /** @group getParam */
  final def getProbabilityCol: String = $(probabilityCol)
  
  /**
   * Param for random seed.
   * @group param
   */
  final val seed: IntParam = new IntParam(this, "seed", "random seed")

  setDefault(seed, this.getClass.getName.hashCode.toInt)

  /** @group getParam */
  final def getSeed: Int = $(seed)
  
  setDefault(x, 8)
  setDefault(y, 8)
  setDefault(epochs, 10)
  
  
  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // we want the input column to exist...
    require(schema.fieldNames.contains($(featuresCol)))
    
    // ...and to be the proper type
    schema($(featuresCol)) match {
      case sf: StructField => require(sf.dataType.equals(Hacks.vectorUDT))
    }
    
    // but we don't want the output columns to exist
    require(!schema.fieldNames.contains($(predictionCol)))
    require(!schema.fieldNames.contains($(probabilityCol)))
    
    schema.add($(predictionCol), "int").add($(probabilityCol), "double")
  }
}

class SOMModel(override val uid: String, private val model: SOM) extends Model[SOMModel] with SOMParams {
  // Members declared in org.apache.spark.ml.Model
  override def copy(extra: org.apache.spark.ml.param.ParamMap): com.redhat.et.silex.som.SOMModel = ???
  
  // Members declared in org.apache.spark.ml.PipelineStage
  def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = {
    validateAndTransformSchema(schema)
  }
  
  // Members declared in org.apache.spark.ml.Transformer
  def transform(dataset: org.apache.spark.sql.Dataset[_]): org.apache.spark.sql.DataFrame = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.linalg.{Vector => SV}
    transformSchema(dataset.schema, logging = true)
    val predictUDF = udf((vector: SV) => model.closestWithSimilarity(vector, None))
    dataset
      .withColumn($(predictionCol), predictUDF(col($(featuresCol))))
      .select(dataset.columns.map(column(_)) ++ Seq(column($(predictionCol) + "._1").as($(predictionCol)), column($(predictionCol) + "._2").as($(probabilityCol))) : _*)
  }
}

class SOMEstimator(override val uid: String) extends Estimator[SOMModel] with SOMParams {
  import org.apache.spark.ml.linalg.{Vector => SV}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row
  
  def this() = this(Identifiable.randomUID("silex-som"))
  
  override def copy(extra: org.apache.spark.ml.param.ParamMap): org.apache.spark.ml.Estimator[com.redhat.et.silex.som.SOMModel] = ???
  def fit(dataset: org.apache.spark.sql.Dataset[_]): com.redhat.et.silex.som.SOMModel = {
    
    transformSchema(dataset.schema)
    
    val examples: RDD[SV] = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: SV) => point
    }
    
    val fdim = examples.take(1)(0).size
    
    val model = new SOMModel(uid, SOM.train($(x), $(y), fdim, $(epochs), examples: RDD[SV], Some($(seed))))
    model.setParent(this)
    
    model
  }
    
  // Members declared in org.apache.spark.ml.PipelineStage
  def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = {
    validateAndTransformSchema(schema)
  }
}