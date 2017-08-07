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

package io.radanalytics.silex.feature.extractor.spark

import org.scalatest._

class SparkFeatureSeqSpec extends FlatSpec with Matchers {
  import io.radanalytics.silex.feature.extractor._
  import io.radanalytics.silex.feature.extractor.spark.implicits._

  import io.radanalytics.silex.feature.extractor.FeatureSeqSpecSupport._
  import io.radanalytics.silex.scalatest.matchers._

  import org.apache.spark.mllib.linalg._
  import org.apache.spark.mllib.regression.LabeledPoint

  def vectorType(v: Vector) = v match {
    case _: DenseVector => 'dense
    case _: SparseVector => 'sparse
    case _ => 'undefined
  }

  it should "construct a FeatureSeq from a Spark DenseVector" in {
    identityTest(FeatureSeq(new DenseVector(Array[Double]())))
    val s1 = FeatureSeq(new DenseVector(Array(3.14, 2.72)))
    s1.length should be (2)
    s1.density should be (1.0)
    xyTest(s1, (0, 3.14), (1, 2.72))
    propertyTest(
      FeatureSeq(new DenseVector(Array(3.14, 2.72))),
      FeatureSeq(new DenseVector(Array(1.1, 2.2))),
      FeatureSeq(new DenseVector(Array(7.77, 8.88))))
  }

  it should "construct a FeatureSeq from a Spark SparseVector" in {
    identityTest(FeatureSeq(new SparseVector(0, Array[Int](), Array[Double]())))
    val s1 = FeatureSeq(new SparseVector(5, Array(1, 3), Array(3.14, 2.72)))
    s1.length should be (5)
    s1.density should be (0.4 +- densityEpsilon)
    xyTest(s1, (0, 0.0), (1, 3.14), (2, 0.0), (3, 2.72), (4, 0.0))
    propertyTest(
      FeatureSeq(new SparseVector(5, Array(1, 3), Array(3.14, 2.72))),
      FeatureSeq(new SparseVector(4, Array(1, 2), Array(3.33, 4.44))),
      FeatureSeq(new SparseVector(10, Array(4, 9), Array(5.0, 9.0))))
  }

  it should "construct a FeatureSeq from a Spark LabeledPoint" in {
    identityTest(
      FeatureSeq(new LabeledPoint(1.0, new SparseVector(0, Array[Int](), Array[Double]()))))
    val s1 = FeatureSeq(new LabeledPoint(1.0, new SparseVector(5, Array(1, 3), Array(3.14, 2.72))))
    s1.length should be (5)
    s1.density should be (0.4 +- densityEpsilon)
    xyTest(s1, (0, 0.0), (1, 3.14), (2, 0.0), (3, 2.72), (4, 0.0))
    propertyTest(
      FeatureSeq(new LabeledPoint(1.0, new SparseVector(5, Array(1, 3), Array(3.14, 2.72)))),
      FeatureSeq(new LabeledPoint(1.0, new DenseVector(Array(1.1, 2.2)))),
      FeatureSeq(new LabeledPoint(1.0, new SparseVector(10, Array(4, 9), Array(5.0, 9.0)))))
  }

  it should "construct a FeatureSeq from mixed Spark vectors" in {
    propertyTest(
      FeatureSeq(Array(8.88, 9.99)),
      FeatureSeq(new LabeledPoint(1.0, new DenseVector(Array(1.1, 2.2)))),
      FeatureSeq(new DenseVector(Array(1.1, 2.2))),
      FeatureSeq(new DenseVector(Array(7.77, 8.88))),
      FeatureSeq(new SparseVector(5, Array(1, 3), Array(3.14, 2.72))),
      FeatureSeq(new SparseVector(10, Array(4, 9), Array(5.0, 9.0))))
  }

  it should "support the toSpark enriched method" in {
    val t1 = new DenseVector(Array(1.1, 2.2))
    val v1 = FeatureSeq(t1).toSpark
    v1.equals(t1) should be (true)
    vectorType(v1) should equal ('dense)

    val t2 = new SparseVector(5, Array(1, 3), Array(1.1, 2.2))
    val v2 = FeatureSeq(t2).toSpark
    v2.equals(t2) should be (true)
    vectorType(v2) should equal ('sparse)

    val t3 = new SparseVector(3, Array(1, 2), Array(1.1, 2.2))
    val v3 = FeatureSeq(t3).toSpark
    v3.equals(t3) should be (true)
    vectorType(v3) should equal ('dense)
  }

  it should "support the toLabeledPoint enriched method" in {
    val t1 = new LabeledPoint(0.0, new DenseVector(Array(1.1, 2.2)))
    val v1 = FeatureSeq(t1).toLabeledPoint(1.0)
    v1.label should be (1.0)
    v1.features.equals(t1.features) should be (true)
    vectorType(v1.features) should equal ('dense)

    val t2 = new LabeledPoint(0.0, new SparseVector(5, Array(1, 3), Array(1.1, 2.2)))
    val v2 = FeatureSeq(t2).toLabeledPoint(2.0)
    v2.label should be (2.0)
    v2.features.equals(t2.features) should be (true)
    vectorType(v2.features) should equal ('sparse)

    val t3 = new LabeledPoint(0.0, new SparseVector(3, Array(1, 2), Array(1.1, 2.2)))
    val v3 = FeatureSeq(t3).toLabeledPoint(3.0)
    v3.label should be (3.0)
    v3.features.equals(t3.features) should be (true)
    vectorType(v3.features) should equal ('dense)
  }
}
