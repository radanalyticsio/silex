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

package com.redhat.et.silex.feature.extractor.breeze

import org.scalatest._

class BreezeFeatureSeqSpec extends FlatSpec with Matchers {
  import com.redhat.et.silex.feature.extractor._
  import com.redhat.et.silex.feature.extractor.breeze.implicits._

  import com.redhat.et.silex.feature.extractor.FeatureSeqSpecSupport._
  import com.redhat.et.silex.scalatest.matchers._

  import _root_.breeze.linalg._

  def vectorType(v: Vector[_]) = v match {
    case _: DenseVector[_] => 'dense
    case _: SparseVector[_] => 'sparse
    case _ => 'undefined
  }

  it should "construct a FeatureSeq from a Breeze DenseVector" in {
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

  it should "construct a FeatureSeq from a Breeze SparseVector" in {
    identityTest(FeatureSeq(new SparseVector(Array[Int](), Array[Double](), 0)))
    val s1 = FeatureSeq(new SparseVector(Array(1, 3), Array(3.14, 2.72), 5))
    s1.length should be (5)
    s1.density should be (0.4 +- densityEpsilon)
    xyTest(s1, (0, 0.0), (1, 3.14), (2, 0.0), (3, 2.72), (4, 0.0))
    propertyTest(
      FeatureSeq(new SparseVector(Array(1, 3), Array(3.14, 2.72), 5)),
      FeatureSeq(new SparseVector(Array(1, 2), Array(3.33, 4.44), 4)),
      FeatureSeq(new SparseVector(Array(4, 9), Array(5.0, 9.0), 10)))
  }

  it should "construct a FeatureSeq from mixed Breeze vectors" in {
    propertyTest(
      FeatureSeq(Array(8.88, 9.99)),
      FeatureSeq(new DenseVector(Array(1.1, 2.2))),
      FeatureSeq(new DenseVector(Array(7.77, 8.88))),
      FeatureSeq(new SparseVector(Array(1, 3), Array(3.14, 2.72), 5)),
      FeatureSeq(new SparseVector(Array(4, 9), Array(5.0, 9.0), 10)))
  }

  it should "support the toBreeze enriched method" in {
    val t1 = new DenseVector(Array(1.1, 2.2))
    val v1 = FeatureSeq(t1).toBreeze
    v1.equals(t1) should be (true)
    vectorType(v1) should equal ('dense)

    val t2 = new SparseVector(Array(1, 3), Array(1.1, 2.2), 5)
    val v2 = FeatureSeq(t2).toBreeze
    v2.equals(t2) should be (true)
    vectorType(v2) should equal ('sparse)

    val t3 = new SparseVector(Array(1, 2), Array(1.1, 2.2), 3)
    val v3 = FeatureSeq(t3).toBreeze
    v3.equals(t3) should be (true)
    vectorType(v3) should equal ('dense)
  }
}
