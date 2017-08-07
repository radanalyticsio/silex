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

package io.radanalytics.silex.cluster

import org.scalatest._

class KMedoidsModelSpec extends FlatSpec with Matchers {
  it should "operate correctly given medoid parameters" in {
    val model = new KMedoidsModel(
      Seq(Vector(0.0, 0.0), Vector(1.0, 1.0)),
      KMedoidsSpecSupport.vectorAbs)
    model.k should be (2)
    model.predict(Vector(0.1, 0.1)) should be (0)
    model.predict(Vector(1.1, 1.1)) should be (1)
    model.predict(Vector(0.49, 0.49)) should be (0)
    model.predict(Vector(0.51, 0.51)) should be (1)

    model.predictor(Vector(0.49, 0.49)) should be (0)
    model.predictor(Vector(0.51, 0.51)) should be (1)

    model.distance(Vector(0.0, 0.0)) should be (0.0 +- 0.00001)
    model.distance(Vector(0.1, 0.1)) should be (0.2 +- 0.00001)
    model.distance(Vector(1.1, 1.1)) should be (0.2 +- 0.00001)
  }
}
