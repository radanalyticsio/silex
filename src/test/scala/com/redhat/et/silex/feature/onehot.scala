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

package com.redhat.et.silex.feature.onehot

import org.scalatest._

import com.redhat.et.silex.testing.PerTestSparkContext
import com.redhat.et.silex.testing.matchers._

class OneHotModelSpec extends FlatSpec with Matchers {
  it should "provide oneHotExtractor" in {
    val hist = Seq(("a", 3.0), ("b", 2.0), ("c", 1.0))
    val model = new OneHotModel(hist)
    model.oneHotExtractor()("a") should beEqSeq(Seq(1.0, 0.0, 0.0))
    model.oneHotExtractor()("b") should beEqSeq(Seq(0.0, 1.0, 0.0))
    model.oneHotExtractor()("c") should beEqSeq(Seq(0.0, 0.0, 1.0))
    model.oneHotExtractor()("d") should beEqSeq(Seq(0.0, 0.0, 0.0))

    model.oneHotExtractor(undefName = "*")("a") should beEqSeq(Seq(1.0, 0.0, 0.0, 0.0))
    model.oneHotExtractor(undefName = "*")("b") should beEqSeq(Seq(0.0, 1.0, 0.0, 0.0))
    model.oneHotExtractor(undefName = "*")("c") should beEqSeq(Seq(0.0, 0.0, 1.0, 0.0))
    model.oneHotExtractor(undefName = "*")("d") should beEqSeq(Seq(0.0, 0.0, 0.0, 1.0))

    model.oneHotExtractor(minFreq = 2)("a") should beEqSeq(Seq(1.0, 0.0))
    model.oneHotExtractor(minFreq = 2)("b") should beEqSeq(Seq(0.0, 1.0))
    model.oneHotExtractor(minFreq = 2)("c") should beEqSeq(Seq(0.0, 0.0))

    model.oneHotExtractor(maxFreq = 2)("a") should beEqSeq(Seq(0.0, 0.0))
    model.oneHotExtractor(maxFreq = 2)("b") should beEqSeq(Seq(1.0, 0.0))
    model.oneHotExtractor(maxFreq = 2)("c") should beEqSeq(Seq(0.0, 1.0))

    model.oneHotExtractor(minProb = 0.33)("a") should beEqSeq(Seq(1.0, 0.0))
    model.oneHotExtractor(minProb = 0.33)("b") should beEqSeq(Seq(0.0, 1.0))
    model.oneHotExtractor(minProb = 0.33)("c") should beEqSeq(Seq(0.0, 0.0))

    model.oneHotExtractor(maxProb = 0.34)("a") should beEqSeq(Seq(0.0, 0.0))
    model.oneHotExtractor(maxProb = 0.34)("b") should beEqSeq(Seq(1.0, 0.0))
    model.oneHotExtractor(maxProb = 0.34)("c") should beEqSeq(Seq(0.0, 1.0))

    model.oneHotExtractor(maxSize = 2)("a") should beEqSeq(Seq(1.0, 0.0))
    model.oneHotExtractor(maxSize = 2)("b") should beEqSeq(Seq(0.0, 1.0))
    model.oneHotExtractor(maxSize = 2)("c") should beEqSeq(Seq(0.0, 0.0))

    model.oneHotExtractor(minFreq = 4)("a") should beEqSeq(Seq.empty[Double])
    model.oneHotExtractor(maxFreq = 0)("a") should beEqSeq(Seq.empty[Double])
    model.oneHotExtractor(minProb = 1.0)("a") should beEqSeq(Seq.empty[Double])
    model.oneHotExtractor(maxProb = 0.0)("a") should beEqSeq(Seq.empty[Double])
    model.oneHotExtractor(maxSize = 0)("a") should beEqSeq(Seq.empty[Double])
  }

  it should "provide multiHotExtractor" in {
    val hist = Seq(("a", 3.0), ("b", 2.0), ("c", 1.0))
    val model = new OneHotModel(hist)
    model.multiHotExtractor()(Seq("a")) should beEqSeq(Seq(1.0, 0.0, 0.0))
    model.multiHotExtractor()(Seq("b")) should beEqSeq(Seq(0.0, 1.0, 0.0))
    model.multiHotExtractor()(Seq("c")) should beEqSeq(Seq(0.0, 0.0, 1.0))
    model.multiHotExtractor()(Seq("d")) should beEqSeq(Seq(0.0, 0.0, 0.0))
    model.multiHotExtractor()(Seq("a", "c")) should beEqSeq(Seq(1.0, 0.0, 1.0))
    model.multiHotExtractor()(Seq("b", "d")) should beEqSeq(Seq(0.0, 1.0, 0.0))

    model.multiHotExtractor(undefName = "*")(Seq("a")) should beEqSeq(Seq(1.0, 0.0, 0.0, 0.0))
    model.multiHotExtractor(undefName = "*")(Seq("b")) should beEqSeq(Seq(0.0, 1.0, 0.0, 0.0))
    model.multiHotExtractor(undefName = "*")(Seq("c")) should beEqSeq(Seq(0.0, 0.0, 1.0, 0.0))
    model.multiHotExtractor(undefName = "*")(Seq("d")) should beEqSeq(Seq(0.0, 0.0, 0.0, 1.0))
    model.multiHotExtractor(undefName = "*")(Seq("a", "c")) should beEqSeq(Seq(1.0, 0.0, 1.0, 0.0))
    model.multiHotExtractor(undefName = "*")(Seq("b", "d")) should beEqSeq(Seq(0.0, 1.0, 0.0, 1.0))

    model.multiHotExtractor(minFreq = 2)(Seq("a")) should beEqSeq(Seq(1.0, 0.0))
    model.multiHotExtractor(minFreq = 2)(Seq("b")) should beEqSeq(Seq(0.0, 1.0))
    model.multiHotExtractor(minFreq = 2)(Seq("c")) should beEqSeq(Seq(0.0, 0.0))
    model.multiHotExtractor(minFreq = 2)(Seq("a", "c")) should beEqSeq(Seq(1.0, 0.0))

    model.multiHotExtractor(maxFreq = 2)(Seq("a")) should beEqSeq(Seq(0.0, 0.0))
    model.multiHotExtractor(maxFreq = 2)(Seq("b")) should beEqSeq(Seq(1.0, 0.0))
    model.multiHotExtractor(maxFreq = 2)(Seq("c")) should beEqSeq(Seq(0.0, 1.0))
    model.multiHotExtractor(maxFreq = 2)(Seq("a", "c")) should beEqSeq(Seq(0.0, 1.0))

    model.multiHotExtractor(minProb = 0.33)(Seq("a")) should beEqSeq(Seq(1.0, 0.0))
    model.multiHotExtractor(minProb = 0.33)(Seq("b")) should beEqSeq(Seq(0.0, 1.0))
    model.multiHotExtractor(minProb = 0.33)(Seq("c")) should beEqSeq(Seq(0.0, 0.0))
    model.multiHotExtractor(minProb = 0.33)(Seq("a", "c")) should beEqSeq(Seq(1.0, 0.0))

    model.multiHotExtractor(maxProb = 0.34)(Seq("a")) should beEqSeq(Seq(0.0, 0.0))
    model.multiHotExtractor(maxProb = 0.34)(Seq("b")) should beEqSeq(Seq(1.0, 0.0))
    model.multiHotExtractor(maxProb = 0.34)(Seq("c")) should beEqSeq(Seq(0.0, 1.0))
    model.multiHotExtractor(maxProb = 0.34)(Seq("a", "c")) should beEqSeq(Seq(0.0, 1.0))

    model.multiHotExtractor(maxSize = 2)(Seq("a")) should beEqSeq(Seq(1.0, 0.0))
    model.multiHotExtractor(maxSize = 2)(Seq("b")) should beEqSeq(Seq(0.0, 1.0))
    model.multiHotExtractor(maxSize = 2)(Seq("c")) should beEqSeq(Seq(0.0, 0.0))

    model.multiHotExtractor(minFreq = 4)(Seq("a")) should beEqSeq(Seq.empty[Double])
    model.multiHotExtractor(maxFreq = 0)(Seq("a")) should beEqSeq(Seq.empty[Double])
    model.multiHotExtractor(minProb = 1.0)(Seq("a")) should beEqSeq(Seq.empty[Double])
    model.multiHotExtractor(maxProb = 0.0)(Seq("a")) should beEqSeq(Seq.empty[Double])
    model.multiHotExtractor(maxSize = 0)(Seq("a")) should beEqSeq(Seq.empty[Double])
  }
}
