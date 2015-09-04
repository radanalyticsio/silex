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

package com.redhat.et.silex.util

import org.scalatest._

import com.redhat.et.silex.testing.matchers._

class RichSliceSpec extends FlatSpec with Matchers {
  import com.redhat.et.silex.util.richslice._

  it should "slice with single indices" in {
    val data = (0 until 10).toVector
    data.richSlice(2, 3, 5, 7) should beEqSeq(Seq(2, 3, 5, 7))
  }

  it should "slice with single negative indices" in {
    val data = (0 until 10).toVector
    data.richSlice(-8, -7, -5, -3) should beEqSeq(Seq(2, 3, 5, 7))
  }

  it should "slice with ranges" in {
    val data = (0 until 10).toVector
    data.richSlice(3 to 6) should beEqSeq(3 to 6)
  }

  it should "slice with stepped ranges" in {
    val data = (0 until 10).toVector
    data.richSlice(3 to 9 by 3) should beEqSeq(3 to 9 by 3)
  }

  it should "slice with star" in {
    val data = (0 until 10).toVector
    data.richSlice(3 until *) should beEqSeq(3 until 10)
    data.richSlice(3 to *) should beEqSeq(3 until 10)
    data.richSlice(* until 7) should beEqSeq(0 until 7)
    data.richSlice(* to 7) should beEqSeq(0 to 7)
    data.richSlice(*) should beEqSeq(data)
  }
}
